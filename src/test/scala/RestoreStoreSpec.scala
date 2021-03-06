package com.banno.samza

import org.specs2.mutable.{Specification, After}
import org.specs2.specification.Scope
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import kafka.server.KafkaConfig
import kafka.utils.{Utils, TestUtils, TestZKUtils}
import kafka.zk.EmbeddedZookeeper
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.message.MessageAndMetadata
import org.apache.samza.task.{StreamTask, InitableTask, MessageCollector, TaskCoordinator}
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.job.ApplicationStatus.{Running, UnsuccessfulFinish}
import org.apache.samza.job.local.ThreadJobFactory
import org.apache.samza.job.StreamJob
import org.apache.samza.config.{Config, MapConfig}
import org.apache.samza.task.TaskContext
import org.apache.samza.container.TaskName
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.serializers.Serde
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedMap
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.Properties
import org.slf4j.LoggerFactory

object RestoreStoreSpecTask {
  val tasks = new HashMap[TaskName, RestoreStoreSpecTask] with SynchronizedMap[TaskName, RestoreStoreSpecTask]
  @volatile var allTasksRegistered = new CountDownLatch(RestoreStoreSpec.totalTasks)

  def register(taskName: TaskName, task: RestoreStoreSpecTask): Unit = {
    tasks += taskName -> task
    allTasksRegistered.countDown()
  }

  def awaitTaskRegistered: Unit = {
    allTasksRegistered.await(60, TimeUnit.SECONDS)
    //Samza's test asserts that ^^ didn't timeout and we have registered expected number of tasks
    RestoreStoreSpecTask.allTasksRegistered = new CountDownLatch(RestoreStoreSpec.totalTasks)
  }
}

class RestoreStoreSpecTask extends StreamTask with InitableTask {
  var store: KeyValueStore[ByteArrayWrapper, Array[Byte]] = null
  val initFinished = new CountDownLatch(1)

  override def init(config: Config, context: TaskContext): Unit = {
    RestoreStoreSpecTask.register(context.getTaskName, this)
    store = context.getStore("mystore").asInstanceOf[KeyValueStore[ByteArrayWrapper, Array[Byte]]]
    initFinished.countDown()
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    //all this serializing/deserializing looks unnecessary, but is essentially what we are doing in real jobs...
    val key: String = implicitly[Serde[String]].fromBytes(envelope.getKey.asInstanceOf[Array[Byte]])
    val keyBytes: ByteArrayWrapper = new ByteArrayWrapper(implicitly[Serde[String]].toBytes(key))
    val count: Int = implicitly[Serde[Int]].fromBytes(envelope.getMessage.asInstanceOf[Array[Byte]])
    val oldTotal: Int = Option(store.get(keyBytes)).map(implicitly[Serde[Int]].fromBytes).getOrElse(0)
    val newTotal: Int = oldTotal + count
    store.put(keyBytes, implicitly[Serde[Int]].toBytes(newTotal))
  }
}

object RestoreStoreSpec extends Specification {
  val totalTasks = 1

  "samza" should {
    "restore store from changelog" in new context {
      val (job, task) = runJob()
      sendToKafka("input", ("a", 1), ("a", 2), ("a", 3))
      getMessages[String, Int]("mystore", 3) must_== Seq(("a", 1), ("a", 3), ("a", 6))

      println("Stopping job the 1st time")
      stopJob(job)

      val (job2, task2) = runJob()
      sendToKafka("input", ("a", 4))
      getMessages[String, Int]("mystore", 1) must_== Seq(("a", 10))

      println("Stopping job the 2nd time")
      stopJob(job)
    }
  }

  trait context extends Scope with After {
    val zkConnect: String = TestZKUtils.zookeeperConnect
    val zookeeper = new EmbeddedZookeeper(zkConnect)

    val brokerIds = 0 to 2
    val ports = TestUtils.choosePorts(3)
    val kafkaBrokers = for ((brokerId, port) <- brokerIds.zip(ports)) yield TestUtils.createServer(new KafkaConfig(TestUtils.createBrokerConfig(brokerId, port)))
    val metadataBrokerList = s"localhost:${ports.head}"

    val jobFactory = new ThreadJobFactory //NOTE we were using JobRunner.run before, but that pretty much just creates a JobFactory, then calls getJob(config).submit

    val jobConfig = Map(
      // "job.factory.class" -> jobFactory.getClass.getCanonicalName,
      "job.name" -> "restore-store-spec",
      "task.class" -> "com.banno.samza.RestoreStoreSpecTask",
      "task.inputs" -> "kafka.input",
      "serializers.registry.bytes.class" -> "org.apache.samza.serializers.ByteSerdeFactory",
      "serializers.registry.byteswrapper.class" -> "com.banno.samza.ByteArrayWrapperSerdeFactory",
      "stores.mystore.factory" -> "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory", //NOTE I think Samza's TestStatefulTask uses LevelDB here?
      "stores.mystore.key.serde" -> "byteswrapper", //Samza's CachedStore cannot handle Array[Byte] keys
      "stores.mystore.msg.serde" -> "bytes",
      "stores.mystore.changelog" -> "kafka.mystore",

      "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
      // Always start consuming at offset 0. This avoids a race condition between
      // the producer and the consumer in this test (SAMZA-166, SAMZA-224).
      // "systems.kafka.samza.offset.default" -> "oldest", // applies to a nonempty topic <================= test works without this
      // "systems.kafka.consumer.auto.offset.reset" -> "smallest", // applies to an empty topic <=========== test works without this
      "systems.kafka.consumer.zookeeper.connect" -> zkConnect,
      "systems.kafka.producer.metadata.broker.list" -> metadataBrokerList,
      // Since using state, need a checkpoint manager
      "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
      "task.checkpoint.system" -> "kafka",
      "task.checkpoint.replication.factor" -> "1",
      // However, don't have the inputs use the checkpoint manager
      // since the second part of the test expects to replay the input streams.
      // "systems.kafka.streams.input.samza.reset.offset" -> "true" //<=========== we do *not* want the job to re-process its input stream when it starts the 2nd time

      //Banno-specific config starts here:
      "systems.kafka.producer.batch.num.messages" -> "1", //immediately send messages to kafka topics
      "stores.mystore.object.cache.size" -> "0" //immediately send puts to key-value store
    )

    def runJob(): (StreamJob, RestoreStoreSpecTask) = {
      val job = jobFactory.getJob(new MapConfig(jobConfig))
      job.submit
      job.waitForStatus(Running, 60000) must_== Running
      RestoreStoreSpecTask.awaitTaskRegistered
      RestoreStoreSpecTask.tasks.size must_== 1

      val task = RestoreStoreSpecTask.tasks.head._2
      task.initFinished.await(60, TimeUnit.SECONDS)
      task.initFinished.getCount must_== 0

      (job, task)
    }

    def stopJob(job: StreamJob): Unit = {
      job.kill
      job.waitForFinish(60000) must_== UnsuccessfulFinish //it's "unsuccessful" because job.kill throws an exception
      RestoreStoreSpecTask.tasks.clear()
    }

    def newProducer(): KafkaProducer = {
      val props = new Properties()
      props.put("bootstrap.servers", metadataBrokerList)
      props.put("client.id", "RestoreStoreSpec")
      props.put("acks", "1")
      props.put("retries", "3")
      props.put("batch.size", "16384")
      props.put("linger.ms", "0")
      props.put("timeout.ms", "30000")
      new KafkaProducer(props)
    }

    def sendToKafka[K: Serde, M: Serde](topic: String, messages: (K, M)*): Unit = {
      val producer = newProducer()
      for ((key, message) <- messages) {
        producer.send(new ProducerRecord(topic, implicitly[Serde[K]].toBytes(key), implicitly[Serde[M]].toBytes(message)))
      }
      producer.close()
    }

    def getMessages[K: Serde, M: Serde](topic: String, count: Int): Seq[(K, M)] = {
      val props = new Properties()
      props.put("group.id", "RestoreStoreSpec")
      props.put("zookeeper.connect", zkConnect)
      props.put("auto.offset.reset", "smallest")

      val consumer = Consumer.create(new ConsumerConfig(props))
      val streams = consumer.createMessageStreams(Map(topic -> 1))
      val iterator = streams(topic)(0).iterator()
      def deserialize(m: MessageAndMetadata[Array[Byte], Array[Byte]]): (K, M) =
        (implicitly[Serde[K]].fromBytes(m.key), implicitly[Serde[M]].fromBytes(m.message))
      val messages = iterator.take(count).toList.map(deserialize)
      consumer.shutdown()
      messages
    }

    def shutdownKafkaBrokers(): Unit = {
      for (kafkaBroker <- kafkaBrokers) {
        kafkaBroker.shutdown
        kafkaBroker.awaitShutdown()
        Utils.rm(kafkaBroker.config.logDirs)
      }
      zookeeper.shutdown
    }

    def after = {
      shutdownKafkaBrokers()
    }
  }
}
