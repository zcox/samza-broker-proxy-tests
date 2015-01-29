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
      getMessages[String, Int]("mystore", 3) must_== Seq(("a", 10))

      println("Stopping job the 2nd time")
      stopJob(job)
    }
  }

  trait context extends Scope with After {
    val zkConnect: String = TestZKUtils.zookeeperConnect
    val zookeeper = new EmbeddedZookeeper(zkConnect)

    val brokerId1 = 0
    val brokerId2 = 1
    val brokerId3 = 2
    val ports = TestUtils.choosePorts(3)
    val (port1, port2, port3) = (ports(0), ports(1), ports(2))
    val kafkaConfig1 = TestUtils.createBrokerConfig(brokerId1, port1)
    val kafkaBroker1 = TestUtils.createServer(new KafkaConfig(kafkaConfig1))
    //TODO kafkaBroker2 and kafkaBroker3?
    val metadataBrokerList = s"localhost:$port1"

    val jobFactory = new ThreadJobFactory //NOTE we were using JobRunner.run before, but that pretty much just creates a JobFactory, then calls getJob(config).submit

    val jobConfig = Map(
      // "job.factory.class" -> jobFactory.getClass.getCanonicalName,
      "job.name" -> "restore-store-spec",
      "task.class" -> "com.banno.samza.RestoreStoreSpecTask",
      "task.inputs" -> "kafka.input",
      // "serializers.registry.string.class" -> "org.apache.samza.serializers.StringSerdeFactory",
      "serializers.registry.bytes.class" -> "org.apache.samza.serializers.ByteSerdeFactory",
      "serializers.registry.byteswrapper.class" -> "com.banno.samza.ByteArrayWrapperSerdeFactory",
      // "stores.mystore.factory" -> "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory", //NOTE I think Samza's TestStatefulTask uses LevelDB here?
      "stores.mystore.factory" -> "org.apache.samza.storage.kv.LevelDbKeyValueStorageEngineFactory", //test fails in same way using either LevelDB or RocksDB
      "stores.mystore.key.serde" -> "byteswrapper", //Samza's CachedStore cannot handle Array[Byte] keys (trollface)
      "stores.mystore.msg.serde" -> "bytes",
      "stores.mystore.changelog" -> "kafka.mystore",

      "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
      // Always start consuming at offset 0. This avoids a race condition between
      // the producer and the consumer in this test (SAMZA-166, SAMZA-224).
      // "systems.kafka.samza.offset.default" -> "oldest", // applies to a nonempty topic <==================== TODO include?
      // "systems.kafka.consumer.auto.offset.reset" -> "smallest", // applies to an empty topic <============== TODO include?
      // "systems.kafka.samza.msg.serde" -> "string",
      "systems.kafka.consumer.zookeeper.connect" -> zkConnect,
      "systems.kafka.producer.metadata.broker.list" -> metadataBrokerList,
      // Since using state, need a checkpoint manager
      "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
      "task.checkpoint.system" -> "kafka",
      "task.checkpoint.replication.factor" -> "1",
      // However, don't have the inputs use the checkpoint manager
      // since the second part of the test expects to replay the input streams.
      // "systems.kafka.streams.input.samza.reset.offset" -> "true" //we do not want the job to re-process its input stream when it starts the 2nd time

      //Banno-specific config starts here:
      "systems.kafka.producer.batch.num.messages" -> "1",
      "stores.mystore.object.cache.size" -> "0"
    )

    def runJob(): (StreamJob, RestoreStoreSpecTask) = {
      val job = jobFactory.getJob(new MapConfig(jobConfig))
      job.submit
      job.waitForStatus(Running, 60000) must_== Running
      RestoreStoreSpecTask.awaitTaskRegistered //on 2nd time through, we block here for 60 secs because RestoreStoreSpecTask.init will never be called, because TaskStorageManager.stopConsumers is blocked by BrokerProxy.stop
      RestoreStoreSpecTask.tasks.size must_== 1 //on 2nd time through this will fail because the task has not fully started yet

      val task = RestoreStoreSpecTask.tasks.head._2
      task.initFinished.await(60, TimeUnit.SECONDS)
      task.initFinished.getCount must_== 0

      (job, task)
    }

    def stopJob(job: StreamJob): Unit = {
      job.kill
      SamzaBrokerProxyThreadInterruptHack.interruptAllBrokerProxyThreads() //if this is removed, then the job will not shutdown, and test will fail on next line
      job.waitForFinish(60000) must_== UnsuccessfulFinish
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
      kafkaBroker1.shutdown
      kafkaBroker1.awaitShutdown()
      Utils.rm(kafkaBroker1.config.logDirs)
      zookeeper.shutdown
    }

    def after = {
      shutdownKafkaBrokers()
    }
  }
}

object SamzaBrokerProxyThreadInterruptHack {
  //BrokerProxy threads currently take some extra manual labor to actually interrupt
  val logger = LoggerFactory.getLogger(getClass)
  val prefix = org.apache.samza.util.ThreadNamePrefix.SAMZA_THREAD_NAME_PREFIX + org.apache.samza.system.kafka.BrokerProxy.BROKER_PROXY_THREAD_NAME_PREFIX
  def isBrokerProxyThread(thread: Thread): Boolean = thread.getName startsWith prefix
  lazy val brokerProxyThreads = Thread.getAllStackTraces.keys filter isBrokerProxyThread
  def interrupt(thread: Thread) = {
    var count = 0
    while(thread.isAlive) {
      thread.interrupt
      count += 1
    }
    logger.info(s"Took $count tries to interrupt ${thread.getName}")
  }
  def interruptAllBrokerProxyThreads() = {
    logger.debug("Interrupting broker proxy threads...")
    brokerProxyThreads foreach interrupt
    logger.debug("Finished interrupting broker proxy threads")
  }
}
