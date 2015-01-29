organization := "com.banno"

name := "samza-broker-proxy-tests"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val samzaVersion = "0.8.0" //brings in kafka 0.8.1.1
  val kafkaVersion = "0.8.1.1"
  val kafkaClientsVersion = "0.8.2-beta"
  Seq(
    "org.apache.samza" %% "samza-kafka" % samzaVersion exclude("log4j", "log4j"),
    // "org.apache.samza" %% "samza-kv-rocksdb" % samzaVersion,
    "org.apache.samza" %% "samza-kv-leveldb" % samzaVersion,
    "org.apache.kafka" %% "kafka" % kafkaVersion classifier "test" exclude("log4j", "log4j"),
    "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion,
    "org.slf4j" % "log4j-over-slf4j" % "1.7.10",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.specs2" %% "specs2" % "2.4.2" % "test"
  )
}

fork := true //https://github.com/facebook/rocksdb/issues/308#issuecomment-68015142

javaOptions in run += "-Xms2560m -Xmx2560m -XX:+UseConcMarkSweepGC -server -d64"

net.virtualvoid.sbt.graph.Plugin.graphSettings
