name := "young-spark-example"

version := "1.0"

scalaVersion := "2.11.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Maven Repository" at "http://repo1.maven.org/maven2/",
  "maven-restlet" at "http://maven.restlet.org")

libraryDependencies ++= {
  val spark_version = "1.6.2"
  Seq(
    "org.apache.spark" %% "spark-core" % spark_version,
    "org.apache.spark" %% "spark-sql" % spark_version,
    "org.apache.spark" %% "spark-streaming" % spark_version,
    "org.apache.spark" %% "spark-mllib" % spark_version,
    "org.apache.spark" %% "spark-hive" % spark_version,
    "org.apache.spark" %% "spark-yarn" % spark_version,
    "org.apache.spark" %% "spark-repl" % spark_version,
    "org.apache.spark" %% "spark-streaming-kafka" % spark_version,
    "org.apache.kafka" % "kafka-clients" % "0.9.0.0",
    "redis.clients" % "jedis" % "2.8.1"
  )
}