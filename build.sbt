ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark-iceberg-example",
    resolvers += "Akka library repository".at("https://repo.akka.io/maven"),
    fork := true,
    run / javaOptions  ++= Seq("-Xms8G", "-Xmx8G", "-XX:+UseConcMarkSweepGC"),
  )
val AkkaVersion = "2.9.3"
val AkkaHttpVersion = "10.6.3"
libraryDependencies ++= Seq(
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "org.apache.spark" %% "spark-hive" % "3.5.3",
  "org.duckdb" % "duckdb_jdbc" % "1.1.1",
  "ch.qos.logback" % "logback-classic" % "1.5.10",
  "org.apache.iceberg" % "iceberg-core" % "1.6.0",
  "org.postgresql" % "postgresql" % "42.2.23",
  "org.apache.iceberg" % "iceberg-aws-bundle" % "1.6.1",
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion
)


enablePlugins(JavaAppPackaging)

dockerExposedVolumes := Seq("/opt/docker/logs/")

dockerBaseImage := "openjdk:11"

dockerExposedPorts := Seq(8001, 8002)

dockerAlias := dockerAlias.value.withTag(Option("v1.0.0"))