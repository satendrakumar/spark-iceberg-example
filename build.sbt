ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark-iceberg-example",
    fork := true,
    run / javaOptions  ++= Seq("-Xms16G", "-Xmx16G", "-XX:+UseConcMarkSweepGC"),
  )

libraryDependencies ++= Seq(
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "org.apache.spark" %% "spark-hive" % "3.5.3",
  "org.duckdb" % "duckdb_jdbc" % "1.1.1",
  "ch.qos.logback" % "logback-classic" % "1.5.9",
  "org.apache.iceberg" % "iceberg-core" % "1.6.0",
)