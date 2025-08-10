import sbt.Keys.*
import sbtassembly.AssemblyPlugin.autoImport.*
import sbtassembly.{MergeStrategy, PathList}

ThisBuild / organization := "ir.mci.dwbi.bigdata"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "1.0.0"

val sparkVersion = "3.2.3"
val log4jVersion = "2.17.1"
val configVersion = "1.4.2"

val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
  "org.apache.kafka" % "kafka-clients" % "2.4.1" % "provided",
  "org.postgresql" % "postgresql" % "42.7.3" % "provided",
  "com.typesafe" % "config" % "1.4.3" % "provided",
  "org.slf4j" % "slf4j-api" % "1.7.36" % "provided",
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion % "provided",
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion % "provided",
  "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion % "provided",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % "provided",
  "org.apache.kafka" % "kafka-log4j-appender" % sparkVersion % "provided",
  "org.scala-lang" % "scala-reflect" % "2.12.15" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "3.2.4" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.2.4" % "provided",
)

lazy val commonSettings = Seq(
  libraryDependencies ++= commonDependencies,
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
  //  assembly / unmanagedResourceDirectories += baseDirectory.value / "config" // Include config files
)

lazy val assemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _@_*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    assemblySettings,
    name := "core"
  )

lazy val all_usage_network_switch = (project in file("jobs/all_usage/network_switch"))
  .dependsOn(core)
  .settings(
    commonSettings,
    assemblySettings,
    name := "all_usage_network_switch",
    assembly / mainClass := Some("ir.mci.dwbi.bigdata.spark_job.all_usage.network_switch.AllUsageNetworkSwitchETL"),
    assembly / assemblyJarName := "all_usage_network_switch.jar"
  )

lazy val all_usage_pgw_new = (project in file("jobs/all_usage/pgw_new"))
  .dependsOn(core)
  .settings(
    commonSettings,
    assemblySettings,
    name := "all_usage_pgw_new",
    assembly / mainClass := Some("ir.mci.dwbi.bigdata.spark_job.all_usage.pgw_new.AllUsagePgwNewETL"),
    assembly / assemblyJarName := "all_usage_pgw_new.jar"
  )

lazy val all_usage_cbs = (project in file("jobs/all_usage/cbs"))
  .dependsOn(core)
  .settings(
    commonSettings,
    assemblySettings,
    name := "all_usage_cbs",
    assembly / mainClass := Some("ir.mci.dwbi.bigdata.spark_job.all_usage.cbs.AllUsageCbsETL"),
    assembly / assemblyJarName := "all_usage_cbs.jar"
  )

lazy val ods_pgw_new = (project in file("jobs/ods/pgw_new"))
  .dependsOn(core)
  .settings(
    commonSettings,
    assemblySettings,
    name := "ods_pgw_new",
    assembly / mainClass := Some("ir.mci.dwbi.bigdata.spark_job.ods.pgw_new.OdsPgwNewETL"),
    assembly / assemblyJarName := "ods_pgw_new.jar"
  )

lazy val ods_network_switch = (project in file("jobs/ods/network_switch"))
  .dependsOn(core)
  .settings(
    commonSettings,
    assemblySettings,
    name := "ods_network_switch",
    assembly / mainClass := Some("ir.mci.dwbi.bigdata.spark_job.ods.network_switch.OdsNetworkSwitchETL"),
    assembly / assemblyJarName := "ods_network_switch.jar"
  )

lazy val root = project
  .in(file("."))
  .aggregate(core, all_usage_network_switch, all_usage_pgw_new, all_usage_cbs, ods_network_switch, ods_pgw_new)
  .settings(
    name := "spark_job",
    publish / skip := true
  )