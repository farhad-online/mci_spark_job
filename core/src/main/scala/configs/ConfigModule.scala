package ir.mci.dwbi.bigdata.spark_job.core.configs


import com.typesafe.config.{Config, ConfigFactory}
import ir.mci.dwbi.bigdata.spark_job.core.configs.hive.HiveConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.kafka.KafkaConsumerConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.spark.SparkConfig
import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import java.io.InputStreamReader
import scala.collection.JavaConverters.asScalaSetConverter

object ConfigModule extends Logger {

  trait ConfigModule {
    lazy val config: Config = ConfigModule.config
  }

  private var configFilePath: Option[String] = None

  def setConfigPath(path: String): Unit = {
    logger.debug(s"set new config path: ${path}")
    configFilePath = Some(path)
  }

  private lazy val config: Config = {
    configFilePath match {
      case Some(path) =>
        val fs: FileSystem = FileSystem.get(new Configuration())
        val fsInputStream: FSDataInputStream = fs.open(new Path(path))
        val reader = new InputStreamReader(fsInputStream)
        ConfigFactory.parseReader(reader)
          .withFallback(ConfigFactory.load())
          .withFallback(ConfigFactory.defaultApplication())
          .resolve()
      case None =>
        ConfigFactory.load()
          .withFallback(ConfigFactory.defaultApplication())
          .resolve()
    }
  }

  def parseSparkConfig(config: Config): SparkConfig = {
    val appName = config.getString("appName")
    val master = config.getString("master")
    val checkpointLocation = config.getString("checkpointLocation")
    val format = config.getString("format")
    val outputMode = config.getString("outputMode")
    val batchMode = config.getString("batchMode")
    val batchFormat = config.getString("batchFormat")
    val triggerInterval = config.getInt("triggerInterval")

    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]

    SparkConfig(appName, master, checkpointLocation, outputMode, batchMode, batchFormat, format, triggerInterval, options)
  }

  def parseKafkaConsumerConfig(config: Config): KafkaConsumerConfig = {
    val format = config.getString("format")
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]

    KafkaConsumerConfig(format, options)
  }

  def parseHiveConfig(config: Config): HiveConfig = {
    val tableName = config.getString("tableName")
    val partitionKey = config.getString("partitionKey")
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]

    HiveConfig(tableName, partitionKey, options)
  }

  private def parseStringMap(config: Config): Map[String, String] = {
    config.entrySet().asScala.map { entry =>
      entry.getKey -> entry.getValue.unwrapped().toString
    }.toMap
  }
}
