package ir.mci.dwbi.bigdata.spark_job.core.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object KafkaReader {
  def read(spark: SparkSession, config: Config): DataFrame = {

    val kafkaConfig: Config = config.getConfig("kafka")
    val kafkaOptions: Map[String, String] = kafkaConfig.entrySet().asScala.map { entry =>
      val key = entry.getKey
      val value = entry.getValue.unwrapped().toString
      (key, value)
    }.toMap

    val topics = kafkaConfig.getStringList("topics").asScala.mkString(",")
    val startingOffsetsValue = config.getString("startingOffsets")

    spark.readStream
      .format("kafka")
      .options(kafkaOptions)
      .option("subscribe", topics)
      .option("startingOffsets", startingOffsetsValue)
      .load()


  }
}