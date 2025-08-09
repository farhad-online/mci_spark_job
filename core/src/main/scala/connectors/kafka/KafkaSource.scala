package connectors.kafka

import ir.mci.dwbi.bigdata.spark_job.core.configs.kafka.KafkaConsumerConfig
import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSource extends Logger {
  def getKafkaSource(spark: SparkSession, KafkaConsumerConfig: KafkaConsumerConfig): DataFrame = {
    spark.readStream
      .format(KafkaConsumerConfig.format)
      .options(KafkaConsumerConfig.options)
      .load()
  }
}
