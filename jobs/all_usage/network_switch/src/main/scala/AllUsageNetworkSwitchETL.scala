package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.network_switch

import ir.mci.dwbi.bigdata.spark_job.core.configs.ConfigModule
import ir.mci.dwbi.bigdata.spark_job.core.connectors.hive.HiveSink
import ir.mci.dwbi.bigdata.spark_job.core.connectors.kafka.KafkaSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object AllUsageNetworkSwitchETL {
  def main(args: Array[String]): Unit = {
    ConfigModule.setConfigPath(args(1))

    val spark = AllUsageNetworkSwitchConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()
    val kafkaSource = KafkaSource.getKafkaSource(spark, AllUsageNetworkSwitchConfig.kafkaConsumer)
    val input = kafkaSource.select(col("value").cast(StringType)).as("value")
    val processedData = AllUsageNetworkSwitchProcessor.process(input)
    HiveSink.hiveSink(processedData, AllUsageNetworkSwitchConfig.hive, AllUsageNetworkSwitchConfig.spark)
  }
}
