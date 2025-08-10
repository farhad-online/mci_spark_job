package ir.mci.dwbi.bigdata.spark_job.jobs.ods.network_switch

import ir.mci.dwbi.bigdata.spark_job.core.configs.ConfigModule
import ir.mci.dwbi.bigdata.spark_job.core.connectors.hive.HiveSink
import ir.mci.dwbi.bigdata.spark_job.core.connectors.kafka.KafkaSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object OdsNetworkSwitchETL {
  def main(args: Array[String]): Unit = {
    ConfigModule.setConfigPath(args(0))

    val spark = OdsNetworkSwitchConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()
    val kafkaSource = KafkaSource.getKafkaSource(spark, OdsNetworkSwitchConfig.kafkaConsumer)
    val input = kafkaSource.select(col("value").cast(StringType)).as("value")
    val processedData = OdsNetworkSwitchProcessor.process(input)
    HiveSink.hiveSink(processedData, OdsNetworkSwitchConfig.hive, OdsNetworkSwitchConfig.spark)
  }
}
