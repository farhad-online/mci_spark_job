package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage

import ir.mci.dwbi.bigdata.spark_job.core.configs.ConfigModule
import ir.mci.dwbi.bigdata.spark_job.core.connectors.hive.HiveSink
import ir.mci.dwbi.bigdata.spark_job.core.connectors.kafka.KafkaSource
import ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.cbs.AllUsageCbsConfig
import ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.network_switch.AllUsageNetworkSwitchConfig
import ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.pgw_new.AllUsagePgwNewConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object AllUsageETL {
  def main(args: Array[String]): Unit = {
    ConfigModule.setConfigPath(args(0))

    val spark = AllUsageConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()
    val kafkaSourcePgwNew = KafkaSource.getKafkaSource(spark, AllUsagePgwNewConfig.kafkaConsumer)
      .select(col("value").cast(StringType)).as("value")
    val kafkaSourceNetworkSwitch = KafkaSource.getKafkaSource(spark, AllUsageNetworkSwitchConfig.kafkaConsumer)
      .select(col("value").cast(StringType)).as("value")
    val kafkaSourceCbs = KafkaSource.getKafkaSource(spark, AllUsageCbsConfig.kafkaConsumer)
      .select(col("value").cast(StringType)).as("value")
    val processedData = AllUsageProcessor.process(kafkaSourcePgwNew, kafkaSourceNetworkSwitch, kafkaSourceCbs)
    HiveSink.hiveSink(processedData, AllUsageConfig.hive, AllUsageConfig.spark)
  }
}
