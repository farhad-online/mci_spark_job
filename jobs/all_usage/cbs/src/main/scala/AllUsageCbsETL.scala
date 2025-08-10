package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.cbs

import ir.mci.dwbi.bigdata.spark_job.core.configs.ConfigModule
import ir.mci.dwbi.bigdata.spark_job.core.connectors.hive.HiveSink
import ir.mci.dwbi.bigdata.spark_job.core.connectors.kafka.KafkaSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object AllUsageCbsETL {
  def main(args: Array[String]): Unit = {
    ConfigModule.setConfigPath(args(0))

    val spark = AllUsageCbsConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()
    val kafkaSource = KafkaSource.getKafkaSource(spark, AllUsageCbsConfig.kafkaConsumer)
    val input = kafkaSource.select(col("value").cast(StringType)).as("value")
    val processedData = AllUsageCbsProcessor.process(input)
    HiveSink.hiveSink(processedData, AllUsageCbsConfig.hive, AllUsageCbsConfig.spark)
  }
}
