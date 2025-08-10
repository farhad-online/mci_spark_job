package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.pgw_new

import ir.mci.dwbi.bigdata.spark_job.core.configs.ConfigModule
import ir.mci.dwbi.bigdata.spark_job.core.connectors.hive.HiveSink
import ir.mci.dwbi.bigdata.spark_job.core.connectors.kafka.KafkaSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object AllUsagePgwNewETL {
  def main(args: Array[String]): Unit = {
    ConfigModule.setConfigPath(args(0))

    val spark = AllUsagePgwNewConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()
    val kafkaSource = KafkaSource.getKafkaSource(spark, AllUsagePgwNewConfig.kafkaConsumer)
    val input = kafkaSource.select(col("value").cast(StringType)).as("value")
    val processedData = AllUsagePgwNewProcessor.process(input)
    HiveSink.hiveSink(processedData, AllUsagePgwNewConfig.hive, AllUsagePgwNewConfig.spark)
  }
}
