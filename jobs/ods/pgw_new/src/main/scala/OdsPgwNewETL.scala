package ir.mci.dwbi.bigdata.spark_job.jobs.ods.pgw_new

import ir.mci.dwbi.bigdata.spark_job.core.configs.ConfigModule
import ir.mci.dwbi.bigdata.spark_job.core.connectors.hive.HiveSink
import ir.mci.dwbi.bigdata.spark_job.core.connectors.kafka.KafkaSource
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object OdsPgwNewETL {
  def main(args: Array[String]): Unit = {
    ConfigModule.setConfigPath(args(0))

    val spark = OdsPgwNewConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()
    val kafkaSource = KafkaSource.getKafkaSource(spark, OdsPgwNewConfig.kafkaConsumer)
    val input = kafkaSource.select(col("value").cast(StringType)).as("value")
    val processedData = OdsPgwNewProcessor.process(input)
    HiveSink.hiveSink(processedData, OdsPgwNewConfig.hive, OdsPgwNewConfig.spark)
  }
}
