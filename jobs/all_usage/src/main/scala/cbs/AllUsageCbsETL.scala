package cbs

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
