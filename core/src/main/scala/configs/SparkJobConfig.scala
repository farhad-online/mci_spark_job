package ir.mci.dwbi.bigdata.spark_job.core.configs

import ir.mci.dwbi.bigdata.spark_job.core.configs.ConfigModule.ConfigModule
import ir.mci.dwbi.bigdata.spark_job.core.configs.hive.HiveConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.kafka.KafkaConsumerConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.spark.SparkConfig


trait SparkJobConfig extends ConfigModule{
  val configPrefix: String = "ir.mci.dwbi.bigdata"

  lazy val name: String = config getString s"${configPrefix}.name"
  lazy val env: String = config getString s"${configPrefix}.env"
  lazy val enable: Boolean = config getBoolean s"${configPrefix}.enable"
  lazy val spark: SparkConfig = ConfigModule.parseSparkConfig(config.getConfig(s"${configPrefix}.spark"))
  lazy val kafkaConsumer: KafkaConsumerConfig = ConfigModule.parseKafkaConsumerConfig(config.getConfig(s"${configPrefix}.sparkKafkaConsumer"))
  lazy val hive: HiveConfig = ConfigModule.parseHiveConfig(config.getConfig(s"${configPrefix}.sparkHive"))
}
