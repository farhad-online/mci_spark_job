package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage

import ir.mci.dwbi.bigdata.spark_job.core.configs.SparkJobConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.kafka.KafkaConsumerConfig

object AllUsageConfig extends SparkJobConfig {
  override val configPrefix: String = "all_usage"
  override lazy val kafkaConsumer: KafkaConsumerConfig = KafkaConsumerConfig.getEmpty
}
