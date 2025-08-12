package ir.mci.dwbi.bigdata.spark_job.core.configs.kafka

import ir.mci.dwbi.bigdata.spark_job.core.configs.{ConfigValidation, CustomOptions}
import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger

case class KafkaConsumerConfig(
                          format: String,
                          options: Map[String, String] = Map.empty
                        ) extends Logger with CustomOptions[KafkaConsumerConfig] with ConfigValidation {
  override def validate(): Either[List[String], Unit] = Right(())
}

object KafkaConsumerConfig {
  def getEmpty: KafkaConsumerConfig = {
    KafkaConsumerConfig(
      "",
      Map()
    )
  }
}
