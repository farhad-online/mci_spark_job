package ir.mci.dwbi.bigdata.spark_job.core.configs.hive

import ir.mci.dwbi.bigdata.spark_job.core.configs.{ConfigValidation, CustomOptions}
import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger

case class HiveConfig(
                       tableName: String,
                       partitionKey: String,
                       options: Map[String, String] = Map.empty
                     ) extends Logger with CustomOptions[HiveConfig] with ConfigValidation {
  override def validate(): Either[List[String], Unit] = Right(())
}

