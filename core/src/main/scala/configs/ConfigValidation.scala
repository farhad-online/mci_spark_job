package ir.mci.dwbi.bigdata.spark_job.core.configs

import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger

trait ConfigValidation extends Logger {
  def validate(): Either[List[String], Unit]
}
