package ir.mci.dwbi.bigdata.spark_job.core.configs

import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger

trait CustomOptions[T] extends Logger {
  def options: Map[String, String]
}
