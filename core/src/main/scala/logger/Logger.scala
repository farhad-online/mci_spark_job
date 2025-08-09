package ir.mci.dwbi.bigdata.spark_job.core.logger

import org.apache.logging.log4j.{LogManager, Logger => Log4jLogger}

trait Logger {
  protected val logger: Log4jLogger = LogManager.getLogger(getClass)
}