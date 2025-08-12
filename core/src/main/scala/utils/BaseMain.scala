package ir.mci.dwbi.bigdata.spark_job.core.utils

import org.slf4j.LoggerFactory
import ir.mci.dwbi.bigdata.spark_job.core.configs.HdfsConfig
import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger
import org.apache.spark.sql.DataFrame

object BaseMain extends Logger{

  def run(processor: DataFrame => DataFrame, args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: <program> <config-path>")
      System.exit(1)
    }

    val configPath = args(0)
    logger.info(s"Using config: $configPath")

    val appConfig = HdfsConfig.getConfig(configPath)
    Etl.run(appConfig, processor)
  }
}
