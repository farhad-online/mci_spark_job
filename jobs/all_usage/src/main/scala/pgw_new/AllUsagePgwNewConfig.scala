package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.pgw_new

import com.typesafe.config.Config
import ir.mci.dwbi.bigdata.spark_job.core.configs.SparkJobConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.hive.HiveConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.spark.SparkConfig

object AllUsagePgwNewConfig extends SparkJobConfig{
  override val configPrefix: String = "all_usage.pgw_new"
  override val name: String = ""
  override lazy val env: String = ""
  override lazy val enable: Boolean = true
  override lazy val spark: SparkConfig = SparkConfig.getEmpty
  override lazy val hive: HiveConfig = HiveConfig.getEmpty
}
