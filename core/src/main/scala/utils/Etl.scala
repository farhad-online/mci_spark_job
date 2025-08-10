package ir.mci.dwbi.bigdata.spark_job.core.utils

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

object Etl {
  def run(appConfig: Config, processor: DataFrame => DataFrame): Unit = {

    val spark = SparkUtils.createSparkSession(appConfig)
    val inputDF = KafkaReader.read(spark, appConfig)
    HiveWriter.write(inputDF,appConfig, processor)
  }
}