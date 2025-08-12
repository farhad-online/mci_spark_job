package ir.mci.dwbi.bigdata.spark_job.core.utils

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object SparkUtils {
  def createSparkSession(config: Config): SparkSession = {
    val sparkConf = new org.apache.spark.SparkConf()
      .setAppName(config.getString("appName"))

    for ((k, v) <- config.getObject("spark").unwrapped.asScala) {
      sparkConf.set(k, v.toString)
    }

    val spark = SparkSession.builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    spark.streams.addListener(new StreamingQueryLogger())
    spark
  }



}