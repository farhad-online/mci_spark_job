package ir.mci.dwbi.bigdata.spark_job.core.configs.spark

import ir.mci.dwbi.bigdata.spark_job.core.configs.{ConfigValidation, CustomOptions}
import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger
import org.apache.spark.sql.SparkSession

case class SparkConfig(
                        appName: String,
                        master: String,
                        checkpointLocation: String,
                        outputMode: String,
                        batchMode: String,
                        batchFormat: String,
                        format: String,
                        triggerInterval: Int,
                        options: Map[String, String] = Map.empty,
                      ) extends Logger with CustomOptions[SparkConfig] with ConfigValidation {
  override def validate(): Either[List[String], Unit] = Right(())

  def getSparkConfig: SparkSession.Builder = {
    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)

    options.foreach { case (key, value) =>
      builder.config(key, value)
    }

    builder
  }
}
