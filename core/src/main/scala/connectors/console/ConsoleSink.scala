package ir.mci.dwbi.bigdata.spark_job.core.connectors.console

import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger
import org.apache.spark.sql.DataFrame


object ConsoleSink extends Logger {
  def consoleSink(df: DataFrame): Unit = {
    df
      .writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()
  }
}

