package ir.mci.dwbi.bigdata.spark_job.core.connectors.hive

import ir.mci.dwbi.bigdata.spark_job.core.configs.hive.HiveConfig
import ir.mci.dwbi.bigdata.spark_job.core.configs.spark.SparkConfig
import ir.mci.dwbi.bigdata.spark_job.core.logger.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

object HiveSink extends Logger {
  def hiveSink(df: DataFrame, hiveConfig: HiveConfig, sparkConfig: SparkConfig): Unit = {
    val query = df.writeStream
      .outputMode(sparkConfig.outputMode)
      .format(sparkConfig.format)
      .option("checkpointLocation", sparkConfig.checkpointLocation)
      .trigger(Trigger.ProcessingTime(s"${sparkConfig.triggerInterval} minutes"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.write
            .format(sparkConfig.batchFormat)
            .mode(sparkConfig.batchMode)
            .partitionBy(hiveConfig.partitionKey)
            .saveAsTable(hiveConfig.tableName)
        }
      }.start()

    query.awaitTermination()
  }
}
