package ir.mci.dwbi.bigdata.spark_job.core.utils

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Utility object for writing Spark streaming DataFrames to Hive tables.
 */
object HiveWriter {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Configuration case class to hold Hive writer settings.
   */
  private case class WriterConfig(
                                   tableName: String,
                                   outputMode: String,
                                   format: String,
                                   checkpointLocation: String,
                                   intervalMinutes: Int,
                                   partitionBy: Seq[String],
                                   options: Map[String, String]
                                 )

  /**
   * Writes a streaming DataFrame to a Hive table with the specified configuration.
   *
   * @param df        The streaming DataFrame to write
   * @param appConfig The Typesafe Config containing writer settings
   * @throws IllegalArgumentException if required configuration is missing or invalid
   */
  def write(df: DataFrame, appConfig: Config, processor: DataFrame => DataFrame): Unit = {
    Try {
      extractConfig(appConfig)
    } match {
      case Success(config) =>
        startStreaming(df, config, processor)
      case Failure(e) =>
        logger.error("Failed to parse configuration", e)
        throw new IllegalArgumentException("Invalid configuration", e)
    }
  }

  /**
   * Extracts and validates configuration from the provided Config object.
   */
  private def extractConfig(appConfig: Config): WriterConfig = {
    require(appConfig.hasPath("interval"), "Missing interval configuration")
    require(appConfig.hasPath("checkpointPath"), "Missing checkpointPath configuration")
    require(appConfig.hasPath("hive"), "Missing hive configuration")

    val hiveConfig = appConfig.getConfig("hive")
    require(hiveConfig.hasPath("table"), "Missing hive.table configuration")
    require(hiveConfig.hasPath("outputMode"), "Missing hive.outputMode configuration")
    require(hiveConfig.hasPath("format"), "Missing hive.format configuration")

    val options = hiveConfig.getConfig("options")
    val partitionBy = if (hiveConfig.hasPath("partitionBy")) {
      hiveConfig.getString("partitionBy").split(",").map(_.trim).toSeq
    } else {
      Seq.empty[String]
    }

    WriterConfig(
      tableName = hiveConfig.getString("table"),
      outputMode = hiveConfig.getString("outputMode"),
      format = hiveConfig.getString("format"),
      checkpointLocation = appConfig.getString("checkpointPath"),
      intervalMinutes = appConfig.getInt("interval"),
      partitionBy = partitionBy,
      options = options.entrySet().asScala.map { entry =>
        entry.getKey -> entry.getValue.unwrapped().toString
      }.toMap
    )
  }

  /**
   * Starts the streaming query to write the DataFrame to Hive.
   */
  private def startStreaming(df: DataFrame, config: WriterConfig,  processor: DataFrame => DataFrame): Unit = {
    val query = df.writeStream
      .outputMode(config.outputMode)
      .format(config.format)
      .option("checkpointLocation", config.checkpointLocation)
      .trigger(Trigger.ProcessingTime(s"${config.intervalMinutes} minutes"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          try {
            logger.info(s"Processing batch $batchId for table ${config.tableName}")
            val processedDF = processor(batchDF)
            val writer = processedDF.write
              .format(config.format)
              .options(config.options)
              .mode("append")

            // Apply partitioning if specified
            val partitionedWriter = if (config.partitionBy.nonEmpty) {
              writer.partitionBy(config.partitionBy: _*)
            } else {
              writer
            }

            partitionedWriter.saveAsTable(config.tableName)
            logger.info(s"Successfully completed batch $batchId")
          } catch {
            case e: Exception =>
              logger.error(s"Failed to process batch $batchId", e)
              throw e
          }
        } else {
          logger.info(s"Skipping empty batch $batchId")
        }
      }
      .start()

    try {
      query.awaitTermination()
    } catch {
      case e: Exception =>
        logger.error("Streaming query terminated with error", e)
        throw e
    }
  }
}