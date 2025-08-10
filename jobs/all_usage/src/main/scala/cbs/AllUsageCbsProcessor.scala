package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.cbs

import ir.mci.dwbi.bigdata.spark_job.core.utils.SparkUDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object AllUsageCbsProcessor {
  def process(df: DataFrame): DataFrame = {
    df
      .withColumn("split_value", split(col("value"), "\\|"))
      .filter(size(col("split_value")).>=(506))
      .select(
        trim(col("split_value")(25)).as("a_number"),
        lit("").as("b_number"),
        SparkUDF.getDiffTimeUDF(col("split_value")(14), col("split_value")(15)).as("duration"),
        lit("c").as("cdr_type"),
        trim(col("split_value")(505)).as("imei"),
        trim(col("split_value")(494)).as("imsi"),
        SparkUDF.getMsLocationCBSUDF(col("split_value")(498)).as("ms_location"),
        concat(lit("0"), col("split_value")(38)).cast(LongType).as("usage"),
        lit("").as("rat_id"),
        substring(col("split_value")(14), 9, 6).as("start_time"),
        substring(col("split_value")(14), 1, 8).cast(IntegerType).as("start_date")
      )
  }
}
