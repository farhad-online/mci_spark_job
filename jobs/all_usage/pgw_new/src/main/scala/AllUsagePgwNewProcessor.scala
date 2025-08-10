package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.pgw_new

import ir.mci.dwbi.bigdata.spark_job.core.utils.SparkUDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, size, split, substring, trim}
import org.apache.spark.sql.types.{IntegerType, LongType}

object AllUsagePgwNewProcessor {
  def process(df: DataFrame): DataFrame = {
    df
      .withColumn("split_value", split(col("value"), "\\|"))
      .filter(size(col("split_value")).===(71))
      .select(
        trim(col("split_value")(8)).as("a_number"),
        lit("").as("b_number"),
        concat(lit("0"), trim(col("split_value")(25))).cast(IntegerType).as("duration"),
        lit("p").as("cdr_type"),
        trim(col("split_value")(13)).as("imei"),
        trim(col("split_value")(10)).as("imsi"),
        SparkUDF.getMsLocationPGWUDF(trim(col("split_value")(14)), trim(col("split_value")(15)), trim(col("split_value")(16))).as("ms_location"),
        concat(lit("0"), trim(col("split_value")(27))).cast(LongType).as("usage"),
        trim(col("split_value")(39)).as("rat_id"),
        trim(substring(col("split_value")(23), 9, 6)).as("start_time"),
        SparkUDF.toJalaliUDF(trim(substring(col("split_value")(23), 1, 8))).cast(IntegerType).as("start_date")
      )
  }
}
