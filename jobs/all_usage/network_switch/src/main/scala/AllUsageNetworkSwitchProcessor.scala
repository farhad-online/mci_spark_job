package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage.network_switch

import ir.mci.dwbi.bigdata.spark_job.core.utils.SparkUDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, length, lit, substring, trim}
import org.apache.spark.sql.types.IntegerType

object AllUsageNetworkSwitchProcessor {
  def process(df: DataFrame): DataFrame = {
    df
      .filter(length(col("value")) >= 180)
      .filter(substring(col("value"), 62, 4).rlike("20[0-9][0-9]"))
      .filter(substring(col("value"), 66, 2).rlike("[0-1][0-9]"))
      .filter(substring(col("value"), 68, 2).rlike("[0-3][0-9]"))
      .select(
        trim(substring(col("value"), 27, 15)).as("a_number"),
        trim(substring(col("value"), 43, 15)).as("b_number"),
        concat(lit("0"), substring(col("value"), 76, 6)).cast(IntegerType).as("duration"),
        trim(substring(col("value"), 11, 1)).as("cdr_type"),
        trim(substring(col("value"), 166, 16)).as("imei"),
        trim(substring(col("value"), 12, 15)).as("imsi"),
        trim(substring(col("value"), 82, 15)).as("ms_location"),
        lit(0L).as("usage"),
        lit("").as("rat_id"),
        trim(substring(col("value"), 70, 6)).as("start_time"),
        SparkUDF.toJalaliUDF(trim(substring(col("value"), 62, 8))).as("start_date"),
      )
  }
}
