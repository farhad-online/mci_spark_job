package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_mgr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, element_at, size, split, substring}

object OdsOcsMgrProcessor {
  def process(df: DataFrame): DataFrame = {

    val schema = OdsOcsMgrSchema.schema
    val expectedNumFields = schema.fields.length
    val processedDF = df
      .selectExpr("CAST(value AS STRING) AS raw")
      .withColumn("fields", split(col("raw"), "\\|"))
      .filter(size(col("fields")) === expectedNumFields)
      .select(
        schema.fields.zipWithIndex.map { case (field, idx) =>
          element_at(col("fields"), idx + 1).cast(field.dataType).as(field.name)
        }: _*
      )
      .withColumn("load_dt", date_format(current_timestamp(), "yyyyMMdd_HHmmss"))
      .withColumn("day_key", substring(col("CUST_LOCAL_END_DATE_C16"), 0, 8))

    processedDF
  }
}
