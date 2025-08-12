package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_sms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object OdsOcsSmsProcessor {
  def process(df: DataFrame): DataFrame = {
    val schema = OdsOcsSmsSchema.schema
    val expectedNumFields = schema.fields.length
//    val processedDF = df
//      .selectExpr("CAST(value AS STRING) AS raw")
//      .withColumn("fields", split(col("raw"), "\\|"))
//      .filter(size(col("fields")) === expectedNumFields)
//      .select(
//        schema.fields.zipWithIndex.map { case (field, idx) =>
//          element_at(col("fields"), idx + 1).cast(field.dataType).as(field.name)
//        }: _*
//      )
//      .withColumn("load_dt", date_format(current_timestamp(), "yyyyMMdd_HHmmss"))
//      .withColumn("day_key", substring(col("CUST_LOCAL_END_DATE_C16"), 0, 8))
//
//    processedDF




        val step1 = df.selectExpr("CAST(value AS STRING) AS raw")
        step1.show(false)
        val step2 = step1.withColumn("fields", split(col("raw"), "\\|"))
        step2.show(false)
    //    val colSize = size(col("fields"))
        step2.select(size(col("fields")).alias("fields_size")).show(false)
        println(expectedNumFields)
        val step3 = step2.filter(size(col("fields")) === expectedNumFields)
        step3.show(false)
        val step4 = step3.select(
                  schema.fields.zipWithIndex.map { case (field, idx) =>
                    element_at(col("fields"), idx + 1).cast(field.dataType).as(field.name)
                  }: _*
                )
        step4.show(false)
        val step5 = step4.withColumn("load_dt", date_format(current_timestamp(), "yyyyMMdd_HHmmss"))
        step5.show(false)
        val step6 = step5.withColumn("day_key", substring(col("CUST_LOCAL_END_DATE_C16"), 0, 8))

        step6.show(false)



        step6
  }
}
