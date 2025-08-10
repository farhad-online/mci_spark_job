package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_mon

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsOcsMonProcessorTest extends AnyFunSuite with Matchers {

  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsOcsMonProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "481100000136002106|0|N|N|202508030150|0|0|0|A|0|20250803015856|20250803015856|20250803015856|14040512052856|14040512052856|14040512052856|S|14001|4|S|145509530004007842|145509430010546308|145509630000255651|12294431|909/1441226|989169498589|20250701||0|10101|0|14550||1006|1|1|5|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|909/1441226|145509630000255651|145509430010546308|12294431|0|0|105413|20250701|0|9169498589|0||||||||||||||||||||||||||||||||||||||||||||||||||F|S|9169498589|481100000021704660|5007|13142851584|13142851584|1106|4|13142851584|704164|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||3|620091|0|1|0,704164,,0;||2|0|0||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{S,145509530004007842,5007,481100000021704660,13142851584,13142851584,20250803015856,20251002015856,20251002015856,1106,4}}}|||||||||{{{2,2,000000000000000000000000,000000000000000000000000,20181204195959,20080510175100,20080510175100,,,20251101202959,20251101202959,20270217202959,20270217202959,20370101000000,20361231235959,,,,,,,,,,}}}|1|300001|0|0|14040512052856|14040710052856|2|60|1|14040512052856|14040710052856|704164|704164|925441549508|0|0|20250723000000||1|A|1000000||||432113967078744|0|0|0|-1|13001192||||mon_187_345_10101_14040512052856_74112.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsOcsMonProcessor.process(inputData)

    // Show processed DataFrame
    println("Processed DataFrame:")
    resultDF.show(false)

    val results = resultDF.collect()

    // Only one valid row should remain
    results.length shouldBe 1

    val row = results.head

    //    // Check column values
    //    row.getAs[String]("id") shouldBe "1"
    //    row.getAs[String]("CUST_LOCAL_END_DATE_C16") shouldBe "2023-08-09"
    //    row.getAs[String]("amount") shouldBe "100"

    // load_dt column should exist and not be empty
    val loadDt = row.getAs[String]("load_dt")
    loadDt should not be empty

    // day_key column should be valid (not null or "-1")
    val dayKey = row.getAs[String]("day_key")
    dayKey should not be null
    dayKey should not be "-1"
  }

  // Optional: stop SparkSession after tests
  def afterAll(): Unit = {
    spark.stop()
  }
}

