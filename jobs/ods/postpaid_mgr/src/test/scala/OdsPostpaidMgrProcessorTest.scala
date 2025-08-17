package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_mgr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsPostpaidMgrProcessorTest extends AnyFunSuite with Matchers {

  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsPostpaidMgrProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "448500000001725249|0|N|N|202508031420|0|0|0|A|0|20250803142427|20250803142427|20250803142427|14040512175427|14040512175427|14040512175427|M|13012|S|100379540005210266|100379440001019253|100379640009487879|45598890|002/1452937|989133705195|20250701||0|10101|0|10037||1006|1|1|5|0|0|0|0|22000000|0|0|0|22000000|0|22000000|2000000|0|0|0|002/1452937|100379640009487879|100379440001019253|45598890|1|22000000|1775472089|20250701|0||0||||||||||||||||||||||||||||||||||||||||||||||||||C|A|002/1452937|7000140000002246294|1001|1775472089|22000000|1069|0|19700101000000|20250823000000|1775472089||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||98000379|0|002/1452937|1|1001|22000000|1775472089|1468256260764769296|0|2000000|B||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||5|98000379|0|0|||0|22000000|0||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{448500008620582136,448500000001725249,0,100379640009487879,7000140000002246294,1,1001,98000379,0,0,22000000,1468256260764769296,0,2000000,0,0,0,0,0,0,1069,1069,B,100379440001019253,20250701,20250701,1.0000}}}||||||{{{100379640009487879,7000140000002246294,20250723000000,20250823000000,22000000,1775472089,1001,1804800000}}}||||||989133705195|0|0|2|100|1|20250723000000||||0100000|0100000|0|698|||0|||931577955864|0||0||3|0|0||1952012694566531073|22000000|||0|0|0|698||0|20370101000000|20370101000000|20370101000000|20370101000000|20370101000000|20370101000000|432112965197627||0|2000000|0||R127|1520||||omgr_135_344_10101_14040512175257_41047.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsPostpaidMgrProcessor.process(inputData)

    // Show processed DataFrame
    println("Processed DataFrame:")
    resultDF.show(false)

    val results = resultDF.collect()

    // Only one valid row should remain
    results.length shouldBe 1

    val row = results.head

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
