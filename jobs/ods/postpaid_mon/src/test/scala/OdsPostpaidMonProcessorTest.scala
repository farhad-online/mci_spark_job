package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_mon

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsPostpaidMonProcessorTest extends AnyFunSuite with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsPostpaidMonProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "504800000122745723|0|N|N|202508022030|0|0|0|A|0|20250802220249|20250802203000|20250802203000|14040512013249|14040512000000|14040512000000|S|14001|4|S|172299510008319175|172299410009741131|172299610004847043|50033333|010/1228376|989103067215|20250701||0|10101|0|17229||1006|1|1|5|0|0|0|0|231000|0|0|0|231000|0|231000|21000|0|0|0|010/1228376|172299610004847043|172299410009741131|50033333|1|231000|4197226537|20250701|0||0||||||||||||||||||||||||||||||||||||||||||||||||||C|A|010/1228376|7000110000002559345|1001|4197226537|231000|1069|0|4197226537||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||98000099|107|010/1228376|1|1001|231000|4197226537|1468256260764769296|1|21000|B||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||3|98000099|0|1|0,107,,210000;||0|210000|210000||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{504800010502154053,504800000122745723,0,172299610004847043,7000110000002559345,1,1001,98000099,107,0,231000,1468256260764769296,1,21000,0,0,0,0,0,0,1069,1069,B,172299410009741131,20250701,20250701,1.0000}}}||||||{{{172299610004847043,7000110000002559345,20250723000000,20250823000000,231000,4197226537,1001,6064000000}}}||||||2|107|1|0|14040512000000|14040513000000|2|1|2658|14040512000000|14040513000000|107|107|9010042101228376|0|0|20250723000000|A|0|A|0100000||||432111305536148|0|21000|0|1510|||||omon_180_344_10101_14040512013010_52178.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsPostpaidMonProcessor.process(inputData)

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
