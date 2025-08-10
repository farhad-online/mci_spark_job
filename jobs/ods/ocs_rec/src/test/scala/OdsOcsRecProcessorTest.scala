package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_rec

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsOcsRecProcessorTest extends AnyFunSuite with Matchers {
  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsOcsRecProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "473900001597525054|0|N|N|202508040820|0|0|0|A|0|20250804082702|20250804082549|20250804082702|14040513115702|14040513115549|14040513115702|D|11000|11|S|133179510004049212|133179410003896511|133179610004098744|37643096|916/1798131|989117753804|20250701|see1;1754295935;68645568;180041043|0|10101|0|13317|989394425932|1003|73|73|1|0|0|0|0|1253965|0|1253965|1253965|0|0|0|113997|0|0|0|916/1798131|133179610004098744|133179410003896511|37643096|0|1253965|106784356|20250701|0||0||||||||||||||||||||||||||||||||||||||||||||||||||B|A|916/1798131|7010000007190643|2000|106784356|1253965|1069|0|106784356||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||620093|300006|916/1798131|0|2000|1253965|106784356|1468256260764769296|1|113997|C||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||1|620093|0|73|0,300006,,1139968;||0|1139968|1139968||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{133179610004098744,7010000007190643,2000,106784356,1253965,20100417164043,20370101000000,20370101000000,1069,0,}}}||{{{473900008782446382,473900001597525054,0,133179610004098744,7010000007190643,0,2000,620093,300006,0,1253965,1468256260764769296,1,113997,0,0,0,0,0,0,1069,1069,C,133179410003896511,20250701,20250701,1.0000}}}|||{{{S,133179510004049212,4001,815674138,1253965,20250621203000,20250922203000,300},{S,133179510004049212,353662478,6923,73,20250722203000,20250822203000,1003}}}||||||||{{{2,2,000000000000000000000000,000000000000000000000000,20200414164043,,20100417164900,,,,20251102202959,,20260131202959,,20261028202959,,,,,,,,,,}}}|989117753804|989394425932|432112968115365||09394425932||1|00|989110450|432112011111608|||20250804082536|14|0|0|2|37413838333338383334|352874815081130||||989110450|1|300006|0|0|2|2|98|999|1|98|111|1|98|999|4|98|999|4|C_VOICE|0|1000000|0|0|1|D22|||0|1|0|||||||0|20250723000000|300006|0|0|0|2|113997|0|0|1110|||||||rec_181_344_10101_14040513115359_45568.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsOcsRecProcessor.process(inputData)

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
