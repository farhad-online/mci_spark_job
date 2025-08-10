package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_mgr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsOcsMgrProcessorTest extends AnyFunSuite with Matchers {

  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsOcsMgrProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "485500001089393238|0|N|N|202508022130|0|0|0|A|0|20250802213900|20250802213900|20250802213900|14040512010900|14040512010900|14040512010900|M|13002|S|226689540013676132|226689440010534833|226689640003458248|54696766|908/1780622|989907424825|20250701|CBSAR947398180228|0|10101|0|22668|9171488365|1006|1|1|5|0|0|0|0|100000000|0|100000000|100000000|0|0|0|0|0|0|0|908/1780622|226689640003458248|226689440010534833|54696766|0|0|3374|20250701|0||0||||||||||||||||||||||||||||||||||||||||||||||||||B|A|908/1780622|7040000017126016|2000|3374|100000000|1069|2|20370101033000|20370101033000|3374||B|A|908/1780622|7040000017126016|2000|3374|100000000|1069|0|20370101033000|20370101033000|3374|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{226689640003458248,7040000017126016,2000,100003374,100000000,20161204125050,20370101000000,20370101000000,1069,2,},{226689640003458248,7040000017126016,2000,3374,100000000,20161204125050,20370101000000,20370101000000,1069,0,}}}|||||||||||||{{{2,2,000000000000000000000000,000000000000000000000000,20211203125050,20161204124900,20161204124900,,,20251031202959,20251101202959,20270313202959,20270314202959,20360312202959,20360313202959,,,,,,,,,,}}}|989171488365|4052101|0|1|3000121|0|20250723000000||||1000000|1000000|0|708|||0|||947398180230|0||0||3|0|100000000|TransferBalance|CBSAR947398180228|0|||90000000|10000000|0|708||0|20251031235959|20251101235959|20270313235959|20270314235959|20360312235959|20360313235959|432111807771673||0|0|0|||-1||||mgr_151_345_10101_14040512010900_49774.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsOcsMgrProcessor.process(inputData)

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
