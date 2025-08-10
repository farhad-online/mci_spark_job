package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_data

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsOcsDataProcessorTest extends AnyFunSuite with Matchers {

  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsOcsDataProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
    "494500001006116655|1|N|N|202508030750|0|0|0|A|0|20250804070736|20250804055149|20250804070736|14040513103736|14040513092149|14040513103736|D|11006|31|S|161160000214571067|161160000214570281|161160000214570282|11000079224063|11000074744686|989182759314|20250701|gyegrhb01.epc.mnc011.mcc432.3gppnetwork.org;3963194016;0;451712180|0|10101|0|16116||1003|307|0|1|1106|40243|39936|2|0|0|0|0|0|0|0|0|0|0|0|11000074744686|161160000214570282|161160000214570281|11000079224063|0|0|263028626|20250701|||0|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||620167|504143|11000074744686|0|2000|0|263028626|0|0|0|C||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||2|620167|17694|40243|0,504143,,11700;|0,504143,,0;|2|0|11700||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{494500009146288452,494500001006116655,1,161160000214570282,719000000047208198,0,2000,620167,504143,0,0,0,0,0,0,0,0,0,0,0,1069,1069,C,161160000214570281,20250701,20250701,1.0000}}}|||{{{S,161160000214571067,1753037030,16457833794,40243,20250725233113,20250824233113,1106}}}||||||||{{{2,2,000000000000000000000000,000000000000000000000000,,,20201018132044,,,,20251102202959,,20260131202959,,20360129202959,,,,,,,,,,}}}|989182759314|mcinet||432112909530634|10.134.66.82|10.134.66.68|10.134.66.82|43211A4EB0161F40B|20250804070736|3|40243|9134|31109|0|8622440696957601|||||08-6c090010624e00195460|451856507||0|1|300044|0|1|0|98|999|1|98|999|1||1|||20250723000000|504143|1000000|6|0|10|0|0|0|21.186.76.107|0|0|11700|1361|504143|2A02:4540:0068:9566:0000:0000:0000:0000||0|||||data_252_345_10101_14040513103019_61975.unl",      // valid row
    "2|2023-08-09",          // missing field
    "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsOcsDataProcessor.process(inputData)

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
