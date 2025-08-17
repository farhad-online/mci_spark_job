package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_data

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsPostpaidDataProcessorTest extends AnyFunSuite with Matchers {
  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsPostpaidDataProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "466000001036573005|1|N|N|202508031540|0|0|0|A|0|20250804043948|20250804034040|20250804043948|14040513080948|14040513071040|14040513080948|D|11006|31|S|207509510008483873|207509410011274885|207509610004830309|50355847|001/2185726|989122396402|20250701|gyegrha02.epc.mnc011.mcc432.3gppnetwork.org;3963224417;0;247244091|0|10101|0|20750||1003|1406|0|1|1106|522240|522240|2|0|0|0|0|0|0|0|0|0|0|522240|001/2185726|207509610004830309|207509410011274885|50355847|1|0|538628459|20250701|0|9122396402|0||||||||||||||||||||||||||||||||||||||||||||||||||F|S|9122396402|466000000021860758|10004|2045943261|522240|1106|0|2045943261|704153|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||620167|704153|001/2185726|1|1001|0|538628459|0|0|0|C||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||2|620167|1771520|522240|0,704153,,1;||1|0|1|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{S,207509510008483873,466000000021860758,10004,2045943261,522240,20250726044957,20250825044957,20250825044957,1106,0,704153,900100126106779415,207509610004830309,1}}}|{{{466000009277168036,466000001036573005,1,207509610004830309,0,1,1001,620167,704153,0,0,0,0,0,0,0,0,0,0,0,1069,1069,C,207509410011274885,20250701,20250701,1.0000}}}||||||||||||989122396402|mcinet||432113933044547|10.134.66.8|10.134.66.36|10.134.66.8|432113221832124|20250804043948|3|522240|522240|0|0|8654020788186004|||||07-23921f9196fefe744bffff00ce005200|247319527||0|2|100|1|1|0|98|999|1|98|999|1||1|||20250723000000|704153|0100000|6|0|2|0|0|0|21.157.215.198|0|0|1|1361||2A02:4540:0025:CD83:0000:0000:0000:0000||0|||||odata_197_344_10101_14040513080030_52922.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsPostpaidDataProcessor.process(inputData)

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
