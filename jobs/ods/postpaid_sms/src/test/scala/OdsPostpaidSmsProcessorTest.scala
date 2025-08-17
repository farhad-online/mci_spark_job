package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_sms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsPostpaidSmsProcessorTest extends AnyFunSuite with Matchers {

  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsPostpaidSmsProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "462300000766347819|0|N|N|202508040830|0|0|0|A|0|20250804083326|20250804083326|20250804083326|14040513120326|14040513120326|14040513120326|E|11002|21|S|209259520006681045|209259420007158768|209259620012019932|38830496|001/4298950|989124145435|20250701|smpp;onlinecharging-1-v500r024c20spc001-988c469d7-kbdhz;145877093;21593681|0|10101|20925|20925|989198277636|1101|1|1|3|0|0|0|0|137900|0|0|0|137900|0|137900|8900|0|0|0|001/4298950|209259620012019932|209259420007158768|38830496|1|137900|2612451624|20250701|0||0||||||||||||||||||||||||||||||||||||||||||||||||||C|A|001/4298950|7000120000003300662|1001|2612451624|137900|1069|0|2612451624||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||620031|100|001/4298950|1|1001|97900|2612451624|1468256260764769296|1|8900|B|98000501|100|001/4298950|1|1001|40000|2612451624|0|0|0|B|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||3|620031|0|1|0,100,,89000;||0|89000|89000|3|98000501|0|1|0,100,,40000;||0|40000|40000|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{462300009738024298,462300000766347819,0,209259620012019932,7000120000003300662,1,1001,620031,100,0,97900,1468256260764769296,1,8900,0,0,0,0,0,0,1069,1069,B,209259420007158768,20250701,20250701,1.0000},{462300009738024299,462300000766347819,0,209259620012019932,7000120000003300662,1,1001,98000501,100,0,40000,0,0,0,0,0,0,0,0,0,1069,1069,B,209259420007158768,20250701,20250701,1.0000}}}||||{{{S,209259520006681045,2,20250804000000,20250805000000,1,4987}}}||{{{209259620012019932,7000120000003300662,20250723000000,20250823000000,137900,2612451624,1001,2736000000}}}||||||989124145435|989198277636|432113958174060||989198277636||1|0|989110280||||20250804083326|0||0|67302724||44|9891100500|0|2|100|1|0|2|0|2|98|999|1|98|21|1|98|999|1|98|999|1|C_SMS|0100000|0|0||||1|1|1|1|||||||0|20250723000000|100|0|0|0|0||8|0|2|0|8900|0|0|1320||||osms_138_344_10101_14040513120204_55707.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsPostpaidSmsProcessor.process(inputData)

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
