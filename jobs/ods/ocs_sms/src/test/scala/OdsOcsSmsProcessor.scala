package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_sms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsOcsSmsProcessorTest extends AnyFunSuite with Matchers {
  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsOcsSmsProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "513500000686235800|0|N|N|202508040900|0|0|0|A|0|20250804090340|20250804090340|20250804090340|14040513123340|14040513123340|14040513123340|E|11002|21|S|190569520010743041|190569420010931153|190569620012711994|10056967138|914/1948524|989907428941|20250701|smpp;onlinecharging-2-v500r024c20spc001-5955f978dc-jl92n;585924526;22191692|0|10101|19056|19056|989940838698|1101|1|1|3|0|0|0|0|156600|0|156600|156600|0|0|0|10600|0|0|0|914/1948524|190569620012711994|190569420010931153|10056967138|0|156600|48981481|20250701|0||0||||||||||||||||||||||||||||||||||||||||||||||||||B|A|914/1948524|7020000016238620|2000|48981481|156600|1069|0|48981481||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||620031|3000121|914/1948524|0|2000|116600|48981481|1468256260764769296|1|10600|C|98000501|3000121|914/1948524|0|2000|40000|48981481|0|0|0|C|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||3|620031|0|1|0,3000121,,106000;||0|106000|106000|3|98000501|0|1|0,3000121,,40000;||0|40000|40000|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{190569620012711994,7020000016238620,2000,48981481,156600,20160727081905,20361231203000,20361231203000,1069,0,}}}||{{{513500009616233711,513500000686235800,0,190569620012711994,7020000016238620,0,2000,620031,3000121,0,116600,1468256260764769296,1,10600,0,0,0,0,0,0,1069,1069,C,190569420010931153,20250701,20250701,1.0000},{513500009616233712,513500000686235800,0,190569620012711994,7020000016238620,0,2000,98000501,3000121,0,40000,0,0,0,0,0,0,0,0,0,1069,1069,C,190569420010931153,20250701,20250701,1.0000}}}|||{{{S,190569520010743041,4001,1051188081,156600,20250621203000,20250922203000,300}}}|{{{S,190569520010743041,2,20250804000000,20250805000000,1,478}}}|||||||{{{2,2,000000000000000000000000,000000000000000000000000,20210726081905,,20160727081800,,,,20251102202959,,20270301202959,,20360301202959,,,,,,,,,,}}}|989907428941|989940838698|432113969532013||989940838698||1|0|989110270||||20250804090340|0||0|61B9EA20||2|9891100500|0|1|3000121|0|0|2|0|2|98|511|1|98|241|1|98|999|1|98|999|1|C_SMS|1000000|0|0||||1|1|1|1|||||||0|20250723000000|3000121|0|0|0|0||8|0|2|0|10600|0|0|1320||||sms_125_345_10101_14040513123047_06964.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsOcsSmsProcessor.process(inputData)

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
