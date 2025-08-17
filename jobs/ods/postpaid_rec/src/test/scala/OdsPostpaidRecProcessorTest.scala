package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_rec

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OdsPostpaidRecProcessorTest extends AnyFunSuite with Matchers {

  // Create SparkSession for tests
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OdsPostpaidRecProcessorTest")
    .getOrCreate()

  import spark.implicits._

  test("process filters invalid rows and adds columns") {
    val inputData = Seq(
      "465700001633715843|0|N|N|202508040900|0|0|0|A|0|20250804090840|20250804090442|20250804090840|14040513123840|14040513123442|14040513123840|D|11000|10|S|101019520010163443|101019420004481870|101019620007112815|53394032|001/5535557|989128459317|20250701|see2;1754298272;67629193;175256609|0|10101|0|10101|989197767984|1003|238|240|1|0|0|0|0|2635512|0|0|0|2635512|0|2635512|239592|0|0|0|001/5535557|101019620007112815|101019420004481870|53394032|1|2635512|703687919|20250701|0||0||||||||||||||||||||||||||||||||||||||||||||||||||C|A|001/5535557|7000120000002059190|1001|703687919|2635512|1069|0|703687919||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||620093|105|001/5535557|1|1001|2635512|703687919|1468256260764769296|1|239592|B||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||1|620093|0|60|0,105,,598980;||0|598980|598980|1|620093|60|178|0,105,,1796940;||0|1796940|1796940|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||{{{465700009289175863,465700001633715843,0,101019620007112815,7000120000002059190,1,1001,620093,105,0,2635512,1468256260764769296,1,239592,0,0,0,0,0,0,1069,1069,B,101019420004481870,20250701,20250701,1.0000}}}|||{{{S,101019520010163443,153583119,39527,240,20250722203000,20250822203000,1003}}}|||{{{101019620007112815,7000120000002059190,20250723000000,20250823000000,2635512,703687919,1001,2628000000}}}||||||989128459317|989197767984|432113982584183||00989197767984||1|00|9891100722|43211314510450202052|||20250804090433|14|9|0|2|353430454430363131304535|||||9891100722|2|105|1|0|2|2|98|999|1|98|21|1|98|999|1|98|999|1||0|0100000|0|0|0||||1|1|1|||||||0|20250723000000|105|0|0|0|2|239592|0|0|1110|||||||orec_175_344_10101_14040513123717_20699.unl",      // valid row
      "2|2023-08-09",          // missing field
      "3|2023-08-09|200|extra" // extra field
    ).toDF("value")

    // Show processed DataFrame
    println("Processed DataFrame:")
    inputData.show(false)

    val resultDF = OdsPostpaidRecProcessor.process(inputData)

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
