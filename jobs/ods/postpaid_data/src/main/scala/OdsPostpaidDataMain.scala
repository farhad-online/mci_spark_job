package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_data

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsPostpaidDataMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsPostpaidDataProcessor.process, args)
  }
}
