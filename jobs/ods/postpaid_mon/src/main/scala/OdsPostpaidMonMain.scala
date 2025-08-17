package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_mon

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsPostpaidMonMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsPostpaidMonProcessor.process, args)
  }
}
