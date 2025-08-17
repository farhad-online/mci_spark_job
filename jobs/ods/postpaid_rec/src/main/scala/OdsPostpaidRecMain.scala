package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_rec

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsPostpaidRecMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsPostpaidRecProcessor.process, args)
  }
}
