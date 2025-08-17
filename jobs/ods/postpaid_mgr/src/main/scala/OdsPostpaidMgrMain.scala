package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_mgr

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsPostpaidMgrMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsPostpaidMgrProcessor.process, args)
  }
}
