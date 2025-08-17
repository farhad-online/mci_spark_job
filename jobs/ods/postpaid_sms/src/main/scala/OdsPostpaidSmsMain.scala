package ir.mci.dwbi.bigdata.spark_job.jobs.ods.postpaid_sms

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsPostpaidSmsMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsPostpaidSmsProcessor.process, args)
  }
}
