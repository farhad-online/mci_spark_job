package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_sms

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsOcsSmsMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsOcsSmsProcessor.process, args)
  }
}
