package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_mon

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsOcsMonMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsOcsMonProcessor.process, args)
  }
}
