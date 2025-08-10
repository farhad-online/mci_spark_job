package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_rec

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsOcsRecMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsOcsRecProcessor.process, args)
  }
}
