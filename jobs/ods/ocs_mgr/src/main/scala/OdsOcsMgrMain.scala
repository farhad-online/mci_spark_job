package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_mgr

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsOcsMgrMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsOcsMgrProcessor.process, args)
  }
}
