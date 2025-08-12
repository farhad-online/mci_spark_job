package ir.mci.dwbi.bigdata.spark_job.jobs.ods.ocs_data

import ir.mci.dwbi.bigdata.spark_job.core.utils.BaseMain

object OdsOcsDataMain {
  def main(args: Array[String]): Unit = {
    BaseMain.run(OdsOcsDataProcessor.process, args)
  }
}
