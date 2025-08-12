package ir.mci.dwbi.bigdata.spark_job.jobs.all_usage

import org.apache.spark.sql.DataFrame


object AllUsageProcessor {
  def process(dfPgwNew: DataFrame, dfNetworkSwitch: DataFrame, dfCbs: DataFrame): DataFrame = {
    dfPgwNew.unionAll(dfNetworkSwitch).unionAll(dfCbs)
  }
}
