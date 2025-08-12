package ir.mci.dwbi.bigdata.spark_job.core.utils

import org.apache.logging.log4j.{LogManager, Logger, ThreadContext}
import org.apache.spark.sql.streaming.StreamingQueryListener

object StreamingQueryLogger {
  private val log: Logger = LogManager.getLogger(classOf[StreamingQueryLogger])
}

class StreamingQueryLogger extends StreamingQueryListener {
  import StreamingQueryLogger.log

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    ThreadContext.put("batchId", progress.batchId.toString)
    log.info(s"--STREAMING-QUERY-PROGRESS-- $progress.json")
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}
