package com.github.jparkie.spark.cassandra

import java.net.InetAddress

import org.apache.cassandra.streaming.StreamEvent._
import org.apache.cassandra.streaming.{ SessionInfo, StreamEvent, StreamEventHandler, StreamState }
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A [[StreamEventHandler]] that logs the current progress of SSTable streaming
 * with completion status, transfer rate, and percentage.
 *
 * @param log A [[Logger]] to write progress.
 */
class SparkCassStreamEventHandler(log: Logger) extends StreamEventHandler {
  class SessionHostMap extends mutable.HashMap[InetAddress, mutable.Set[SessionInfo]]
    with mutable.MultiMap[InetAddress, SessionInfo]

  private val startTime: Long = System.nanoTime()
  private var lastTime: Long = startTime
  private var lastProgress: Long = _

  private var peakRate: Long = _
  private var totalFiles: Long = _

  private val sessionHostMap: SessionHostMap = new SessionHostMap

  override def onFailure(t: Throwable): Unit = {
    // TODO: Investigate onFailure.
    // Do Nothing.
  }

  override def onSuccess(result: StreamState): Unit = {
    // TODO: Investigate onSuccess.
    // Do Nothing.
  }

  override def handleStreamEvent(event: StreamEvent): Unit = {
    import StreamEvent.Type._

    val currentEventType = event.eventType

    (event, currentEventType) match {
      case (sessionPreparedEvent: SessionPreparedEvent, STREAM_PREPARED) =>
        handleStreamPrepared(sessionPreparedEvent)
      case (sessionCompleteEvent: SessionCompleteEvent, STREAM_COMPLETE) =>
        handleStreamComplete(sessionCompleteEvent)
      case (progressEvent: ProgressEvent, FILE_PROGRESS) =>
        handleFileProgress(progressEvent)
      case _ =>
      // Do Nothing.
    }
  }

  private def handleStreamPrepared(sessionPreparedEvent: SessionPreparedEvent): Unit = {
    val currentSessionInfo = sessionPreparedEvent.session
    sessionHostMap.addBinding(currentSessionInfo.peer, currentSessionInfo)

    log.info(s"Session to ${currentSessionInfo.connecting.getHostAddress}.")
  }

  private def handleStreamComplete(sessionCompleteEvent: SessionCompleteEvent): Unit = {
    if (sessionCompleteEvent.success) {
      log.info(s"Stream to ${sessionCompleteEvent.peer.getHostAddress} successful.")
    } else {
      log.info(s"Stream to ${sessionCompleteEvent.peer.getHostAddress} failed.")
    }
  }

  private def handleFileProgress(progressEvent: ProgressEvent): Unit = {
    def mbPerSec(bytes: Long, timeInNano: Long): Long = {
      val SEC_FACTOR = 10E09
      val MB_FACTOR = 1024 * 1024

      val bytesPerNano = bytes.asInstanceOf[Double] / timeInNano
      (bytesPerNano * SEC_FACTOR / MB_FACTOR).asInstanceOf[Long]
    }

    val currentProgressInfo = progressEvent.progress

    val currentTime = System.nanoTime()

    val currentProgressBuilder = new StringBuilder()
    currentProgressBuilder.append("Stream Progress: ")

    var totalProgress = 0L
    var totalSize = 0L

    val updateTotalFiles = totalFiles == 0
    for (currentPeer <- sessionHostMap.keys) {
      currentProgressBuilder.append(s"[${currentPeer.toString}]")
      for (currentSession <- sessionHostMap.getOrElse(currentPeer, Set.empty[SessionInfo])) {
        val currentSize = currentSession.getTotalSizeToSend
        var currentProgress = 0L
        var completionStatus = 0

        if ((currentSession.peer == currentProgressInfo.peer) &&
          (currentSession.sessionIndex == currentProgressInfo.sessionIndex)) {
          currentSession.updateProgress(currentProgressInfo)
        }
        for (existingProgressInfo <- currentSession.getSendingFiles.asScala) {
          if (existingProgressInfo.isCompleted)
            completionStatus += 1

          currentProgress += existingProgressInfo.currentBytes
        }

        totalProgress = totalProgress + currentProgress
        totalSize = totalSize + currentSize

        val currentPercentage = if (currentSize == 0) 100L else currentProgress * 100L / currentSize

        currentProgressBuilder.append(s"-${currentSession.sessionIndex}:")
        currentProgressBuilder.append(s"$completionStatus/${currentSession.getTotalFilesToSend} ")
        currentProgressBuilder.append(f"$currentPercentage%-3d").append("% ")

        if (updateTotalFiles) {
          totalFiles = totalFiles + currentSession.getTotalFilesToSend
        }
      }
    }

    val deltaTime = currentTime - lastTime
    val deltaProgress = totalProgress - lastProgress

    lastTime = currentTime
    lastProgress = totalProgress

    val totalPercentage = if (totalSize == 0) 100L else totalProgress * 100L / totalSize
    val deltaRate = mbPerSec(deltaProgress, deltaTime)

    currentProgressBuilder.append("Total: ")
    currentProgressBuilder.append(f"$totalPercentage%-3d").append("% ")
    currentProgressBuilder.append(f"$deltaRate%-3d").append("MB/s ")

    val averageRate = mbPerSec(totalProgress, currentTime - startTime)
    if (averageRate > peakRate) {
      peakRate = averageRate
    }

    currentProgressBuilder.append(s"(Average: $averageRate MB/s)")

    log.info(currentProgressBuilder.toString.trim)
  }
}
