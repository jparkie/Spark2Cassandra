package com.github.jparkie.spark.cassandra

import org.apache.cassandra.utils.OutputHandler
import org.slf4j.Logger

/**
 * A wrapper for [[Logger]] for [[com.github.jparkie.spark.cassandra.client.SparkCassSSTableLoaderClient]].
 *
 * @param log A [[Logger]] to wrap.
 */
class SparkCassOutputHandler(log: Logger) extends OutputHandler {
  override def warn(msg: String): Unit = {
    log.warn(msg)
  }

  override def warn(msg: String, th: Throwable): Unit = {
    log.warn(msg, th)
  }

  override def debug(msg: String): Unit = {
    log.debug(msg)
  }

  override def output(msg: String): Unit = {
    log.info(msg)
  }
}
