package com.github.jparkie.spark.cassandra

import grizzled.slf4j.Logging
import org.apache.cassandra.utils.OutputHandler
import org.slf4j.Logger

/**
 * A wrapper for [[Logger]] for [[com.github.jparkie.spark.cassandra.client.SparkCassSSTableLoaderClient]].
 */
class SparkCassOutputHandler extends OutputHandler with Logging {
  override def warn(msg: String): Unit = {
    warn(msg)
  }

  override def warn(msg: String, th: Throwable): Unit = {
    warn(msg, th)
  }

  override def debug(msg: String): Unit = {
    debug(msg)
  }

  override def output(msg: String): Unit = {
    info(msg)
  }
}
