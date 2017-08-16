package com.github.jparkie.spark.cassandra.client

import java.net.InetAddress

import com.datastax.spark.connector.cql.{ AuthConf, CassandraConnector }
import com.github.jparkie.spark.cassandra.conf.SparkCassServerConf
import grizzled.slf4j.Logging

import scala.collection.mutable

private[cassandra] trait SparkCassSSTableLoaderClientManager extends Serializable with Logging {
  case class SessionKey(
    hosts:               Set[InetAddress],
    port:                Int,
    authConf:            AuthConf,
    sparkCassServerConf: SparkCassServerConf
  ) extends Serializable

  @transient
  private[client] val internalClients = mutable.HashMap.empty[SessionKey, SparkCassSSTableLoaderClient]

  private[client] def buildSessionKey(
    cassandraConnector:  CassandraConnector,
    sparkCassServerConf: SparkCassServerConf
  ): SessionKey = {
    SessionKey(cassandraConnector.hosts, cassandraConnector.port, cassandraConnector.authConf, sparkCassServerConf)
  }

  private[client] def buildClient(
    cassandraConnector:  CassandraConnector,
    sparkCassServerConf: SparkCassServerConf
  ): SparkCassSSTableLoaderClient = {
    val newSession = cassandraConnector.openSession()

    info(s"Created SSTableLoaderClient to the following Cassandra nodes: ${cassandraConnector.hosts}")

    val sparkCassSSTableLoaderClient = new SparkCassSSTableLoaderClient(newSession, sparkCassServerConf)

    sys.addShutdownHook {
      info("Closed Cassandra Session for SSTableLoaderClient.")

      sparkCassSSTableLoaderClient.stop()
    }

    sparkCassSSTableLoaderClient
  }

  /**
   * Gets or creates a [[SparkCassSSTableLoaderClient]] per authorized Cassandra cluster configurations.
   *
   * @param cassandraConnector The [[CassandraConnector]] to create a [[com.datastax.driver.core.Session]].
   * @param sparkCassServerConf Configurations to connect to Cassandra Transport Layer.
   * @return
   */
  def getClient(
    cassandraConnector:  CassandraConnector,
    sparkCassServerConf: SparkCassServerConf
  ): SparkCassSSTableLoaderClient = synchronized {
    val currentSessionKey = buildSessionKey(cassandraConnector, sparkCassServerConf)
    internalClients.getOrElseUpdate(currentSessionKey, buildClient(cassandraConnector, sparkCassServerConf))
  }

  /**
   * Evicts and closes all [[SparkCassSSTableLoaderClient]]s and their [[com.datastax.driver.core.Session]].
   *
   * Note: Utilize for testing purposes only.
   */
  private[cassandra] def evictAll(): Unit = synchronized {
    internalClients.values.foreach(_.stop())
    internalClients.clear()
  }
}

object SparkCassSSTableLoaderClientManager extends SparkCassSSTableLoaderClientManager