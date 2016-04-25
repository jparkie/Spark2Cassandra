package com.github.jparkie.spark.cassandra

import java.net.{ InetAddress, InetSocketAddress }

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait CassandraServerSpecLike extends BeforeAndAfterAll { this: Suite =>
  // Remove protected modifier because of SharedSparkContext.
  override def beforeAll(): Unit = {
    super.beforeAll()

    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
  }

  // Remove protected modifier because of SharedSparkContext.
  override def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()

    super.afterAll()
  }

  def getClusterName: String = {
    EmbeddedCassandraServerHelper.getClusterName
  }

  def getHosts: Set[InetAddress] = {
    val temporaryAddress =
      new InetSocketAddress(EmbeddedCassandraServerHelper.getHost, EmbeddedCassandraServerHelper.getNativeTransportPort)
        .getAddress

    Set(temporaryAddress)
  }

  def getNativeTransportPort: Int = {
    EmbeddedCassandraServerHelper.getNativeTransportPort
  }

  def getRpcPort: Int = {
    EmbeddedCassandraServerHelper.getRpcPort
  }

  def getCassandraConnector: CassandraConnector = {
    CassandraConnector(hosts = getHosts, port = getNativeTransportPort)
  }

  def createKeyspace(session: Session, keyspace: String): Unit = {
    session.execute(
      s"""CREATE KEYSPACE "$keyspace"
          |WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
       """.stripMargin
    )
  }
}
