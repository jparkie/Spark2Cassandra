package com.github.jparkie.spark.cassandra.client

import com.github.jparkie.spark.cassandra.CassandraServerSpecLike
import com.github.jparkie.spark.cassandra.conf.SparkCassServerConf
import org.scalatest.{ MustMatchers, WordSpec }

class SparkCassSSTableLoaderClientManagerSpec extends WordSpec with MustMatchers with CassandraServerSpecLike {
  "SparkCassSSTableLoaderClientManager" must {
    "return one SparkCassSSTableLoaderClient in getClient()" in {
      val testSparkCassServerConf = SparkCassServerConf()

      SparkCassSSTableLoaderClientManager.getClient(getCassandraConnector, testSparkCassServerConf)
      SparkCassSSTableLoaderClientManager.getClient(getCassandraConnector, testSparkCassServerConf)
      SparkCassSSTableLoaderClientManager.getClient(getCassandraConnector, testSparkCassServerConf)

      assert(SparkCassSSTableLoaderClientManager.internalClients.size == 1)

      SparkCassSSTableLoaderClientManager.evictAll()
    }

    "evictAll() ensures all sessions are stopped and internalClients is empty" in {
      val testSparkCassServerConf = SparkCassServerConf()

      val outputClient = SparkCassSSTableLoaderClientManager.getClient(getCassandraConnector, testSparkCassServerConf)

      SparkCassSSTableLoaderClientManager.evictAll()

      assert(outputClient.session.isClosed)
      assert(SparkCassSSTableLoaderClientManager.internalClients.isEmpty)
    }
  }
}
