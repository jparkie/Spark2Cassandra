package com.github.jparkie.spark.cassandra.client

import com.github.jparkie.spark.cassandra.CassandraServerSpecLike
import com.github.jparkie.spark.cassandra.conf.SparkCassServerConf
import org.apache.cassandra.tools.BulkLoadConnectionFactory
import org.scalatest.{ MustMatchers, WordSpec }

class SparkCassSSTableLoaderClientSpec extends WordSpec with MustMatchers with CassandraServerSpecLike {
  val testKeyspace = "test_keyspace"
  val testTable = "test_table"

  override def beforeAll(): Unit = {
    super.beforeAll()

    getCassandraConnector.withSessionDo { currentSession =>
      createKeyspace(currentSession, testKeyspace)

      currentSession.execute(
        s"""CREATE TABLE $testKeyspace.$testTable (
           |  test_key VARCHAR PRIMARY KEY,
           |  test_value BIGINT
           |);
         """.stripMargin
      )
    }
  }

  "SparkCassSSTableLoaderClient" must {
    "initialize successfully" in {
      getCassandraConnector.withSessionDo { currentSession =>
        val testSession = currentSession
        val testSparkCassServerConf = SparkCassServerConf()
        val testSparkCassSSTableLoaderClient = new SparkCassSSTableLoaderClient(testSession, testSparkCassServerConf)

        testSparkCassSSTableLoaderClient.init(testKeyspace)
      }
    }

    "ensure tables contain TableIdentifier(testKeyspace, testTable)" in {
      getCassandraConnector.withSessionDo { currentSession =>
        val testSession = currentSession
        val testSparkCassServerConf = SparkCassServerConf()
        val testSparkCassSSTableLoaderClient = new SparkCassSSTableLoaderClient(testSession, testSparkCassServerConf)

        testSparkCassSSTableLoaderClient.init(testKeyspace)

        assert(testSparkCassSSTableLoaderClient.tables
          .contains(testTable))
      }
    }

    "retrieve CFMetaData" in {
      getCassandraConnector.withSessionDo { currentSession =>
        val testSession = currentSession
        val testSparkCassServerConf = SparkCassServerConf()
        val testSparkCassSSTableLoaderClient = new SparkCassSSTableLoaderClient(testSession, testSparkCassServerConf)

        testSparkCassSSTableLoaderClient.init(testKeyspace)

        val outputCFMetaData = testSparkCassSSTableLoaderClient.getTableMetadata(testTable)
        outputCFMetaData.ksName mustEqual testKeyspace
        outputCFMetaData.cfName mustEqual testTable
      }
    }

    "getConnectionFactory successfully" in {
      getCassandraConnector.withSessionDo { currentSession =>
        val testSession = currentSession
        val testSparkCassServerConf = SparkCassServerConf()
        val testSparkCassSSTableLoaderClient = new SparkCassSSTableLoaderClient(testSession, testSparkCassServerConf)

        testSparkCassSSTableLoaderClient.init(testKeyspace)

        val outputConnectionFactory = testSparkCassSSTableLoaderClient
          .getConnectionFactory

        assert(outputConnectionFactory.isInstanceOf[BulkLoadConnectionFactory])
      }
    }

    "close session on stop()" in {
      val testSession = getCassandraConnector.openSession()
      val testSparkCassServerConf = SparkCassServerConf()
      val testSparkCassSSTableLoaderClient = new SparkCassSSTableLoaderClient(testSession, testSparkCassServerConf)

      testSparkCassSSTableLoaderClient.stop()

      assert(testSession.isClosed)
    }
  }
}
