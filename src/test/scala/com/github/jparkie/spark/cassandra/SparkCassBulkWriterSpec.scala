package com.github.jparkie.spark.cassandra

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.AllColumns
import com.datastax.spark.connector.writer.{RowWriterFactory, SqlRowWriter}
import com.github.jparkie.spark.cassandra.client.SparkCassSSTableLoaderClientManager
import com.github.jparkie.spark.cassandra.conf.{SparkCassServerConf, SparkCassWriteConf}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest._

import scala.collection.JavaConverters._

class SparkCassBulkWriterSpec extends WordSpec with MustMatchers with CassandraServerSpecLike with SharedSparkContext {
  val testKeyspace = "test_keyspace"
  val testTable = "test_table"

  override def beforeAll(): Unit = {
    super.beforeAll()

    getCassandraConnector.withSessionDo { currentSession =>
      createKeyspace(currentSession, testKeyspace)

      currentSession.execute(
        s"""CREATE TABLE $testKeyspace.$testTable (
            |  test_key BIGINT PRIMARY KEY,
            |  test_value VARCHAR
            |);
         """.stripMargin
      )
    }
  }

  "SparkCassBulkWriter" must {
    "write() successfully" in {
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._

      implicit val testRowWriterFactory: RowWriterFactory[Row] = SqlRowWriter.Factory

      val testCassandraConnector = getCassandraConnector
      val testSparkCassWriteConf = SparkCassWriteConf()
      val testSparkCassServerConf = SparkCassServerConf(
        // See https://github.com/jsevellec/cassandra-unit/blob/master/cassandra-unit/src/main/resources/cu-cassandra.yaml
        storagePort = 7010
      )

      val testSparkCassBulkWriter = SparkCassBulkWriter(
        testCassandraConnector,
        testKeyspace,
        testTable,
        AllColumns,
        testSparkCassWriteConf,
        testSparkCassServerConf
      )

      val testRDD = sc.parallelize(1 to 25)
        .map(currentNumber => (currentNumber.toLong, s"Hello World: $currentNumber!"))
      val testDataFrame = testRDD.toDF("test_key", "test_value")

      sc.runJob(testDataFrame.rdd, testSparkCassBulkWriter.write _)

      getCassandraConnector.withSessionDo { currentSession =>
        val queryStatement = QueryBuilder.select("test_key", "test_value")
          .from(testKeyspace, testTable)
          .limit(25)

        val resultSet = currentSession.execute(queryStatement)

        val outputSet = resultSet.all.asScala
          .map(currentRow => (currentRow.getLong("test_key"), currentRow.getString("test_value")))
          .toMap

        for (currentNumber <- 1 to 25) {
          val currentKey = currentNumber.toLong

          outputSet(currentKey) mustEqual s"Hello World: $currentNumber!"
        }
      }

      SparkCassSSTableLoaderClientManager.evictAll()
    }
  }
}