package com.github.jparkie.spark.cassandra.rdd

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.writer.{ DefaultRowWriter, RowWriterFactory }
import com.datastax.spark.connector.{ AllColumns, ColumnSelector }
import com.github.jparkie.spark.cassandra.SparkCassBulkWriter
import com.github.jparkie.spark.cassandra.conf.{ SparkCassServerConf, SparkCassWriteConf }
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe._

/**
 * Extension of [[RDD]] with [[bulkLoadToCass()]] function.
 *
 * @param rdd The [[RDD]] to lift into extension.
 */
class SparkCassRDDFunctions[T: ColumnMapper: TypeTag](rdd: RDD[T]) extends Serializable {
  /**
   * SparkContext to schedule [[SparkCassBulkWriter]] Tasks.
   */
  private[rdd] val internalSparkContext = rdd.sparkContext

  /**
   * Loads the data from [[RDD]] to a Cassandra table. Uses the specified column names.
   *
   * Writes SSTables to a temporary directory then loads the SSTables directly to Cassandra.
   *
   * @param keyspaceName The name of the Keyspace to use.
   * @param tableName The name of the Table to use.
   * @param columns The list of column names to save data to.
   *                Uses only the unique column names, and you must select all primary key columns.
   *                All other fields are discarded. Non-selected property/column names are left unchanged.
   * @param sparkCassWriteConf Configurations to coordinate and to limit writes.
   * @param sparkCassServerConf Configurations to connect to Cassandra Transport Layer.
   */
  def bulkLoadToCass(
    keyspaceName:        String,
    tableName:           String,
    columns:             ColumnSelector      = AllColumns,
    sparkCassWriteConf:  SparkCassWriteConf  = SparkCassWriteConf.fromSparkConf(internalSparkContext.getConf),
    sparkCassServerConf: SparkCassServerConf = SparkCassServerConf.fromSparkConf(internalSparkContext.getConf)
  )(implicit
    connector: CassandraConnector = CassandraConnector(internalSparkContext.getConf),
    rwf: RowWriterFactory[T] = DefaultRowWriter.factory[T]): Unit = {
    val sparkCassBulkWriter = SparkCassBulkWriter(
      connector,
      keyspaceName,
      tableName,
      columns,
      sparkCassWriteConf,
      sparkCassServerConf
    )

    internalSparkContext.runJob(rdd, sparkCassBulkWriter.write _)
  }
}
