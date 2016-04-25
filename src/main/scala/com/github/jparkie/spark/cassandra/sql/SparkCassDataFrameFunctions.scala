package com.github.jparkie.spark.cassandra.sql

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{ RowWriterFactory, SqlRowWriter }
import com.datastax.spark.connector.{ AllColumns, ColumnSelector }
import com.github.jparkie.spark.cassandra.SparkCassBulkWriter
import com.github.jparkie.spark.cassandra.conf.{ SparkCassServerConf, SparkCassWriteConf }
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Extension of [[DataFrame]] with [[bulkLoadToCass()]] function.
 *
 * @param dataFrame The [[DataFrame]] to lift into extension.
 */
class SparkCassDataFrameFunctions(dataFrame: DataFrame) extends Serializable {
  /**
   * SparkContext to schedule [[SparkCassBulkWriter]] Tasks.
   */
  private[sql] val internalSparkContext = dataFrame.sqlContext.sparkContext

  /**
   * Loads the data from [[DataFrame]] to a Cassandra table. Uses the specified column names.
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
    rwf: RowWriterFactory[Row] = SqlRowWriter.Factory): Unit = {
    val sparkCassBulkWriter = SparkCassBulkWriter(
      connector,
      keyspaceName,
      tableName,
      columns,
      sparkCassWriteConf,
      sparkCassServerConf
    )

    internalSparkContext.runJob(dataFrame.rdd, sparkCassBulkWriter.write _)
  }
}
