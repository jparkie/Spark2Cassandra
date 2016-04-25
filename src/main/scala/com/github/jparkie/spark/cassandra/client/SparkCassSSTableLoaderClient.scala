package com.github.jparkie.spark.cassandra.client

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{ ConsistencyLevel, Metadata, Session }
import com.github.jparkie.spark.cassandra.conf.SparkCassServerConf
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.composites.CellNames
import org.apache.cassandra.db.marshal.{ AbstractType, CompositeType, TypeParser, UTF8Type }
import org.apache.cassandra.db.{ ColumnFamilyType, Keyspace, SystemKeyspace }
import org.apache.cassandra.io.sstable.SSTableLoader
import org.apache.cassandra.streaming.StreamConnectionFactory
import org.apache.cassandra.tools.BulkLoadConnectionFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Coordinates the streaming of SSTables to the proper Cassandra data nodes through automated column family discovery.
 *
 * @param session A prolonged [[Session]] to query system keyspaces.
 * @param sparkCassServerConf Configurations to connect to Cassandra Transport Layer.
 */
class SparkCassSSTableLoaderClient(
  val session:             Session,
  val sparkCassServerConf: SparkCassServerConf
) extends SSTableLoader.Client {
  import SparkCassSSTableLoaderClient._

  private[client] val tables: mutable.Map[TableIdentifier, CFMetaData] = mutable.HashMap.empty[TableIdentifier, CFMetaData]

  override def init(keyspace: String): Unit = {
    val cluster = session.getCluster

    val metaData = cluster.getMetadata
    val metaDataPartitioner = metaData.getPartitioner

    setPartitioner(metaDataPartitioner)

    val tokenRanges = metaData.getTokenRanges.asScala
    val tokenFactory = getPartitioner.getTokenFactory

    for (tokenRange <- tokenRanges) {
      val endpoints = metaData.getReplicas(Metadata.quote(keyspace), tokenRange).asScala

      val range = new TokenRange(
        tokenFactory.fromString(tokenRange.getStart.getValue.toString),
        tokenFactory.fromString(tokenRange.getEnd.getValue.toString)
      )

      for (endpoint <- endpoints) {
        addRangeForEndpoint(range, endpoint.getAddress)
      }
    }

    fetchCFMetaData(keyspace)
  }

  override def stop(): Unit = {
    if (!session.isClosed) {
      session.close()
    }
  }

  override def getCFMetaData(keyspace: String, cfName: String): CFMetaData = {
    tables(TableIdentifier(keyspace, cfName))
  }

  override def getConnectionFactory: StreamConnectionFactory = {
    new BulkLoadConnectionFactory(
      sparkCassServerConf.storagePort,
      sparkCassServerConf.sslStoragePort,
      sparkCassServerConf.getServerEncryptionOptions,
      false
    )
  }

  private def getCFColumnsWithoutCollections: List[String] = {
    val allColumns = CFMetaData.SchemaColumnFamiliesCf.allColumnsInSelectOrder.asScala

    allColumns
      .filter(columnDefinition => !columnDefinition.`type`.isCollection)
      .map(columnDefinition => UTF8Type.instance.getString(columnDefinition.name.bytes))
      .toList
  }

  private def fetchCFMetaData(keyspace: String): Unit = {
    def makeRawAbstractType(comparator: AbstractType[_], subComparator: AbstractType[_]): AbstractType[_] = {
      if (subComparator == null) {
        comparator
      } else {
        CompositeType.getInstance(List[AbstractType[_]](comparator, subComparator).asJava)
      }
    }

    val selectColumns = getCFColumnsWithoutCollections

    val queryStatement = QueryBuilder.select(selectColumns: _*)
      .from(Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF)
      .where(QueryBuilder.eq("keyspace_name", keyspace))
      .setConsistencyLevel(ConsistencyLevel.ONE)

    val cfMetaDataRowIterator = session.execute(queryStatement).iterator.asScala
    for (cfMetaDataRow <- cfMetaDataRowIterator) {
      val cfName = cfMetaDataRow.getString("columnfamily_name")
      val cfId = cfMetaDataRow.getUUID("cf_id")
      val cfType = ColumnFamilyType.valueOf(cfMetaDataRow.getString("type"))
      val cfRawComparator = TypeParser.parse(cfMetaDataRow.getString("comparator"))
      val cfSubComparator = {
        if (cfMetaDataRow.isNull("subcomparator"))
          null
        else
          TypeParser.parse(cfMetaDataRow.getString("subcomparator"))
      }
      val cfIsDense = cfMetaDataRow.getBool("is_dense")
      val cfComparator = CellNames.fromAbstractType(makeRawAbstractType(cfRawComparator, cfSubComparator), cfIsDense)

      tables.put(TableIdentifier(keyspace, cfName), new CFMetaData(keyspace, cfName, cfType, cfComparator, cfId))
    }
  }
}

object SparkCassSSTableLoaderClient {
  type TokenRange = org.apache.cassandra.dht.Range[org.apache.cassandra.dht.Token]

  case class TableIdentifier(keyspace: String, cfName: String)
}