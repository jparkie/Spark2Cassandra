package com.github.jparkie.spark.cassandra.client

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.TimeUnit
import java.util.{ ArrayList, Collections, HashMap, List, Map, Set, UUID }
import com.datastax.driver.core._
import com.github.jparkie.spark.cassandra.conf.SparkCassServerConf
import org.apache.cassandra.config.ColumnDefinition.ClusteringOrder
import org.apache.cassandra.config.{ CFMetaData, ColumnDefinition, SchemaConstants }
import org.apache.cassandra.cql3.ColumnIdentifier
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.dht.{ IPartitioner, Range, Token }
import org.apache.cassandra.io.sstable.SSTableLoader
import org.apache.cassandra.schema.{ CQLTypeParser, SchemaKeyspace, Types }
import org.apache.cassandra.streaming.StreamConnectionFactory
import org.apache.cassandra.tools.BulkLoadConnectionFactory
import org.apache.cassandra.utils.FBUtilities

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

  private[client] val tables: mutable.Map[String, CFMetaData] = mutable.HashMap.empty[String, CFMetaData]

  override def init(keyspace: String): Unit = {
    import scala.collection.JavaConversions._

    val cluster = session.getCluster
    val metadata = cluster.getMetadata
    val tokenRanges = metadata.getTokenRanges.asScala
    val partitioner = FBUtilities.newPartitioner(metadata.getPartitioner)
    val tokenFactory = partitioner.getTokenFactory

    for (tokenRange <- tokenRanges) {
      val endpoints: util.Set[Host] = metadata.getReplicas(Metadata.quote(keyspace), tokenRange)

      val range = new TokenRange(
        tokenFactory.fromString(tokenRange.getStart.getValue.toString),
        tokenFactory.fromString(tokenRange.getEnd.getValue.toString)
      )

      for (endpoint <- endpoints) {
        addRangeForEndpoint(range, endpoint.getAddress)
      }
    }

    val types = fetchTypes(keyspace, session)
    tables.putAll(fetchTables(keyspace, session, partitioner, types))
  }

  override def stop(): Unit = {
    if (!session.isClosed) {
      session.close()
    }
  }

  private def fetchTypes(keyspace: String, session: Session) = {
    val query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TYPES)
    val types = Types.rawBuilder(keyspace)
    import scala.collection.JavaConversions._
    for (row <- session.execute(query, keyspace)) {
      val name = row.getString("type_name")
      val fieldNames = row.getList("field_names", classOf[String])
      val fieldTypes = row.getList("field_types", classOf[String])
      types.add(name, fieldNames, fieldTypes)
    }
    types.build
  }

  /*
     * The following is a slightly simplified but otherwise duplicated version of
     * SchemaKeyspace.createTableFromTableRowAndColumnRows().
     * It might be safer to have a simple wrapper of the driver ResultSet/Row implementing
     * UntypedResultSet/UntypedResultSet.Row and reuse the original method.
     *
     * Note: It is not safe for this class to use static methods from SchemaKeyspace (static final fields are ok)
     * as that triggers initialization of the class, which fails in client mode.
     */
  private def fetchTables(keyspace: String, session: Session, partitioner: IPartitioner, types: Types): mutable.Map[String, CFMetaData] = {
    val tables = new util.HashMap[String, CFMetaData]
    val query = String.format(
      "SELECT * FROM %s.%s WHERE keyspace_name = ?",
      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES
    )
    import scala.collection.JavaConversions._
    for (row <- session.execute(query, keyspace)) {
      val name = row.getString("table_name")
      tables.put(name, createTableMetadata(keyspace, session, partitioner, false, row, name, types))
    }
    tables
  }

  private def createTableMetadata(keyspace: String, session: Session, partitioner: IPartitioner, isView: Boolean, row: Row, name: String, types: Types) = {
    val id = row.getUUID("id")
    val flags = if (isView) Collections.emptySet
    else CFMetaData.flagsFromStrings(row.getSet("flags", classOf[String]))
    val isSuper = flags.contains(CFMetaData.Flag.SUPER)
    val isCounter = flags.contains(CFMetaData.Flag.COUNTER)
    val isDense = flags.contains(CFMetaData.Flag.DENSE)
    val isCompound = isView || flags.contains(CFMetaData.Flag.COMPOUND)
    val columnsQuery = String.format(
      "SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.COLUMNS
    )
    val defs = new util.ArrayList[ColumnDefinition]
    import scala.collection.JavaConversions._
    for (colRow <- session.execute(columnsQuery, keyspace, name)) {
      defs.add(createDefinitionFromRow(colRow, keyspace, name, types))
    }
    val metadata = CFMetaData.create(keyspace, name, id, isDense, isCompound, isSuper, isCounter, isView, defs, partitioner)
    val droppedColumnsQuery = String.format(
      "SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.DROPPED_COLUMNS
    )
    val droppedColumns = new util.HashMap[ByteBuffer, CFMetaData.DroppedColumn]
    import scala.collection.JavaConversions._
    for (colRow <- session.execute(droppedColumnsQuery, keyspace, name)) {
      val droppedColumn = createDroppedColumnFromRow(colRow, keyspace)
      droppedColumns.put(UTF8Type.instance.decompose(droppedColumn.name), droppedColumn)
    }
    metadata.droppedColumns(droppedColumns)
    metadata
  }

  private def createDefinitionFromRow(row: Row, keyspace: String, table: String, types: Types) = {
    val order = ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase)
    var `type` = CQLTypeParser.parse(keyspace, row.getString("type"), types)
    if (order eq ClusteringOrder.DESC) `type` = ReversedType.getInstance(`type`)
    val name = ColumnIdentifier.getInterned(`type`, row.getBytes("column_name_bytes"), row.getString("column_name"))
    val position = row.getInt("position")
    val kind = ColumnDefinition.Kind.valueOf(row.getString("kind").toUpperCase)
    new ColumnDefinition(keyspace, table, name, `type`, position, kind)
  }

  private def createDroppedColumnFromRow(row: Row, keyspace: String) = {
    val name = row.getString("column_name")
    val `type` = CQLTypeParser.parse(keyspace, row.getString("type"), Types.none)
    val droppedTime = TimeUnit.MILLISECONDS.toMicros(row.getTimestamp("dropped_time").getTime)
    new CFMetaData.DroppedColumn(name, `type`, droppedTime)
  }

  override def getConnectionFactory: StreamConnectionFactory = {
    new BulkLoadConnectionFactory(
      sparkCassServerConf.storagePort,
      sparkCassServerConf.sslStoragePort,
      sparkCassServerConf.getServerEncryptionOptions,
      false
    )
  }

  override def getTableMetadata(tableName: String): CFMetaData = tables(tableName)
}

object SparkCassSSTableLoaderClient {
  type TokenRange = org.apache.cassandra.dht.Range[org.apache.cassandra.dht.Token]

  case class TableIdentifier(keyspace: String, cfName: String)
}