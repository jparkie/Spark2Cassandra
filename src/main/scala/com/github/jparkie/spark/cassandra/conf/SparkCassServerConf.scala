package com.github.jparkie.spark.cassandra.conf

import com.github.jparkie.spark.cassandra.util.SparkCassConfParam
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption
import org.apache.spark.SparkConf

/**
 * Server encryption settings for [[com.github.jparkie.spark.cassandra.client.SparkCassSSTableLoaderClient]]
 * to communicate with other Cassandra nodes utilizing the Transport Layer.
 *
 * @param storagePort The 'storage_port' defined in cassandra.yaml.
 * @param sslStoragePort The 'ssl_storage_port' defined in cassandra.yaml.
 * @param internodeEncryption The 'server_encryption_options:internode_encryption' defined in cassandra.yaml.
 * @param keyStorePath The 'server_encryption_options:keystore' defined in cassandra.yaml.
 * @param keyStorePassword The 'server_encryption_options:keystore_password' defined in cassandra.yaml.
 * @param trustStorePath The 'server_encryption_options:truststore' defined in cassandra.yaml.
 * @param trustStorePassword The 'server_encryption_options:truststore_password' defined in cassandra.yaml.
 * @param protocol The 'server_encryption_options:protocol' defined in cassandra.yaml.
 * @param algorithm The 'server_encryption_options:algorithm' defined in cassandra.yaml.
 * @param storeType The 'server_encryption_options:store_type' defined in cassandra.yaml.
 * @param cipherSuites The 'server_encryption_options:cipher_suites' defined in cassandra.yaml.
 * @param requireClientAuth The 'server_encryption_options:require_client_auth' defined in cassandra.yaml.
 */
case class SparkCassServerConf(
  storagePort:         Int         = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_STORAGE_PORT.default,
  sslStoragePort:      Int         = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_SSL_STORAGE_PORT.default,
  internodeEncryption: String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_INTERNODE_ENCRYPTION.default,
  keyStorePath:        String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PATH.default,
  keyStorePassword:    String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PASSWORD.default,
  trustStorePath:      String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER__TRUSTSTORE_PATH.default,
  trustStorePassword:  String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_TRUSTSTORE_PASSWORD.default,
  protocol:            String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_PROTOCOL.default,
  algorithm:           String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_ALGORITHM.default,
  storeType:           String      = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_STORE_TYPE.default,
  cipherSuites:        Set[String] = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_CIPHER_SUITES.default,
  requireClientAuth:   Boolean     = SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_REQUIRE_CLIENT_AUTH.default
) extends Serializable {
  require(
    SparkCassServerConf.AllowedInternodeEncryptions.contains(internodeEncryption),
    s"Invalid value of spark.cassandra.bulk.server.internode.encryption: $internodeEncryption. " +
      s"Expected any of ${SparkCassServerConf.AllowedInternodeEncryptions.mkString(", ")}."
  )

  private[cassandra] def getServerEncryptionOptions: ServerEncryptionOptions = {
    val actualInternodeEncryption = internodeEncryption match {
      case "all"  => InternodeEncryption.all
      case "none" => InternodeEncryption.none
      case "dc"   => InternodeEncryption.dc
      case "rack" => InternodeEncryption.rack
    }

    val encryptionOptions = new ServerEncryptionOptions()
    encryptionOptions.internode_encryption = actualInternodeEncryption
    encryptionOptions.keystore = keyStorePath
    encryptionOptions.keystore_password = keyStorePassword
    encryptionOptions.truststore = trustStorePath
    encryptionOptions.truststore_password = trustStorePassword
    encryptionOptions.cipher_suites = cipherSuites.toArray
    encryptionOptions.protocol = protocol
    encryptionOptions.algorithm = algorithm
    encryptionOptions.store_type = storeType
    encryptionOptions.require_client_auth = requireClientAuth

    encryptionOptions
  }
}

object SparkCassServerConf {
  val AllowedInternodeEncryptions = Set(
    "all",
    "none",
    "dc",
    "other"
  )

  val SPARK_CASSANDRA_BULK_SERVER_STORAGE_PORT = SparkCassConfParam[Int](
    name = "spark.cassandra.connection.bulk.server.storage.port",
    default = 7000
  )

  val SPARK_CASSANDRA_BULK_SERVER_SSL_STORAGE_PORT = SparkCassConfParam[Int](
    name = "spark.cassandra.connection.bulk.server.sslStorage.port",
    default = 7443
  )

  val SPARK_CASSANDRA_BULK_SERVER_INTERNODE_ENCRYPTION = SparkCassConfParam[String](
    name = "spark.cassandra.connection.bulk.server.internode.encryption",
    default = "all"
  )

  val SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PATH = SparkCassConfParam[String](
    name = "spark.cassandra.connection.ssl.keyStore.path",
    default = "conf/.keystore"
  )

  val SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PASSWORD = SparkCassConfParam[String](
    name = "spark.cassandra.connection.ssl.keyStore.password",
    default = "cassandra"
  )

  val SPARK_CASSANDRA_BULK_SERVER__TRUSTSTORE_PATH = SparkCassConfParam[String](
    name = "spark.cassandra.connection.ssl.trustStore.path",
    default = "conf/.truststore"
  )

  val SPARK_CASSANDRA_BULK_SERVER_TRUSTSTORE_PASSWORD = SparkCassConfParam[String](
    name = "spark.cassandra.connection.ssl.trustStore.password",
    default = "cassandra"
  )

  val SPARK_CASSANDRA_BULK_SERVER_PROTOCOL = SparkCassConfParam[String](
    name = "spark.cassandra.bulk.server.protocol",
    default = "TLS"
  )

  val SPARK_CASSANDRA_BULK_SERVER_ALGORITHM = SparkCassConfParam[String](
    name = "spark.cassandra.bulk.server.algorithm",
    default = "SunX509"
  )

  val SPARK_CASSANDRA_BULK_SERVER_STORE_TYPE = SparkCassConfParam[String](
    name = "spark.cassandra.connection.ssl.keyStore.type",
    default = "JKS"
  )

  val SPARK_CASSANDRA_BULK_SERVER_CIPHER_SUITES = SparkCassConfParam[Set[String]](
    name = "spark.cassandra.connection.ssl.enabledAlgorithms",
    default = Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
  )

  val SPARK_CASSANDRA_BULK_SERVER_REQUIRE_CLIENT_AUTH = SparkCassConfParam[Boolean](
    name = "spark.cassandra.connection.ssl.clientAuth.enabled",
    default = false
  )

  /**
   * Extracts [[SparkCassServerConf]] from a [[SparkConf]].
   *
   * @param sparkConf A [[SparkConf]].
   * @return A [[SparkCassServerConf]] from a [[SparkConf]].
   */
  def fromSparkConf(sparkConf: SparkConf): SparkCassServerConf = {
    val tempStoragePort = sparkConf.getInt(
      SPARK_CASSANDRA_BULK_SERVER_STORAGE_PORT.name,
      SPARK_CASSANDRA_BULK_SERVER_STORAGE_PORT.default
    )
    val tempSSLStoragePort = sparkConf.getInt(
      SPARK_CASSANDRA_BULK_SERVER_SSL_STORAGE_PORT.name,
      SPARK_CASSANDRA_BULK_SERVER_SSL_STORAGE_PORT.default
    )
    val tempInternodeEncryption = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER_INTERNODE_ENCRYPTION.name,
      SPARK_CASSANDRA_BULK_SERVER_INTERNODE_ENCRYPTION.default
    )
    val tempKeyStorePath = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PATH.name,
      SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PATH.default
    )
    val tempKeyStorePassword = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PASSWORD.name,
      SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PASSWORD.default
    )
    val tempTrustStorePath = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER__TRUSTSTORE_PATH.name,
      SPARK_CASSANDRA_BULK_SERVER__TRUSTSTORE_PATH.default
    )
    val tempTrustStorePassword = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER_TRUSTSTORE_PASSWORD.name,
      SPARK_CASSANDRA_BULK_SERVER_TRUSTSTORE_PASSWORD.default
    )
    val tempProtocol = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER_PROTOCOL.name,
      SPARK_CASSANDRA_BULK_SERVER_PROTOCOL.default
    )
    val tempAlgorithm = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER_ALGORITHM.name,
      SPARK_CASSANDRA_BULK_SERVER_ALGORITHM.default
    )
    val tempStoreType = sparkConf.get(
      SPARK_CASSANDRA_BULK_SERVER_STORE_TYPE.name,
      SPARK_CASSANDRA_BULK_SERVER_STORE_TYPE.default
    )
    val tempCipherSuites = sparkConf.getOption(SPARK_CASSANDRA_BULK_SERVER_CIPHER_SUITES.name)
      .map(_.split(",").map(_.trim).toSet).getOrElse(SPARK_CASSANDRA_BULK_SERVER_CIPHER_SUITES.default)
    val tempRequireClientAuth = sparkConf.getBoolean(
      SPARK_CASSANDRA_BULK_SERVER_REQUIRE_CLIENT_AUTH.name,
      SPARK_CASSANDRA_BULK_SERVER_REQUIRE_CLIENT_AUTH.default
    )

    require(
      AllowedInternodeEncryptions.contains(tempInternodeEncryption),
      s"Invalid value of spark.cassandra.bulk.server.internode.encryption: $tempInternodeEncryption. " +
        s"Expected any of ${AllowedInternodeEncryptions.mkString(", ")}."
    )

    SparkCassServerConf(
      storagePort = tempStoragePort,
      sslStoragePort = tempSSLStoragePort,
      internodeEncryption = tempInternodeEncryption,
      keyStorePath = tempKeyStorePath,
      keyStorePassword = tempKeyStorePassword,
      trustStorePath = tempTrustStorePath,
      trustStorePassword = tempTrustStorePassword,
      protocol = tempProtocol,
      algorithm = tempAlgorithm,
      storeType = tempStoreType,
      cipherSuites = tempCipherSuites,
      requireClientAuth = tempRequireClientAuth
    )
  }
}