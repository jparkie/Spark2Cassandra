package com.github.jparkie.spark.cassandra.conf

import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption
import org.apache.spark.SparkConf
import org.scalatest.{ MustMatchers, WordSpec }

class SparkCassServerConfSpec extends WordSpec with MustMatchers {
  "SparkCassServerConf" must {
    "be extracted from SparkConf successfully" in {
      val inputSparkConf = new SparkConf()
        .set("spark.cassandra.bulk.server.storage.port", "1")
        .set("spark.cassandra.bulk.server.sslStorage.port", "2")
        .set("spark.cassandra.bulk.server.internode.encryption", "all")
        .set("spark.cassandra.bulk.server.keyStore.path", "test_keystore_path")
        .set("spark.cassandra.bulk.server.keyStore.password", "test_keystore_password")
        .set("spark.cassandra.bulk.server.trustStore.path", "test_truststore_path")
        .set("spark.cassandra.bulk.server.trustStore.password", "test_truststore_password")
        .set("spark.cassandra.bulk.server.protocol", "test_protocol")
        .set("spark.cassandra.bulk.server.algorithm", "test_algorithm")
        .set("spark.cassandra.bulk.server.store.type", "test_store_type")
        .set("spark.cassandra.bulk.server.cipherSuites", "test_cipher_suites_1,test_cipher_suites_2")
        .set("spark.cassandra.bulk.server.requireClientAuth", "true")

      val outputSparkCassServerConf = SparkCassServerConf.fromSparkConf(inputSparkConf)

      outputSparkCassServerConf.storagePort mustEqual 1
      outputSparkCassServerConf.sslStoragePort mustEqual 2
      outputSparkCassServerConf.internodeEncryption mustEqual "all"
      outputSparkCassServerConf.keyStorePath mustEqual "test_keystore_path"
      outputSparkCassServerConf.keyStorePassword mustEqual "test_keystore_password"
      outputSparkCassServerConf.trustStorePath mustEqual "test_truststore_path"
      outputSparkCassServerConf.trustStorePassword mustEqual "test_truststore_password"
      outputSparkCassServerConf.protocol mustEqual "test_protocol"
      outputSparkCassServerConf.algorithm mustEqual "test_algorithm"
      outputSparkCassServerConf.storeType mustEqual "test_store_type"
      outputSparkCassServerConf.cipherSuites mustEqual Set("test_cipher_suites_1", "test_cipher_suites_2")
      outputSparkCassServerConf.requireClientAuth mustEqual true
    }

    "set defaults when no properties set in SparkConf" in {
      val inputSparkConf = new SparkConf()

      val outputSparkCassServerConf = SparkCassServerConf.fromSparkConf(inputSparkConf)

      outputSparkCassServerConf.storagePort mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_STORAGE_PORT.default
      outputSparkCassServerConf.sslStoragePort mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_SSL_STORAGE_PORT.default
      outputSparkCassServerConf.internodeEncryption mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_INTERNODE_ENCRYPTION.default
      outputSparkCassServerConf.keyStorePath mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PATH.default
      outputSparkCassServerConf.keyStorePassword mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PASSWORD.default
      outputSparkCassServerConf.trustStorePath mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER__TRUSTSTORE_PATH.default
      outputSparkCassServerConf.trustStorePassword mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_TRUSTSTORE_PASSWORD.default
      outputSparkCassServerConf.protocol mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_PROTOCOL.default
      outputSparkCassServerConf.algorithm mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_ALGORITHM.default
      outputSparkCassServerConf.storeType mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_STORE_TYPE.default
      outputSparkCassServerConf.cipherSuites mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_CIPHER_SUITES.default
      outputSparkCassServerConf.requireClientAuth mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_REQUIRE_CLIENT_AUTH.default
    }

    "reject invalid internode encryption in SparkConf" in {
      val inputSparkConf = new SparkConf()
        .set("spark.cassandra.bulk.server.internode.encryption", "N/A")

      intercept[IllegalArgumentException] {
        SparkCassServerConf.fromSparkConf(inputSparkConf)
      }
    }

    "getServerEncryptionOptions correctly" in {
      val inputSparkConf = new SparkConf()

      val outputSparkCassServerConf = SparkCassServerConf.fromSparkConf(inputSparkConf)
      val outputServerEncryptionOptions = outputSparkCassServerConf.getServerEncryptionOptions

      outputServerEncryptionOptions.internode_encryption mustEqual
        InternodeEncryption.none
      outputServerEncryptionOptions.keystore mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PATH.default
      outputServerEncryptionOptions.keystore_password mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_KEYSTORE_PASSWORD.default
      outputServerEncryptionOptions.truststore mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER__TRUSTSTORE_PATH.default
      outputServerEncryptionOptions.truststore_password mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_TRUSTSTORE_PASSWORD.default
      outputServerEncryptionOptions.cipher_suites mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_CIPHER_SUITES.default.toArray
      outputServerEncryptionOptions.algorithm mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_ALGORITHM.default
      outputServerEncryptionOptions.store_type mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_STORE_TYPE.default
      outputServerEncryptionOptions.require_client_auth mustEqual
        SparkCassServerConf.SPARK_CASSANDRA_BULK_SERVER_REQUIRE_CLIENT_AUTH.default
    }
  }
}
