package com.github.jparkie.spark.cassandra.util

/**
 * Defines parameter to extract values from SparkConf.
 *
 * @param name The key in SparkConf.
 * @param default The default value to fallback on missing key.
 */
case class SparkCassConfParam[T](name: String, default: T)
