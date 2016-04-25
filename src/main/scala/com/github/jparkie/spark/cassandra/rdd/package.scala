package com.github.jparkie.spark.cassandra

import com.datastax.spark.connector.mapper.ColumnMapper
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.universe._

package object rdd {
  /**
   * Implicitly lift a [[RDD]] with [[SparkCassRDDFunctions]].
   *
   * @param rdd A [[RDD]] to lift.
   * @return Enriched [[RDD]] with [[SparkCassRDDFunctions]].
   */
  implicit def sparkCassRDDFunctions[T: ColumnMapper: TypeTag](rdd: RDD[T]): SparkCassRDDFunctions[T] = {
    new SparkCassRDDFunctions[T](rdd)
  }
}
