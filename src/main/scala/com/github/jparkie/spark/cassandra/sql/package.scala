package com.github.jparkie.spark.cassandra

import org.apache.spark.sql.DataFrame

package object sql {
  /**
   * Implicitly lift a [[DataFrame]] with [[SparkCassDataFrameFunctions]].
   *
   * @param dataFrame A [[DataFrame]] to lift.
   * @return Enriched [[DataFrame]] with [[SparkCassDataFrameFunctions]].
   */
  implicit def sparkCassDataFrameFunctions(dataFrame: DataFrame): SparkCassDataFrameFunctions = {
    new SparkCassDataFrameFunctions(dataFrame)
  }
}
