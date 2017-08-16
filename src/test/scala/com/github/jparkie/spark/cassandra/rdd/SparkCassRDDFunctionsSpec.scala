package com.github.jparkie.spark.cassandra.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._

class SparkCassRDDFunctionsSpec extends WordSpec with Matchers with SharedSparkContext {
  "Package com.github.jparkie.spark.cassandra.rdd" must {
    "lift RDD into SparkCassRDDFunctions" in {
      val testRDD = sc.parallelize(1 to 25)
        .map(currentNumber => (currentNumber.toLong, s"Hello World: $currentNumber!"))

      // If internalSparkContext is available, RDD was lifted.
      testRDD.internalSparkContext
    }
  }
}
