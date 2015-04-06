package raw.csv

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class SparkExperiments extends FunSuite {

//  test("spark") {
//    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Test")
//    val spark = new SparkContext(conf)
//
//    val tNames = spark.parallelize(1 until 10000000, 10).map(i => {
//      //      printf("%d] %s\n", i , Thread.currentThread().getName)
//      Thread.currentThread().getName
////                }).collect().toSet
////          }).toLocalIterator.toSet
//    }).distinct().collect().toSet
//
//    println("Threads: " + tNames)
//    spark.stop()
//  }

  test("spark") {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Test")
    val spark = new SparkContext(conf)

    val tNames = spark.parallelize(1 until 10000000, 10).map(i => {
      //      printf("%d] %s\n", i , Thread.currentThread().getName)
      Thread.currentThread().getName
      //                }).collect().toSet
      //          }).toLocalIterator.toSet
    }).distinct().collect().toSet

    println("Threads: " + tNames)
    spark.stop()
  }
}
