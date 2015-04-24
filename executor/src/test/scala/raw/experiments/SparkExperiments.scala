package raw.experiments

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.collection.Map

class SparkExperiments extends FunSuite {

  //  ignore("spark join") {
  //    println("Students:\n" + students.collect().mkString("\n"))
  //    println("Profs:\n" + profs.collect.mkString("\n"))
  //    val res: RDD[(Student, Professor)] = students.cartesian(profs)
  //    println(s"Students x Profs:\n${res.collect.mkString("\n")}")
  //    val filtered = res.filter({ case (s, p) => s.name.last == p.name.last })
  //    println(s"Students x Profs:\n${filtered.collect.mkString("\n")}")
  //  }


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

  //  test("spark query") {
  //    profs
  //      .filter(((arg) => true))
  //      .cartesian(departments.filter(((arg) => true)))
  //      .filter(((arg) => true))
  //      .filter(((arg) => true))
  //      .map(((arg) => {
  //      final class $anon extends scala.AnyRef with Serializable {
  //        def toMap = Map("_1".$minus$greater(_1), "_2".$minus$greater(_2), "_3".$minus$greater(_3), "_4".$minus$greater(_4), "_5".$minus$greater(_5));
  //        val _1 = arg._1.name;
  //        val _2 = arg._1.office;
  //        val _3 = arg._2.name;
  //        val _4 = arg._2.discipline;
  //        val _5 = arg._2.prof
  //      };
  //      new $anon()
  //    })).toLocalIterator.toSet
  //  }

  ignore("spark") {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Test")
    val spark = new SparkContext(conf)

    val tNames: Map[String, Long] = spark.parallelize(1 until 10000000, 10).map(i => {
      Thread.currentThread().getName
    }).countByValue()



    println("Threads: " + tNames)
    spark.stop()
  }
}
