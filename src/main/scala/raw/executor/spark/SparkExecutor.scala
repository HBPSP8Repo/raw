package raw.executor.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import raw.{QueryError, World, QueryResult}
import raw.algebra.PhysicalAlgebra
import raw.algebra.PhysicalAlgebra.Scan
import raw.executor.Executor

object SparkExecutor extends Executor {

  // Note: SparkExecutor is likely not an object becaue it needs to keep state.
  //       Should an executor return a new world? Or a world diff?

  val conf = new SparkConf().setAppName("RAW Spark")
  val sc = new SparkContext(conf)

  def execute(root: PhysicalAlgebra.AlgebraNode, world: World): Either[QueryError, QueryResult] = ???
//  root match {
//    case ScanLocalFile(path, _) => ??? //sc.textFile(path)
//  }

}
