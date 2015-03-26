import java.nio.file.{Path, Paths}

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class SparkSQL2Test extends FunSuite with LazyLogging {

  val conf = new SparkConf().setMaster("local[2]").setAppName("Test")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val baseDirectory: Path = Paths.get( """C:\cygwin64\home\nuno\code\raw\src\test\resources\data""")

  val moviesDF: DataFrame = sqlContext.jsonFile(baseDirectory.resolve("movies.json").toString)
  val actorsDF: DataFrame = sqlContext.jsonFile(baseDirectory.resolve("actors.json").toString)
  val actorsSpousesDF: DataFrame = sqlContext.jsonFile(baseDirectory.resolve("actorsSpouses.json").toString)

  test("simple") {
    //    isOK( """for (a <- list(1)) yield sum a""", 1)
    println("\nMovies schema: " + moviesDF.schema.treeString);
    println("** Movies:"); moviesDF.show()
    println("** Actors:"); actorsDF.show()
    println("** Spouses:"); actorsSpousesDF.show()

    actorsDF.join(actorsSpousesDF, actorsDF("name") === actorsSpousesDF("name")).show()
    actorsDF.select(actorsDF("name"), actorsDF("born") + 1).show()
  }
}
