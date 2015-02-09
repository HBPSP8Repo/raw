package raw

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkTest extends App {

  println("Hello from SparkTest")

  // for (d <- departments, p <- profs, d.prof = p.name) yield set (dept_name := d.name, prof_name := p.name, prof_office := p.office)

  val conf = new SparkConf().setAppName("RAW Spark").setMaster("local")
  val sc = new SparkContext(conf)

  val csvDepartments = sc.textFile("src/test/data/smokeTest/departments.csv")
  val csvProfs = sc.textFile("src/test/data/smokeTest/profs.csv")

  case class Department(name: String, discipline: String, prof: String)
  case class Prof(name: String, office: String)

  val departments = csvDepartments.map{row => val tuple = row.split(","); Department(tuple(0), tuple(1), tuple(2))}
  val profs = csvProfs.map{row => val tuple = row.split(","); Prof(tuple(0), tuple(1))}

  val join1 = departments.map{dept => (dept.prof, dept)}
  val join2 = profs.map{prof => (prof.name, prof)}

  val temp = join1.join(join2)
  val result = temp.map{joined => val key = joined._1; val (dept, prof) = joined._2; Map("dept_name" -> dept.name, "prof_name" -> prof.name, "prof_office" -> prof.office )}

  result.collect().foreach(println)

  println(result.getClass())

  // for (speed_limit <- speed_limits, observation <- radar, speed_limit.location = observation.location, observation.speed < speed_limit.min_speed or observation.speed > speed_limit.max_speed) yield list (name := observation.name, location := observation.location)

//
//  reduce(
//    list,
//    record("name" -> arg(1).person, "location" -> arg(1).location),
//    join(
//      arg(0).location == arg(1).location && arg(1).speed > arg(0).max_speed,
//      select(scan("speed_limits")),
//      select(scan("radar"))))
//}


//
//
//  println(input.count())
//  Thread.sleep(10000)
//  val csv = input.map(line => line.split(",").map(elem => elem.trim))
//  val dep2 = csv.filter(x => x(3) == "dep2")
//  dep2.collect().foreach(println)
}
