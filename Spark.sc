import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._


val conf = new SparkConf().setMaster("local[2]").setAppName("dataFrame")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
val df = sqlContext.jsonFile("C:\\cygwin64\\home\\nuno\\code\\raw\\src\\test\\data\\smokeTest\\students.csv")

