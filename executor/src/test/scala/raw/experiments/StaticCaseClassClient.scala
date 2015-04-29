//package raw.experiments
//
//import java.io.{FileOutputStream, ObjectOutputStream}
//import java.nio.file.Paths
//
//import com.google.common.io.Files
//import raw.csv.Student
//
//object StaticCaseClassClient {
//
//  def main(args: Array[String]):Unit  = {
//    val s = new Student("joe", 1980, "some", "aaa")
//    val p =  RawMacros.query("Hello world", s)
//    println(p)
//
//    println(" " + p.getClass)
//    val tmpDir = System.getProperty("java.io.tmpdir")
//    val oos = new ObjectOutputStream(new FileOutputStream(Paths.get(tmpDir, "file.ser").toString))
//    oos.writeObject(p)
//    oos.close
//  }
//}
