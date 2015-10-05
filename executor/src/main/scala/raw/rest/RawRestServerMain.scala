package raw.rest

import com.typesafe.scalalogging.StrictLogging
import org.rogach.scallop.{ScallopOption, ScallopConf}

object RawRestServerMain extends StrictLogging {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      banner("Scala/Spark OQL execution server")
      val executor: ScallopOption[String] = opt[String]("executor", default = Some("scala"), short = 'e')
      val storageDir: ScallopOption[String] = opt[String]("storage-dir", default = None, short = 's')
    }

    val server = new RawRestServer(Conf.executor(), Conf.storageDir.get)
    server.start()
  }
}
