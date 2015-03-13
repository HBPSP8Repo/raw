package raw

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FunSuite
import raw.executor.reference.ReferenceExecutor

class MyTest extends FunSuite with LazyLogging {
  val world = new World(Map())
  val executor = ReferenceExecutor

  def isOK(q: String, expected: AnyVal): Unit = {
    info(s"$q")

    Query(q, world, executor) match {
      case Left(qe) => println(s"error: $qe")
      case Right(qr) => println(s"Success: $qr")
    }
  }

  test("simple") {
    isOK( """for (a <- list(1)) yield sum a""", 1)
  }
}
