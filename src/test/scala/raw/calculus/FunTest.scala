package raw.calculus

import org.scalatest.FunSuite

class FunTest extends FunSuite {

  def parse(w: World, q: String) = {
    val c = Driver.parse(q)
    assert(w.errors(c).length === 0)
    c
  }

}
