package raw.experiments

case class Small(val a1: Int, val b2: Int)

case class Big(val a1: Int,
               val b2: Int,
               val c3: Int,
               val d4: Int,
               val e5: Int,
               val f6: Int,
               val g7: Int,
               val b8: Int,
               val a9: Int,
               val b10: Int,
               val a11: Int,
               val b12: Int,
               val a13: Int,
               val b14: Int,
               val a15: Int,
               val b16: Int,
               val a17: Int,
               val b18: Int,
               val a19: Int,
               val b20: Int,
               val a21: Int,
               val b22: Int,
               val a23: Int,
               val b24: Int,
               val b25: Int,
               val a26: Int,
               val b27: Int,
               val a28: Int,
               val b29: Int,
               val b30: Int,
               val a31: Int,
               val b32: Int,
               val a33: Int,
               val b34: Int,
               val b35: Int,
               val a36: Int,
               val b37: Int,
               val a38: Int,
               val b39: Int
                )

object BigCaseClasses {
  def tryMatch(x: AnyRef) = x match {
    case Big(1, b2, c3, d4, e5, f6, g7, b8, a9, b10, a11, b12, a13, b14, a15, b16, a17, b18, a19, b20, a21, b22, _, b24, b25, a26, b27, a28, b29, b30, a31, b32, a33, b34, b35, a36, b37, a38, b39) =>
      println("Matched"); println(s"Values: 1 $b2 $a38")
    case Big(2, b2, c3, d4, e5, f6, g7, b8, a9, b10, a11, b12, a13, b14, a15, b16, a17, b18, a19, b20, a21, b22, _, b24, b25, a26, b27, a28, b29, b30, a31, b32, a33, b34, b35, a36, b37, a38, b39) =>
      println("Matched"); println(s"Values: 1 $b2 $a38")
    case Small(a1, a2) => println(s"Values: $a1 $a2")
    case s: String => println(s"String: $s")
    case _ => println("???")
  }

  def main(args: Array[String]) {
    val a: Big = Big(1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 1, 2, 1, 2, 3, 4, 1, 2, 1, 2, 3, 4, 1, 2, 1, 2, 3)
    println("Big: " + a)
    val b = Small(2, 3)
    tryMatch(a)
    tryMatch(b)
    tryMatch("Puff")
    tryMatch(new Integer(4))
  }
}

