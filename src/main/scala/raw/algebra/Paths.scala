package raw.algebra

/** Path
  */
sealed abstract class Path

case class BoundArg(a: Arg) extends Path

case class ClassExtent(name: String) extends Path

case class InnerPath(p: Path, name: String) extends Path
