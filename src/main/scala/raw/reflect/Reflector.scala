package raw.reflect

import scala.reflect.runtime.{universe => ru}

object Reflector {

  import ru._

  private def getTypeOf[T](x: T)(implicit tag: TypeTag[T]) = typeOf[T]

  def getRawType[T](x: T)(implicit tag: TypeTag[T]): raw.Type = {
    def recurse(t: Type): raw.Type = t match {
      // TODO: Add support for Any
      // TODO: Add support for case classes (become record types)
      // TODO: Add support for Maps[String, ...] (become record type with type variable)
      case TypeRef(_, c, Nil) if c.fullName == "scala.Int" => raw.IntType()
      case TypeRef(_, c, List(t1)) if c.fullName == "scala.Predef.Set" => raw.SetType(recurse(t1))
      case TypeRef(_, c, List(t1)) if c.fullName == "scala.List" => raw.ListType(recurse(t1))
      case TypeRef(_, c, List()) if c.fullName == "scala.Predef.String" => raw.StringType()
      case TypeRef(_, c, t1) if c.fullName.startsWith("scala.Tuple") =>
        val regex = """scala\.Tuple(\d+)""".r
        c.fullName match {
          case regex(n) => raw.RecordType(List.tabulate(n.toInt){ case i => raw.AttrType(s"_${i + 1}", recurse(t1(i))) })
        }
      case TypeRef(_, c, t1) if c.fullName.startsWith("scala.Function") =>
        val regex = """scala\.Function(\d+)""".r
        c.fullName match {
          case regex(n) => raw.FunType(recurse(t1(0)), recurse(t1(1)))
        }
      case TypeRef(a, b, c) => println(s"a $a b ${b.fullName} c $c"); ???
    }

    recurse(getTypeOf(x))
  }



}
