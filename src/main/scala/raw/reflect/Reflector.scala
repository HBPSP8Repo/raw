package raw.reflect

import scala.collection.immutable.ListMap
import scala.reflect.runtime.{universe => ru}

object Reflector extends App {
//import scala.collection.immutable.Seq
//import scala.reflect.ClassTag
  import ru._

  //private def getTypeOf[T](x: T)(implicit tag: TypeTag[T]) = typeOf[T]

  val m = ru.runtimeMirror(getClass.getClassLoader)

  def getRawType[T](x: T)(implicit tag: TypeTag[T]): raw.Type =
    typeOf[T] match {
      case t if t =:= ru.definitions.IntTpe => raw.IntType()
      case t @ TypeRef(pre, sym, args) if t <:< typeOf[List[_]] =>
        
        raw.ListType(getRawType(m.reflect(args(0).typeSymbol.asClass)))
      case TypeRef(a, b, c) => println(s"$a $b $c"); ???
    }

  println(getRawType(1))
  println(getRawType(List[Int](1,2)))

    //case class Students(name: String, age: Int)

    //
//
//
//
//    def recurse(t: Type): raw.Type = t match {
//      // TODO: Add support for case classes (become record types)
//      // TODO: Add support for Maps[String, ...] (become record type with type variable)
//
//      //case x1 => x1 =:= ru.definitions.IntTpe; ???
//        // case y1 => y1 <:< typeOf[List]
//
//
//      case TypeRef(_, c, Nil) if c.fullName == "scala.Int" => raw.IntType()
//      case TypeRef(_, c, List(t1)) if c.fullName == "scala.Predef.Set" => raw.SetType(recurse(t1))
//      case TypeRef(_, c, List(t1)) if c.fullName == "scala.List" => raw.ListType(recurse(t1))
//      case TypeRef(_, c, List()) if c.fullName == "scala.Predef.String" => raw.StringType()
//      case TypeRef(_, c, t1) if c.fullName.startsWith("scala.Tuple") =>
//        val regex = """scala\.Tuple(\d+)""".r
//        c.fullName match {
//          case regex(n) => raw.RecordType(List.tabulate(n.toInt){ case i => raw.AttrType(s"_${i + 1}", recurse(t1(i))) })
//        }
//      case TypeRef(_, c, t1) if c.fullName.startsWith("scala.Function") =>
//        val regex = """scala\.Function(\d+)""".r
//        c.fullName match {
//          case regex(n) => raw.FunType(recurse(t1(0)), recurse(t1(1)))
//        }
//      case TypeRef(_, c, List()) if c.fullName == "scala.Any" => raw.TypeVariable(new raw.Variable())
//
//      case TypeRef(_, b, List()) => {
//        val constructorSymbol = b
//        val defaultConstructor =
//          if (constructorSymbol.isMethod) constructorSymbol.asMethod
//          else {
//            val ctors = constructorSymbol.asTerm.alternatives
//            ctors.map { _.asMethod }.find { _.isPrimaryConstructor }.get
//          }
//
//        ListMap[String, Type]() ++ defaultConstructor.paramLists.reduceLeft(_ ++ _).map {
//          sym => sym.name.toString -> tpe.member(sym.name).asMethod.returnType
//        }
//
//      }
//
//
//
//      case TypeRef(a, b, c) => println(s"a $a b ${b.fullName} c $c"); ???
//    }
//
//    def isCaseClassOrWhat_?(v: Any): Boolean = {
//      import reflect.runtime.universe._
//      val typeMirror = runtimeMirror(v.getClass.getClassLoader)
//      val instanceMirror = typeMirror.reflect(v)
//      val symbol = instanceMirror.symbol
//      symbol.isCaseClass
//    }
//
////    val r = if (isCaseClassOrWhat_?(x)) {
////      x match {
////        case p: Product => raw.RecordType(p.getClass.getDeclaredFields.map(_.getName).zip(p.productIterator.to).map{ case (name, field) => raw.AttrType(name, getRawType(field)) }.to)
////      }
////    } else
////    recurse(getTypeOf(x))
////    println(r)
////    r
//
//    /**
//     * Returns a map from formal parameter names to types, containing one
//     * mapping for each constructor argument.  The resulting map (a ListMap)
//     * preserves the order of the primary constructor's parameter list.
//     */
//    def caseClassParamsOf[T: TypeTag]: ListMap[String, Type] = {
//      val tpe = typeOf[T]
//      val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
//      val defaultConstructor =
//        if (constructorSymbol.isMethod) constructorSymbol.asMethod
//        else {
//          val ctors = constructorSymbol.asTerm.alternatives
//          ctors.map { _.asMethod }.find { _.isPrimaryConstructor }.get
//        }
//
//      ListMap[String, Type]() ++ defaultConstructor.paramLists.reduceLeft(_ ++ _).map {
//        sym => sym.name.toString -> tpe.member(sym.name).asMethod.returnType
//      }
//    }
//
////    val r = x match {
////      case p: Product => {
////        raw.RecordType(x.getClass.getDeclaredFields.map(_.getName).map { case name => raw.AttrType(name, raw.IntType()) }.to)
//
////        val foo = caseClassParamsOf[T]
////        println("foo is " + foo)
////        val rr =
////        raw.RecordType(foo.map{ case (k, v) => raw.AttrType(k, recurse(v)) }.to)
////        println(rr)
////        rr
//
////        val m = ru.runtimeMirror(x.getClass.getClassLoader)
////        val t = getTypeOf(x)
////        //raw.RecordType(p.getClass.getDeclaredFields.map(_.getName).map { case name =>
////        val foo = t.members.collect {
////          case y: ru.MethodSymbol if y.isGetter => y
////        }
////        raw.RecordType(foo.map{ case sym =>
////          val name = sym.fullName
//////        raw.RecordType(p.getClass.getDeclaredFields.map(field => {
//////          field setAccessible true
//////          field.getName// -> field.get(this)
//////        }).map { case name =>
////          //println("name is " + name)
////          //val sym = t.decl(ru.TermName(name)).asTerm
////          val im =  m.reflect(x)
////          val fieldMirror = im.reflectField(sym)
////          raw.AttrType(name, getRawType(fieldMirror))}.to)
////      }
////      case _ => recurse(getTypeOf(x))
////    }
////    println(r)
////    r
//    recurse(getTypeOf(x))
//
//  }
//
//

}
