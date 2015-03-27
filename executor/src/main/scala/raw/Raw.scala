package raw

import scala.language.experimental.macros
import shapeless.HList

class RawImpl(val c: scala.reflect.macros.whitebox.Context) {

  def query(q: c.Expr[String], catalog: c.Expr[HList]) = {

    import c.universe._

    /** Bail out during compilation with error message.
      */
    def bail(message: String) = c.abort(c.enclosingPosition, message)

    /** Check whether the query is known at compile time.
      */
    def queryKnown: Option[String] = q.tree match {
      case Literal(Constant(s: String)) => Some(s)
      case _                            => None
    }

    /** Infer RAW's type from the Scala type.
      */
    def inferType(t: c.Type): raw.Type = t match {
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Int"                                                            => raw.IntType()
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Any"                                                            => raw.TypeVariable(new raw.Variable())
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Predef.Set"                                                => raw.SetType(inferType(t1))
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.List" || sym.fullName == "scala.collection.immutable.List" => raw.ListType(inferType(t1))
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Predef.String"                                                  => raw.StringType()
      case TypeRef(_, sym, t1) if sym.fullName.startsWith("scala.Tuple")                                                  =>
        val regex = """scala\.Tuple(\d+)""".r
        sym.fullName match {
          case regex(n) => raw.RecordType(List.tabulate(n.toInt) { case i => raw.AttrType(s"_${i + 1}", inferType(t1(i))) })
        }
      case TypeRef(_, sym, t1) if sym.fullName.startsWith("scala.Function")                                               =>
        val regex = """scala\.Function(\d+)""".r
        sym.fullName match {
          case regex(n) => raw.FunType(inferType(t1(0)), inferType(t1(1)))
        }
      case t@TypeRef(_, sym, Nil)                                                                                         =>
        val ctor = t.decl(termNames.CONSTRUCTOR).asMethod
        raw.RecordType(ctor.paramLists.head.map { case sym1 => raw.AttrType(sym1.name.toString, inferType(sym1.typeSignature)) })
      case TypeRef(pre, sym, args)                                                                                        => bail(s"Unsupported TypeRef($pre, $sym, $args)")
    }

    /** Build code-generated query plan from logical algebra tree.
      */
    def buildCode(root: algebra.LogicalAlgebra.LogicalAlgebraNode, world: World, accessPaths: Map[String, Tree]): Tree = {
      import algebra.Expressions._
      import algebra.LogicalAlgebra._

      val typer = new algebra.Typer(world)

      def build(a: algebra.LogicalAlgebra.LogicalAlgebraNode): Tree = {

        def exp(e: Exp): Tree = {

          def tipe(t: raw.Type): String = t match {
            case _: BoolType         => "Boolean"
            case _: StringType       => "String"
            case _: IntType          => "Int"
            case _: FloatType        => "Float"
            //case RecordType(atts)    => "record" <> parens(group(nest(lsep(atts.map((att: AttrType) => att.idn <> "=" <> tipe(att.tipe)), comma))))
            case BagType(innerType)  => ???
            case ListType(innerType) => s"List[${tipe(innerType)}]"
            case SetType(innerType)  => s"Set[${tipe(innerType)}]"
            case UserType(idn)       => tipe(world.userTypes(idn))
            case FunType(t1, t2)     => ???
            case TypeVariable(v)     => ???
            case _: AnyType          => ???
            case _: NothingType      => ???
          }

          def binaryOp(op: BinaryOperator): String = op match {
            case _: Eq  => "=="
            case _: Neq => "!="
            case _: Ge  => ">="
            case _: Gt  => ">"
            case _: Le  => "<="
            case _: Lt  => "<"
            case _: Sub => "-"
            case _: Div => "/"
            case _: Mod => "%"
          }

          def recurse(e: Exp): String = e match {
            case Null                        => "null"
            case BoolConst(v)                => v.toString
            case IntConst(v)                 => v
            case FloatConst(v)               => v
            case StringConst(v)              => s""""$v""""
            case _: Arg                      => "arg"
            case RecordProj(e1, idn)         => s"${recurse(e1)}.$idn"
            case r @ RecordCons(atts)            =>
              val vals = atts.map(att => s"val ${att.idn} = ${recurse(att.e)}").mkString(";")
              val maps = atts.map(att => s""""${att.idn}" -> ${att.idn}""").mkString(",")
              s"""new {
                def toMap = Map($maps)
                $vals
              }"""
//            {
//              val uniqueId = if (r.hashCode() < 0) s"_a${r.hashCode().toString}" else s"_b${r.hashCode().toString}"
//              val params = atts.map(att => s"${att.idn}: ${typer.expressionType(e)}").mkString(",")
//              val vals = atts.map(att => recurse(att.e)).mkString(",")
//              val maps = atts.map(att => s""""${att.idn}" -> ${att.idn}""").mkString(",")
//              val x = s"""
//              case class $uniqueId($params) {
//                def toMap = Map($maps)
//              }
//              $uniqueId($vals)
//              """
//              println(x)
//              x}
            case IfThenElse(e1, e2, e3)      => s"if (${recurse(e1)}) ${recurse(e2)} else ${recurse(e3)}"
            case BinaryExp(op, e1, e2)       => s"${recurse(e1)} ${binaryOp(op)} ${recurse(e2)}"
            case MergeMonoid(m, e1, e2)      => ???
            case UnaryExp(op, e1)            => op match {
              case _: Not      => s"!${recurse(e1)}"
              case _: Neg      => s"-${recurse(e1)}"
              case _: ToBool   => s"${recurse(e1)}.toBoolean"
              case _: ToInt    => s"${recurse(e1)}.toInt"
              case _: ToFloat  => s"${recurse(e1)}.toFloat"
              case _: ToString => s"${recurse(e1)}.toString"
            }
          }

          c.parse(s"(arg => ${recurse(e)})")
        }

        def zero(m: PrimitiveMonoid): Tree = m match {
          case _: AndMonoid      => q"true"
          case _: OrMonoid       => q"false"
          case _: SumMonoid      => q"0"
          case _: MultiplyMonoid => q"1"
          case _: MaxMonoid      => q"0"  // TODO: Fix since it is not a monoid
        }

        def fold(m: PrimitiveMonoid): Tree = m match {
          case _: AndMonoid      => q"((a, b) => a && b)"
          case _: OrMonoid       => q"((a, b) => a || b)"
          case _: SumMonoid      => q"((a, b) => a + b)"
          case _: MultiplyMonoid => q"((a, b) => a * b)"
          case _: MaxMonoid      => q"((a, b) => if (a > b) a else b)"
        }

        a match {
          case Nest(m, e, f, p, g, child) => m match {
            case m1: PrimitiveMonoid =>
              val z1 = m1 match {
                case _: AndMonoid      => BoolConst(true)
                case _: OrMonoid       => BoolConst(false)
                case _: SumMonoid      => IntConst("0")
                case _: MultiplyMonoid => IntConst("1")
                case _: MaxMonoid      => IntConst("0") // TODO: Fix since it is not a monoid
              }
              val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
              q"""${build(child)}.groupBy(${exp(f)}).map(v => (v._1, v._2.filter(${exp(p)}))).map(v => (v._1, v._2.map(${exp(f1)}))).map(v => (v._1, v._2.foldLeft(${zero(m1)})(${fold(m1)})))"""
            case m1: BagMonoid =>
              ???
            case m1: ListMonoid =>
              ???
            case m1: SetMonoid =>
              val f1 = q"""(arg => if (${exp(g)}(arg) == null) Set() else ${exp(e)}(arg))"""  // TODO: Remove indirect function call
              q"""${build(child)}.groupBy(${exp(f)}).map(v => (v._1, v._2.filter(${exp(p)}))).map(v => (v._1, v._2.map($f1))).map(v => (v._1, v._2.toSet))"""
          }
          case OuterJoin(p, left, right) =>
            q"""
            ${build(left)}.flatMap(l =>
              if (l == null)
                List((null, null))
              else {
                val ok = ${build(right)}.map(r => (l, r)).filter(${exp(p)})
                if (ok.isEmpty)
                  List((l, null))
                else
                  ok.map(r => (l, r))
              }
            )
            """
          case r @ Reduce(m, e, p, child) =>
            val code = m match {
              case m1: PrimitiveMonoid =>
                q"""${build(child)}.filter(${exp(p)}).map(${exp(e)}).foldLeft(${zero(m1)})(${fold(m1)})"""
              case _: BagMonoid =>
                ???
              case _: ListMonoid =>
                q"""${build(child)}.filter(${exp(p)}).map(${exp(e)}).toList"""
              case _: SetMonoid =>
                q"""${build(child)}.filter(${exp(p)}).map(${exp(e)}).toSet"""
            }
            if (r eq root)
              code
            else
              q"""List($code)"""
          case algebra.LogicalAlgebra.Select(p, child) => // The AST node Select() conflicts with built-in node Select() in c.universe
            q"""${build(child)}.filter(${exp(p)})"""
          case Scan(name, _) =>
            q"""${accessPaths(name)}"""
        }
      }

      build(root)
    }

    case class AccessPath(tipe: raw.Type, tree: Tree)

    /** Retrieve names, types and trees from the catalog passed by the user.
      */
    def hlistToAccessPath: Map[String, AccessPath] = catalog.tree match {
      case Apply(Apply(TypeApply(_, _), items), _) => {
        items.map {
          case Apply(TypeApply(Select(Apply(_, List(Literal(Constant(name)))), _), List(scalaType)), List(tree)) =>
            name.toString -> AccessPath(inferType(scalaType.tpe), tree)
        }.toMap
      }
    }

    /** Macro main code.
      *
      * Check if the query string is known at compile time.
      * If so, build the algebra tree, then build the executable code, and return that to the user.
      * If not, build code that calls the interpreted executor and return that to the user.
      */

    queryKnown match {
      case Some(qry) =>
        //val accessPaths = hmapToAccessPath
        val accessPaths = hlistToAccessPath
        val world = new World(sources = accessPaths.map { case (name, AccessPath(tipe, _)) => name -> tipe })
        Query.apply(qry, world) match {
          case Right(tree) => {
            val generatedCode = buildCode(tree, world, accessPaths.map { case (name, AccessPath(_, tree)) => name -> tree })
            println("Generated code: " + generatedCode)
            c.Expr[Any](generatedCode)
          }
          case Left(err) => bail(err.err)
        }
      case None =>
        ???
    }
  }
}

object Raw {
  def query(q: String, catalog: HList): Any = macro RawImpl.query
}
