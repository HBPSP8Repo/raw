package raw

import raw.psysicalalgebra.PhysicalAlgebra._
import raw.psysicalalgebra.{LogicalToPhysicalAlgebra, PhysicalAlgebraPrettyPrinter}
import shapeless.HList

import scala.language.experimental.macros

class RawImpl(val c: scala.reflect.macros.whitebox.Context) {

  import raw.algebra.Typer
  import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
  import raw.psysicalalgebra.LogicalToPhysicalAlgebra
  import raw.psysicalalgebra.PhysicalAlgebra._

  def query(q: c.Expr[String], catalog: c.Expr[HList]) = {

    import c.universe._

    /** Bail out during compilation with error message.
      */
    def bail(message: String) = c.abort(c.enclosingPosition, message)

    /** Check whether the query is known at compile time.
      */
    def queryKnown: Option[String] = q.tree match {
      case Literal(Constant(s: String)) => Some(s)
      case _ => None
    }

    /** Infer RAW's type from the Scala type.
      */
    def inferType(t: c.Type): (raw.Type, Boolean) = {
      val rawType = t match {
        case TypeRef(_, sym, Nil) if sym.fullName == "scala.Int" => raw.IntType()
        case TypeRef(_, sym, Nil) if sym.fullName == "scala.Any" => raw.TypeVariable(new raw.Variable())
        case TypeRef(_, sym, Nil) if sym.fullName == "scala.Predef.String" => raw.StringType()

        case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Predef.Set" => raw.SetType(inferType(t1)._1)
        case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.List" || sym.fullName == "scala.collection.immutable.List" => raw.ListType(inferType(t1)._1)
        case TypeRef(_, sym, t1) if sym.fullName.startsWith("scala.Tuple") =>
          val regex = """scala\.Tuple(\d+)""".r
          sym.fullName match {
            case regex(n) => raw.RecordType(List.tabulate(n.toInt) { case i => raw.AttrType(s"_${i + 1}", inferType(t1(i))._1) }, None)
          }
        case TypeRef(_, sym, t1) if sym.fullName.startsWith("scala.Function") =>
          val regex = """scala\.Function(\d+)""".r
          sym.fullName match {
            case regex(n) => raw.FunType(inferType(t1(0))._1, inferType(t1(1))._1)
          }

        case t@TypeRef(_, sym, Nil) =>
          val symName = sym.fullName
          val ctor = t.decl(termNames.CONSTRUCTOR).asMethod
          raw.RecordType(ctor.paramLists.head.map { case sym1 => raw.AttrType(sym1.name.toString, inferType(sym1.typeSignature)._1) }, Some(symName))

        case TypeRef(_, sym, List(t1)) if sym.fullName == "org.apache.spark.rdd.RDD" => {
          raw.ListType(inferType(t1)._1)
        }

        case TypeRef(pre, sym, args) =>
          bail(s"Unsupported TypeRef($pre, $sym, $args)")
      }

      val isSpark = t.typeSymbol.fullName.equals("org.apache.spark.rdd.RDD")
      (rawType, isSpark)
    }

    def recordTypeSym(r: RecordType) = r match {
      case RecordType(_, Some(symName)) => symName
      case _ =>
        // TODO: The following naming convention may conflict with user type names. Consider prefixing all types using `type = Prefix???`
        val uniqueId = if (r.hashCode() < 0) s"_n${Math.abs(r.hashCode()).toString}" else s"_p${r.hashCode().toString}"
        uniqueId
    }

    /** Build code-generated query plan from logical algebra tree.
      */
    def buildCode(logicalTree: LogicalAlgebraNode, physicalTree: PhysicalAlgebraNode, world: World, typer: Typer, accessPaths: Map[String, Tree]): Tree = {
      import algebra.Expressions._

      /** Build case classes that correspond to record types.
        */
      def buildCaseClasses: Set[Tree] = {

        import org.kiama.rewriting.Rewriter.collect

        // Extractor for anonymous record types
        object IsAnonRecordType {
          def unapply(e: raw.algebra.Expressions.Exp): Option[RecordType] = typer.expressionType(e) match {
            case r @ RecordType(_, None) => Some(r)
            case _                       => None
          }
        }

        // Create Scala type from RAW type
        def tipe(t: raw.Type): String = t match {
          case _: BoolType         => "Boolean"
          case FunType(t1, t2)     => ???
          case _: StringType       => "String"
          case _: IntType          => "Int"
          case _: FloatType        => "Float"
          case r: RecordType       => recordTypeSym(r)
          case BagType(innerType)  => ???
          case ListType(innerType) => s"List[${tipe(innerType)}]"
          case SetType(innerType)  => s"Set[${tipe(innerType)}]"
          case UserType(idn)       => tipe(world.userTypes(idn))
          case TypeVariable(v)     => ???
          case _: AnyType          => ???
          case _: NothingType      => ???
        }

        // Collect all record types from tree
        val collectAnonRecordTypes = collect[List, raw.RecordType] {
          case IsAnonRecordType (t) => t
        }

        // Convert all collected record types to a set to remove repeated types, leaving only the structural types
        val anonRecordTypes = collectAnonRecordTypes (logicalTree).toSet

        // Create corresponding case class
        val code = anonRecordTypes
          .map {
            case r @ RecordType(atts, None) =>
              val args = atts.map(att => s"${att.idn}: ${tipe(att.tipe)}").mkString(",")
              s"""case class ${recordTypeSym(r)}($args)"""
          }

        code.map(c.parse)
      }

      def build(a: PhysicalAlgebraNode): Tree = {

        def exp(e: Exp): Tree = {

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
            case Null => "null"
            case BoolConst(v) => v.toString
            case IntConst(v) => v
            case FloatConst(v) => v
            case StringConst(v) => s""""$v""""
            case _: Arg => "arg"
            case RecordProj(e1, idn) => s"${recurse(e1)}.$idn"
            case RecordCons(atts) =>
              val sym = recordTypeSym(typer.expressionType(e) match { case r: RecordType => r })
              val vals = atts.map(att => recurse(att.e)).mkString(",")
              s"""$sym($vals)"""
            case IfThenElse(e1, e2, e3) => s"if (${recurse(e1)}) ${recurse(e2)} else ${recurse(e3)}"
            case BinaryExp(op, e1, e2) => s"${recurse(e1)} ${binaryOp(op)} ${recurse(e2)}"
            case MergeMonoid(m, e1, e2) => m match {
              case _: SumMonoid => ???
              case _: MaxMonoid => ???
              case _: MultiplyMonoid => ???
              case _: AndMonoid => s"${recurse(e1)} && ${recurse(e2)}"
              case _: OrMonoid => ???
            }
            case UnaryExp(op, e1) => op match {
              case _: Not => s"!${recurse(e1)}"
              case _: Neg => s"-${recurse(e1)}"
              case _: ToBool => s"${recurse(e1)}.toBoolean"
              case _: ToInt => s"${recurse(e1)}.toInt"
              case _: ToFloat => s"${recurse(e1)}.toFloat"
              case _: ToString => s"${recurse(e1)}.toString"
            }
          }

          c.parse(s"(arg => ${recurse(e)})")
        }

        def zero(m: PrimitiveMonoid): Tree = m match {
          case _: AndMonoid => q"true"
          case _: OrMonoid => q"false"
          case _: SumMonoid => q"0"
          case _: MultiplyMonoid => q"1"
          case _: MaxMonoid => q"0" // TODO: Fix since it is not a monoid
        }

        def fold(m: PrimitiveMonoid): Tree = m match {
          case _: AndMonoid => q"((a, b) => a && b)"
          case _: OrMonoid => q"((a, b) => a || b)"
          case _: SumMonoid => q"((a, b) => a + b)"
          case _: MultiplyMonoid => q"((a, b) => a * b)"
          case _: MaxMonoid => q"((a, b) => if (a > b) a else b)"
        }

        println("Generating code for node: " + a)
        a match {
          case ScalaNest(m, e, f, p, g, child) => m match {
            case m1: PrimitiveMonoid =>
              val z1 = m1 match {
                case _: AndMonoid => BoolConst(true)
                case _: OrMonoid => BoolConst(false)
                case _: SumMonoid => IntConst("0")
                case _: MultiplyMonoid => IntConst("1")
                case _: MaxMonoid => IntConst("0") // TODO: Fix since it is not a monoid
              }
              val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
              q"""${build(child)}.groupBy(${exp(f)}).toList.map(v => (v._1, v._2.filter(${exp(p)}))).map(v => (v._1, v._2.map(${exp(f1)}))).map(v => (v._1, v._2.foldLeft(${zero(m1)})(${fold(m1)})))"""
            case m1: BagMonoid =>
              ???
            case m1: ListMonoid =>
              val f1 = q"""(arg => if (${exp(g)}(arg) == null) List() else ${exp(e)}(arg))""" // TODO: Remove indirect function call
              q"""${build(child)}.groupBy(${exp(f)}).toList.map(v => (v._1, v._2.filter(${exp(p)}))).map(v => (v._1, v._2.map($f1))).map(v => (v._1, v._2.to[scala.collection.immutable.List]))"""
            case m1: SetMonoid =>
              val f1 = q"""(arg => if (${exp(g)}(arg) == null) Set() else ${exp(e)}(arg))""" // TODO: Remove indirect function call
              q"""${build(child)}.groupBy(${exp(f)}).toSet.map(v => (v._1, v._2.filter(${exp(p)}))).map(v => (v._1, v._2.map($f1))).map(v => (v._1, v._2.to[scala.collection.immutable.Set]))"""
          }
          case ScalaOuterJoin(p, left, right) =>
            q"""
            ${build(left)}.flatMap(l =>
              if (l == null)
                List((null, null))
              else {
                val ok = ${build(right)}.map(r => (l, r)).filter(${exp(p)})
                if (ok.isEmpty)
                  List((l, null))
                else
                  ok
              }
            )
            """
          case r@ScalaReduce(m, e, p, child) =>
            val code = m match {
              case m1: PrimitiveMonoid =>
                // TODO: Replace foldLeft with fold?
                q"""${build(child)}.filter(${exp(p)}).map(${exp(e)}).foldLeft(${zero(m1)})(${fold(m1)})"""
              case _: BagMonoid =>
                /* TODO: There is no Bag implementation on the Scala or Java standard libs. Here we use Guava's Bag
                 * implementation. Verify if this dependency on Guava could cause problems.
                 * The code below does not import any of the dependencies, instead it uses the fully qualified package names.
                 * The rationale is that if the client code does any manipulation of the result as an ImmutableMultiset,
                 * then it must declare the imports to compile. If it does not downcast the result, then it does not need
                 * the imports.
                 */
                q"""val e = ${build(child)}.filter(${exp(p)}).map(${exp(e)})
                    com.google.common.collect.ImmutableMultiset.copyOf(scala.collection.JavaConversions.asJavaIterable(e))"""
              case _: ListMonoid =>
                q"""${build(child)}.filter(${exp(p)}).map(${exp(e)}).to[scala.collection.immutable.List]"""
              case _: SetMonoid =>
                q"""${build(child)}.filter(${exp(p)}).map(${exp(e)}).to[scala.collection.immutable.Set]"""
            }
            if (r eq physicalTree)
              code
            else
              q"""List($code)"""
          case ScalaSelect(p, child) => // The AST node Select() conflicts with built-in node Select() in c.universe
            q"""${build(child)}.filter(${exp(p)})"""
          case ScalaScan(name, _) =>
            q"""${accessPaths(name)}"""

          // Spark operators
          case SparkScan(name, tipe) =>
            q"""${accessPaths(name)}"""

          case SparkSelect(p, child) =>
            q"""${build(child)}.filter(${exp(p)})"""

          case SparkReduce(m, e, p, child) =>
            val childCode = build(child)
            val code = m match {
              case m1: PrimitiveMonoid =>
                /* TODO: Use Spark implementations of monoids if supported. For instance, RDD has max and min actions.
                 * Compare the Spark implementations versus a generic code generator based on fold
                 */
                q"""${childCode}.filter(${exp(p)}).map(${exp(e)}).fold(${zero(m1)})(${fold(m1)})"""

              case _: ListMonoid =>
                // TODO Can this be made more efficient?
                q"""${childCode}.filter(${exp(p)}).map(${exp(e)}).toLocalIterator.to[scala.collection.immutable.List]"""

              case _: BagMonoid =>
                // TODO: Can we improve the lower bound for the value?
                q"""val m: scala.collection.Map[_, Long] = ${childCode}.filter(${exp(p)}).map(${exp(e)}).countByValue()
                    val b = com.google.common.collect.ImmutableMultiset.builder[Any]()
                    m.foreach( p => b.addCopies(p._1, p._2.toInt) )
                    b.build()
                 """
              case _: SetMonoid =>
                /* - calling distinct in each partition reduces the size of the data that has to be sent to the driver,
                 *   by eliminating the duplicates early.
                 *
                 * - toLocalIterator() retrieves one partition at a time by the driver, which requires less memory than
                 * collect(), which first gets all results.
                 *
                 * - toSet is a Scala local operation.
                 */
                q"""${childCode}.filter(${exp(p)}).map(${exp(e)}).distinct.toLocalIterator.to[scala.collection.immutable.Set]"""
            }
            code

          case SparkNest(m, e, f, p, g, child) =>
            val childTree: Tree = build(child)
            //            println("Child: " + childTree)
            // Common part
            // build a ProductCons of the f variables and group rows of the child by its value
            //            val tree = q"""${childTree}.groupBy(${exp(f)}).map(v => (v._1, v._2.filter(${exp(p)})))"""
            childTree
          //            m match {
          //              case m1: PrimitiveMonoid =>
          //                val z1 = m1 match {
          //                  case _: AndMonoid => BoolConst(true)
          //                  case _: OrMonoid => BoolConst(false)
          //                  case _: SumMonoid => IntConst("0")
          //                  case _: MultiplyMonoid => IntConst("1")
          //                  case _: MaxMonoid => IntConst("0") // TODO: Fix since it is not a monoid
          //                }
          //                val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
          //                q"""${tree}.map(v => (v._1, v._2.map(${exp(f1)}))).map(v => (v._1, v._2.foldLeft(${zero(m1)})(${fold(m1)})))"""
          //              case m1: BagMonoid =>
          //                ???
          //              case m1: ListMonoid =>
          //                ???
          //              case m1: SetMonoid =>
          //                val f1 = q"""(arg => if (${exp(g)}(arg) == null) Set() else ${exp(e)}(arg))""" // TODO: Remove indirect function call
          //                q"""${tree}.map(v => (v._1, v._2.map($f1))).map(v => (v._1, v._2.to[scala.collection.immutable.Set]))"""
          //            }
          case r@SparkJoin(p, left, right) => {
            val leftCode = build(left)
            val rightCode = build(right)
            q"""
               val rddLeft = ${leftCode}
               val rddRight = ${rightCode}
               rddLeft.cartesian(rddRight).filter(${exp(p)})
             """
          }
          case SparkMerge(m, left, right) => ???

          /*
outer-join, X OJ(p) Y, is a left outer-join between X and Y using the join
predicate p. The domain of the second generator (the generator of w) in
Eq. (O5) is always nonempty. If Y is empty or there are no elements that
can be joined with v (this condition is tested by universal quantification),
then the domain is the singleton value [NULL], i.e., w becomes null.
Otherwise each qualified element w of Y is joined with v.

Delegates to PairRDDFunctions#leftOuterJoin. We cannot use this method directly, because
it takes RDDs in the following form: (k, v), (k, w) => (k, (v, Option(w)), using k to make the
matching. While we have (p, left, right) and want  (v, w) with p used to match the elements.
The code bellow does the following transformations:
1. Compute RDD(v, w) such that v!=null and p(v, w) is true.
2. Apply PairRDDFunctions#leftOuterJoin.
  RDD(v, w).leftOuterJoin( RDD(v, v) ) => (v, (v, Option[w]))
3. Transform in the output format of this operator.
   (v, (v, Some[w])) -> (v, w)
   (v, (v, None)) -> (v, null)
           */
          case SparkOuterJoin(p, left, right) =>
            val code = q"""
              val leftRDD = ${build(left)}
              val rightRDD = ${build(right)}
              val matching = leftRDD
                .cartesian(rightRDD)
                .filter(tuple => tuple._1 != null)
                .filter(${exp(p)})

              val resWithOption = leftRDD
                .map(v => (v, v))
                .leftOuterJoin(matching)

              resWithOption.map( {
                case (v1, (v2, None)) => (v1, null)
                case (v1, (v2, Some(w))) => (v1, w)
              })
              """
            code
          case SparkOuterUnnest(path, pred, child) => ???
          case SparkUnnest(path, pred, child) => ???
        }
      }

      val caseClasses = buildCaseClasses
      val code = build(physicalTree)

      q"""
      ..$caseClasses
      $code"""
    }

    case class AccessPath(tipe: raw.Type, tree: Tree, isSpark: Boolean)

    /** Retrieve names, types and trees from the catalog passed by the user.
      */
    def hlistToAccessPath: Map[String, AccessPath] = {
      catalog.tree match {
        case Apply(Apply(TypeApply(_, _), items), _) =>
          items.flatMap {
            case Apply(TypeApply(Select(Apply(_, List(Literal(Constant(name)))), _), List(scalaType)), List(tree)) =>
              println(s"Access path found: $name, scalaType: $scalaType, tree: $tree")
              val (rawType, isSpark) = inferType(scalaType.tpe)
              List(name.toString -> AccessPath(rawType, tree, isSpark))
            case Apply(TypeApply(_, _), items1) =>
              items1.map {
                case Apply(TypeApply(Select(Apply(_, List(Literal(Constant(name)))), _), List(scalaType)), List(tree)) =>
                  println(s"Access path found: $name, scalaType: $scalaType, tree: $tree")
                  val (rawType, isSpark) = inferType(scalaType.tpe)
                  name.toString -> AccessPath(rawType, tree, isSpark)
              }
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
        // Extract from the HList given by the user in the query to a description of the access type, scan operators.
        val accessPaths: Map[String, AccessPath] = hlistToAccessPath
        println(s"Access paths: $accessPaths")
        // Create the catalog
        val sources = accessPaths.map({ case (name, AccessPath(rawType, _, _)) => name -> rawType })
        val world = new World(sources)
        // Parse the query, using the catalog generated from what the user gave.
        Query(qry, world) match {
          case Right(logicalTree) => {
            val typer = new algebra.Typer(world)
            val isSpark: Map[String, Boolean] = accessPaths.map({ case (name, ap) => (name, ap.isSpark) })
            val physicalTree = LogicalToPhysicalAlgebra(logicalTree, isSpark)
            val generatedCode: Tree = buildCode(logicalTree, physicalTree, world, typer, accessPaths.map { case (name, AccessPath(_, tree, _)) => name -> tree })
            QueryLogger.log(qry, PhysicalAlgebraPrettyPrinter(physicalTree), generatedCode.toString)
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
