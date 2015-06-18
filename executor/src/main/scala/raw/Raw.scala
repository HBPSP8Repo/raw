package raw

import com.typesafe.scalalogging.StrictLogging
import raw.algebra.LogicalAlgebra.LogicalAlgebraNode
import raw.algebra.{LogicalAlgebraPrettyPrinter, Unnester, LogicalAlgebra, Typer}
import raw.compilerclient.QueryCompilerClient
import raw.psysicalalgebra.PhysicalAlgebra._
import raw.psysicalalgebra.{LogicalToPhysicalAlgebra, PhysicalAlgebraPrettyPrinter}

import scala.annotation.StaticAnnotation
import scala.collection.mutable.ArrayBuffer
import scala.language.experimental.macros


//class queryAnnotation(query: String, catalog: HList) extends StaticAnnotation {
class rawQueryAnnotation extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro RawImpl.query_impl
}

/*
 */
abstract class RawQuery {
//  val query: String

  def computeResult: Any
}


class RawImpl(val c: scala.reflect.macros.whitebox.Context) extends StrictLogging {

  import c.universe._

  /** Bail out during compilation with error message. */
  def bail(message: String) = c.abort(c.enclosingPosition, message)

  case class InferredType(rawType: raw.Type, isSpark: Boolean)

  /** Infer RAW's type from the Scala type. */
  def inferType(t: c.Type): InferredType = {
    val rawType = t match {
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Int" => raw.IntType()
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Any" => raw.TypeVariable(new raw.Variable())
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Predef.String" => raw.StringType()

      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Predef.Set" => raw.SetType(inferType(t1).rawType)
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.List" || sym.fullName == "scala.collection.immutable.List" => raw.ListType(inferType(t1).rawType)
      case TypeRef(_, sym, t1) if sym.fullName.startsWith("scala.Tuple") =>
        val regex = """scala\.Tuple(\d+)""".r
        sym.fullName match {
          case regex(n) => raw.RecordType(List.tabulate(n.toInt) { case i => raw.AttrType(s"_${i + 1}", inferType(t1(i)).rawType) }, None)
        }
      case TypeRef(_, sym, t1) if sym.fullName.startsWith("scala.Function") =>
        val regex = """scala\.Function(\d+)""".r
        sym.fullName match {
          case regex(n) => raw.FunType(inferType(t1(0)).rawType, inferType(t1(1)).rawType)
        }

      case t@TypeRef(_, sym, Nil) =>
        val symName = sym.fullName
        val ctor = t.decl(termNames.CONSTRUCTOR).asMethod
        raw.RecordType(ctor.paramLists.head.map { case sym1 => raw.AttrType(sym1.name.toString, inferType(sym1.typeSignature).rawType) }, Some(symName))

      case TypeRef(_, sym, List(t1)) if sym.fullName == "org.apache.spark.rdd.RDD" =>
        raw.ListType(inferType(t1).rawType)

      case TypeRef(pre, sym, args) =>
        bail(s"Unsupported TypeRef($pre, $sym, $args)")
    }

    val isSpark = t.typeSymbol.fullName.equals("org.apache.spark.rdd.RDD")
    InferredType(rawType, isSpark)
  }

  def recordTypeSym(r: RecordType) = r match {
    case RecordType(_, Some(symName)) => symName
    case _ =>
      // TODO: The following naming convention may conflict with user type names. Consider prefixing all types using `type = Prefix???`
      val uniqueId = if (r.hashCode() < 0) s"_n${Math.abs(r.hashCode()).toString}" else s"_p${r.hashCode().toString}"
      uniqueId
  }

  /** Build case classes that correspond to record types.
    */
  def buildCaseClasses(logicalTree: LogicalAlgebraNode, world: World, typer: Typer): Set[Tree] = {
    import org.kiama.rewriting.Rewriter.collect

    // Extractor for anonymous record types
    object IsAnonRecordType {
      def unapply(e: raw.algebra.Expressions.Exp): Option[RecordType] = typer.expressionType(e) match {
        case r@RecordType(_, None) => Some(r)
        case _ => None
      }
    }

    // Create Scala type from RAW type
    def tipe(t: raw.Type): String = t match {
      case _: BoolType => "Boolean"
      case FunType(t1, t2) => ???
      case _: StringType => "String"
      case _: IntType => "Int"
      case _: FloatType => "Float"
      case r: RecordType => recordTypeSym(r)
      case BagType(innerType) => ???
      case ListType(innerType) => s"List[${tipe(innerType)}]"
      case SetType(innerType) => s"Set[${tipe(innerType)}]"
      case UserType(idn) => tipe(world.userTypes(idn))
      case TypeVariable(v) => ???
      case _: AnyType => ???
      case _: NothingType => ???
    }

    // Collect all record types from tree
    val collectAnonRecordTypes = collect[List, raw.RecordType] {
      case IsAnonRecordType(t) => t
    }

    // Convert all collected record types to a set to remove repeated types, leaving only the structural types
    val anonRecordTypes = collectAnonRecordTypes(logicalTree).toSet

    // Create corresponding case class
    val code = anonRecordTypes
      .map {
      case r@RecordType(atts, None) =>
        val args = atts.map(att => s"${att.idn}: ${tipe(att.tipe)}").mkString(",")
        val cl = s"""case class ${recordTypeSym(r)}($args)"""
        cl
    }

    code.map(c.parse)
  }

  /** Build code-generated query plan from logical algebra tree.
    */
  def buildCode(physicalTree: PhysicalAlgebraNode, world: World, typer: Typer): Tree = {
    import algebra.Expressions._

    def exp(e: Exp): Tree = {
      def binaryOp(op: BinaryOperator): String = op match {
        case _: Eq => "=="
        case _: Neq => "!="
        case _: Ge => ">="
        case _: Gt => ">"
        case _: Le => "<="
        case _: Lt => "<"
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

    def zeroExp(m: PrimitiveMonoid): Const = m match {
      case _: AndMonoid => BoolConst(true)
      case _: OrMonoid => BoolConst(false)
      case _: SumMonoid => IntConst("0")
      case _: MultiplyMonoid => IntConst("1")
      case _: MaxMonoid => IntConst("0") // TODO: Fix since it is not a monoid
    }

    def build(a: PhysicalAlgebraNode): Tree = a match {
      case ScalaNest(m, e, f, p, g, child) => m match {
        case m1: PrimitiveMonoid =>
          val z1 = zeroExp(m1)
          val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
          q"""val child = ${build(child)}
              logger.debug("Child:\n{}", child.mkString("\n"))

              val grouped = child.groupBy(${exp(f)})
              logger.debug("grouped:\n{}", grouped.mkString("\n"))

              val map1 = grouped.toList.map(v => (v._1, v._2.filter(${exp(p)})))
              logger.debug("map1:\n{}", map1.mkString("\n"))

              val map2 = map1.map(v => (v._1, v._2.map(${exp(f1)})))
              logger.debug("map2:\n{}", map2.mkString("\n"))

              val map3 = map2.map(v => (v._1, v._2.foldLeft(${zero(m1)})(${fold(m1)})))
              logger.debug("map3:\n{}", map3.mkString("\n"))
              map3"""
        case m1: BagMonoid => ???
        case m1: ListMonoid => ???
        /* The scala code generated here does not type check because it generates an anonymous function without
        specifying the type of the argument.
        [error] <macro>:1: missing parameter type
        [error] (arg => arg._2)
        [error]  ^
         */
        //          val f1 = q"""(arg => if (${exp(g)}(arg) == null) List() else ${exp(e)}(arg))""" // TODO: Remove indirect function call

        /* Also does not work, because the zero value should be typed in order for the type inference to work.
          That is "List[Student]()" instead of "List()"". We currently do not have type information.
         */
        //          val f1 = IfThenElse(BinaryExp(Eq(), g, Null), ZeroCollectionMonoid(ListMonoid()), e)
        //          q"""${build(child)}
        //             .groupBy(${exp(f)})
        //             .toList
        //             .map(v => (v._1, v._2.filter(${exp(p)})))
        //             .map(v => (v._1, v._2.map(${exp(f1)})))
        //             .map(v => (v._1, v._2.to[scala.collection.immutable.List]))"""

        case m1: SetMonoid => ???
        // Not implemented for the same reasons as the ListMonoid
        //          val f1 = q"""(arg => if (${exp(g)}(arg) == null) Set() else ${exp(e)}(arg))""" // TODO: Remove indirect function call
        //          q"""val grouped = ${build(child)}.groupBy(${exp(f)})
        //    println("Grouped: " + grouped)
        //    val mapped1 = grouped.toSet.map(v => (v._1, v._2.filter(${exp(p)})))
        //    println("mapped1: " + mapped1)
        //    val mapped2 = mapped1.map(v => (v._1, v._2.map($f1)))
        //    println("mapped2: " + mapped2)
        //    val mapped3 = mapped2.map(v => (v._1, v._2.to[scala.collection.immutable.Set]))
        //    println("mapped3: " + mapped3)
        //    mapped3
        //  """
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
        Ident(TermName(name))

      // Spark operators
      case SparkScan(name, tipe) => Ident(TermName(name))
      //            q"""${accessPaths(name)}"""

      /////////////////////////////////////////////////////
      // Spark
      /////////////////////////////////////////////////////
      /*
      * Fegaras: Ch 6. Basic Algebra O1
      * Join(X, Y, p), joins the collections X and Y using the join predicate p.
      * This join is not necessarily between two sets; if, for example, X is a list
      * and Y is a bag, then, according to Eq. (D4), the output is a bag.
      */
      case r@SparkJoin(p, left, right) =>
        val leftCode = build(left)
        val rightCode = build(right)
        q"""
     val rddLeft = $leftCode
     val rddRight = $rightCode
     val res = rddLeft.cartesian(rddRight).filter(${exp(p)})
     res
   """

      /* Fegaras: Ch 6. Basic Algebra O2
      * Select(X, p), selects all elements of X that satisfy the predicate p.
      */
      case SparkSelect(p, child) =>
        q"""${build(child)}.filter(${exp(p)})"""

      /*
      * Fegaras: Ch 6. Basic Algebra O3
      * unnest(X, path, p), returns the collection of all pairs (x, y) for each x \in X and for each y \in x.path
      * that satisfy the predicate p(x, y).
      */
      case SparkUnnest(path, pred, child) => ???
        /*
        Something along the lines:
        child.flatMap(x => for { y <- x.path; if pred(x, y)} yield (x, y))

         No shuffling involved, all local computation.
         */

      /* Fegaras: Ch 6. Basic Algebra O4
      Reduce(X, Q, e, p), collects the values e(x) for all x \in X that satisfy p(x) using the accumulator Q.
      The reduce operator can be thought of as a generalized version of the relational projection operator.
      */
      case SparkReduce(m, e, p, child) =>
        val childCode = build(child)
        val filterPredicate = exp(p)
        val mapFunction = exp(e)
        val filterMapCode = q"""
     val childRDD = $childCode
     childRDD
       .filter($filterPredicate)
       .map($mapFunction)
     """
        val code = m match {
          case m1: PrimitiveMonoid =>
            /* TODO: Use Spark implementations of monoids if supported. For instance, RDD has max and min actions.
             * Compare the Spark implementations versus a generic code generator based on fold
             */
            q"""$filterMapCode
           .fold(${zero(m1)})(${fold(m1)})"""

          case _: ListMonoid =>
            // TODO Can this be made more efficient?
            q"""$filterMapCode
          .toLocalIterator
          .to[scala.collection.immutable.List]"""

          case _: BagMonoid =>
            // TODO: Can we improve the lower bound for the value?
            q"""
                val m: scala.collection.Map[_, Long] = $filterMapCode
             .countByValue()
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
            q"""$filterMapCode
            .distinct
            .toLocalIterator
            .to[scala.collection.immutable.Set]"""
        }
        q"""
        val start = "************ SparkReduce ************"
        val res = $code
        val end= "************ SparkReduce ************"
        res
        """

      case SparkNest(m, e, f, p, g, child) =>
        val childTree: Tree = build(child)
        val code = m match {
          case m1: PrimitiveMonoid =>
            val z1 = zeroExp(m1)
            val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
            q"""
  val grouped = $childTree.groupBy(${exp(f)})
  logger.debug("groupBy:\n{}", toString(grouped))

  val filteredP = grouped.mapValues(v => v.filter(${exp(p)}))
  logger.debug("filteredP:\n{}", toString(filteredP))

  val mapped = grouped.mapValues(v => v.map(${exp(f1)}))
  logger.debug("mapped:\n{}", toString(mapped))

  val folded = mapped.mapValues(v => v.fold(${zero(m1)})(${fold(m1)}))
  logger.debug("folded:\n{}", toString(folded))
  folded
  """
          // See ScalaNest case for the reason why these cases are not implemented.
          case m1: BagMonoid => ???
          case m1: ListMonoid => ???
          case m1: SetMonoid => ???
        }

        q"""
val start = "************ SparkNest ************"
def toString[R](rdd: RDD[R]): String = {
  rdd.collect().toList.mkString("\n")
}
val res = $code
val end = "************ SparkNest ************"
res
"""

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

      // TODO: Using null below can be problematic with RDDs of value types (AnyVal)?
      case SparkOuterJoin(p, left, right) =>
        val code = q"""
    val start = "************ SparkOuterJoin ************"
    val leftRDD = ${build(left)}
    val rightRDD = ${build(right)}
    val matching = leftRDD
      .cartesian(rightRDD)
      .filter(tuple => tuple._1 != null)
      .filter(${exp(p)})

    val resWithOption = leftRDD
      .map(v => (v, v))
      .leftOuterJoin(matching)

    val res = resWithOption.map( {
      case (v1, (v2, None)) => (v1, null)
      case (v1, (v2, Some(w))) => (v1, w)
    })
    val end = "************ SparkOuterJoin ************"
    res
    """
        code
      case SparkOuterUnnest(path, pred, child) => ???
    }

    build(physicalTree)
  }


  def extractParams(tree: Tree): (Tree, Tree) = tree match {
    case q"new $name( ..$params )" =>
      println("Extracted params: " + params)
      params match {
        case List(queryTree, catalogTree) =>
          println("query: " + queryTree + ", catalog: " + catalogTree)
          (queryTree.asInstanceOf[Tree], catalogTree.asInstanceOf[Tree])
        //        case List(queryTree:c.Expr[String], catalogTree:c.Expr[HList]) => (queryTree, catalogTree)
        //        case q"($query:String, $catalog:HList)"  List(queryTree:c.Expr[String], catalogTree:c.Expr[HList]) => (queryTree, catalogTree)
      }
  }

  case class AccessPath(name: String, rawType: raw.Type, isSpark: Boolean)

  sealed abstract class QLanguage
  case object MonoidCompreheension extends QLanguage
  case object OQL extends QLanguage

  def extractQueryAndAccessPath(makro: Tree): (String, QLanguage, List[AccessPath]) = {
    /**
     * Check if the query string is known at compile time.
     * If so, build the algebra tree, then build the executable code, and return that to the user.
     * If not, build code that calls the interpreted executor and return that to the user.
     */
    // Typecheck the full tree to resolve references like List[Student] to scala.List[raw.Student]
    val typ = c.typecheck(makro.duplicate)
    //    logger.debug("Typed tree:\n" + showRaw(typ))
    typ match {
      case ClassDef(_, className: TypeName, _, Template(_, _, body: List[Tree])) =>
        logger.info("Analysing class: " + className)
        var query: Option[String] = None
        var queryLanguage: Option[QLanguage] = None
        val accessPaths = new ArrayBuffer[AccessPath]()
        body.foreach(v => {
          logger.info("Checking element: " + showCode(v))
          //          logger.debug("Tree: " + showRaw(v))
          v match {
            case ValDef(_, TermName("query "), _, Literal(Constant(queryString: String))) =>
              logger.info("Found monoid comprehension query: " + queryString)
              if (query.isDefined) {
                bail(s"Multiple queries found. Previous: ${query.get}, Current: $queryString")
              }
              query = Some(queryString)
              queryLanguage = Some(MonoidCompreheension)

            case ValDef(_, TermName("oql "), _, Literal(Constant(queryString: String))) =>
              logger.info("Found oql query: " + queryString)
              if (query.isDefined) {
                bail(s"Multiple queries found. Previous: ${query.get}, Current: $queryString")
              }
              query = Some(queryString)
              queryLanguage = Some(OQL)

            case ValDef(_, TermName(termName), typeTree, _) =>
              val inferredType = inferType(typeTree.tpe)
              logger.info("Found data source: " + termName + ", Raw type: " + inferredType.rawType + ", isSpark: " + inferredType.isSpark)
              accessPaths += AccessPath(termName.trim, inferredType.rawType, inferredType.isSpark)

            case _ =>
              logger.info("Ignoring element")
          }
        })

        (query.get, queryLanguage.get, accessPaths.toList)
    }

    //    annottees.head.tree match {
    //      case q"class $className(..$params) extends RawQuery { ..$body }" =>
    //        println(s"Matched top level Query object. Name: $className, Params: $params, Body: $body")
    //        var query: Option[String] = None
    //        val accessPathsBuilder = mutable.HashMap[String, AccessPath]()
    //
    //        params.foreach(v => {
    //          println("Matching v: " + v + " " + showRaw(v))
    //          val typed: Tree = c.typecheck(v.duplicate)
    //          println("Typed: " + typed)
    //          v match {
    //            case ValDef(_, term, typeTree, accessPathTree) =>
    //              val t: Tree = q""" $term : $typeTree"""
    //              println(" " + t + " " + showRaw(t))
    //              val typed = c.typecheck(t)
    //              println(" " + typed)
    //            case ValDef(_, TermName(termName), typeTree, accessPathTree) =>
    //              println("Found access path. name: " + termName + ", type: " + typeTree + ", raw: " + showRaw(typeTree) + ", expression: " + accessPathTree)
    //              println("Type checking: " + typeTree)
    //              val typed = c.typecheck(typeTree)
    //              println("Typed: " + typed)
    //              val (rawTpe, isSpark) = inferType(typeTree.tpe)
    //              accessPathsBuilder.put(termName, AccessPath(rawTpe, accessPathTree, isSpark))
    //          }
    //        })
    //
    //        body.foreach(v => {
    //          println("Matching v: " + v + " " + showRaw(v))
    //          val vTyped = c.typecheck(v)
    //          println("Matching field: " + v + ".\nTree: " + showRaw(vTyped))
    //          vTyped match {
    //            case ValDef(_, TermName("query"), _, Literal(Constant(queryString: String))) =>
    //              query = Some(queryString)
    //
    //            case ValDef(_, TermName(termName), typeTree, accessPathTree) =>
    //              println("Found access path. name: " + termName + ", type: " + typeTree + ", expression: " + accessPathTree)
    //              val (rawTpe, isSpark) = inferType(typeTree.tpe)
    //              accessPathsBuilder.put(termName, AccessPath(rawTpe, accessPathTree, isSpark))
    //          }
    //        })

  }

//  def apply(query: String, world: World): Either[QueryError, LogicalAlgebra.LogicalAlgebraNode] = {
//    parse(query) match {
//      case Right(tree) => analyze(tree, world) match {
//        case None        => Right(Unnester(tree, world))
//        case Some(error) => Left(error)
//      }
//      case Left(error) => Left(error)
//    }
//  }
//
//}
  def remoteQuery(query: String, world: World): Either[QueryError, LogicalAlgebra.LogicalAlgebraNode] = {
    val compilerClient = new QueryCompilerClient
    compilerClient.compile(query) match {
      case Right(logicalAlgebra) => Right(logicalAlgebra)
      case Left(error) => Left(new ParserError(error))
    }
  }

  def generateCode(makro: Tree, query: String, queryLanguage:QLanguage, accessPaths: List[AccessPath]): c.Expr[Any] = {
    val sources = accessPaths.map(ap => (ap.name, ap.rawType)).toMap
    val world = new World(sources)

    // Parse the query, using the catalog generated from what the user gave.
    val parseResult: Either[QueryError, LogicalAlgebraNode] = queryLanguage match {
      case OQL => remoteQuery(query, world)
      case MonoidCompreheension => Query(query, world)
    }

    parseResult match {
      case Right(logicalTree) =>
        logger.info("Built logical algebra")
        logger.info("Logical algebra:\n{}", LogicalAlgebraPrettyPrinter(logicalTree))
        val typer = new algebra.Typer(world)
        val isSpark: Map[String, Boolean] = accessPaths.map(ap => (ap.name, ap.isSpark)).toMap
        logger.info("Building physical algebra")
        val physicalTree = LogicalToPhysicalAlgebra(logicalTree, isSpark)
        val algebraStr = PhysicalAlgebraPrettyPrinter(physicalTree)
        logger.info("Algebra:\n{}", algebraStr)

        val caseClasses: Set[Tree] = buildCaseClasses(logicalTree, world, typer)
        logger.info("case classes:\n{}", caseClasses.map(showCode(_)).mkString("\n"))

        val generatedTree: Tree = buildCode(physicalTree, world, typer)
        //        logger.info("Query execution code:\n{}", showCode(generatedTree))

        /* Generate the actual scala code. Need to extract the class parameters and the body from the annotated
        class. The body and the class parameters will be copied unmodified in the resulting code.
        */
        val q"class $className(..$paramsAsAny) extends RawQuery { ..$body }" = makro

        /* We generate a companion object that will contain the actual code for executing the query in the computeResult
        method. This method takes as arguments the data sources that were given by the user as class arguments to the
        annotated class. So we need to generate a list of "s1:T[1], s2:[T2], ..." for the method definition, and a list of
        "s1, s2, ..." for method invocation.
        */
        val methodDefParameters: List[ValDef] = paramsAsAny.asInstanceOf[List[ValDef]]
        val methodCallParameters: List[Ident] = methodDefParameters.map(valDef => Ident(valDef.name))

        /* Generating the expanded class plus the companion:
        http://stackoverflow.com/questions/21032869/create-or-extend-a-companion-object-using-a-macro-annotation-on-the-class
        */
        val moduleName = TermName(className.decodedName.toString)
        val companion = q"""
  object $moduleName extends com.typesafe.scalalogging.StrictLogging {
    ..$caseClasses
    def apply(..$methodDefParameters) = {
       $generatedTree
    }
  }"""

        val clazz = q"""
  class $className(..$methodDefParameters) extends RawQuery {
    ..$body
    def computeResult = $moduleName.apply(..$methodCallParameters)
  }
"""
        val block = Block(List(companion, clazz), Literal(Constant(())))

        val scalaCode = showCode(block)
        logger.info("Generated code:\n{}", scalaCode)
        QueryLogger.log(query, algebraStr, scalaCode)

        c.Expr[Any](block)
      case Left(err) => bail(err.err)
    }
  }

  def query_impl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    if (annottees.size > 1) {
      bail(s"Expected a single annottated element. Found: ${annottees.size}\n" + annottees.map(expr => showCode(expr.tree)).mkString("\n"))
    }
    val annottee: Expr[Any] = annottees.head
    val annottation = c.prefix.tree
    val makro = annottee.tree
    //    logger.debug("Annotation: " + annottation)
    logger.info("Expanding annotated target:\n{}", showCode(makro))
    //    logger.debug("Tree:\n" + showRaw(makro))

    val (query: String, language:QLanguage, accessPaths: List[AccessPath]) = extractQueryAndAccessPath(makro)
    logger.info("Access paths: {}", accessPaths.map(ap => ap.name + ", isSpark: " + ap.isSpark).mkString("; "))

    generateCode(makro, query, language, accessPaths)
  }
}
