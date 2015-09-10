package raw

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.datanucleus.metadata.QueryLanguage
import .LogicalAlgebraNode
import raw.algebra.{LogicalAlgebraPrettyPrinter, Typer}
import raw.compilerclient.OQLToPlanCompilerClient
import raw.psysicalalgebra.PhysicalAlgebra._
import raw.psysicalalgebra.{LogicalToPhysicalAlgebra, PhysicalAlgebraPrettyPrinter}

import scala.annotation.StaticAnnotation
import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
//import scala.language.experimental.macros

import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox
// TODO: Work in progress, to explore the possibility of compiling a query using the Toolbox compiler.
object RawCompileTime extends StrictLogging {
  val mirror = runtimeMirror(getClass.getClassLoader)
  val toolbox = mirror.mkToolBox()

  var userCaseClassesMap: Map[RecordType, String] = _

  case class InferredType(rawType: raw.Type, isSpark: Boolean)
  /**   Bail out during compilation with error message. */
  def bail(message: String) = throw new RuntimeException(message)

  /** Infer RAW's type from the Scala type. */
  def inferType(t: toolbox.u.Type): InferredType = {
    val rawType = t match {
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Int" => raw.IntType()
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Any" => raw.TypeVariable(raw.calculus.SymbolTable.next())
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Predef.String" => raw.StringType()

      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Predef.Set" => raw.SetType(inferType(t1).rawType)
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Seq" => raw.ListType(inferType(t1).rawType) // TODO can we pattern match on something else
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

  def recordTypeSym(r: RecordType): String = r match {
    case RecordType(_, Some(symName)) => symName
    case _ =>
      userCaseClassesMap.get(r).get
    //      // TODO: The following naming convention may conflict with user type names. Consider prefixing all types using `type = Prefix???`
    //      val uniqueId = if (r.hashCode() < 0) s"_n${Math.abs(r.hashCode()).toString}" else s"_p${r.hashCode().toString}"
    //      uniqueId
  }

  // Create Scala type from RAW type
  def tipe(t: raw.Type, world: World): String = {
    t match {
      case _: BoolType => "Boolean"
      case FunType(t1, t2) => ???
      case _: StringType => "String"
      case _: IntType => "Int"
      case _: FloatType => "Float"
      case r: RecordType => recordTypeSym(r)
      case BagType(innerType) => s"com.google.common.collect.ImmutableMultiset[${tipe(innerType, world)}]"
      case ListType(innerType) => s"Seq[${tipe(innerType, world)}]"
      case SetType(innerType) => s"Set[${tipe(innerType, world)}]"
      case UserType(idn) => tipe(world.userTypes(idn), world)
      case TypeVariable(v) => ???
      case _: AnyType => ???
      case _: NothingType => ???
      case _: CollectionType => ???
    }
  }

  // Scala tuple from rawtype.
  def tipeAsTuple(t: raw.Type, world: World): String = {
    logger.info(s"Typing: $t")
    val tt = t match {
      case _: BoolType => "Boolean"
      case FunType(t1, t2) => ???
      case _: StringType => "String"
      case _: IntType => "Int"
      case _: FloatType => "Float"
      case r@RecordType(_, Some(name)) => s"$name"
      case r@RecordType(atts, None) => atts.sortBy(att => att.idn).map(att => s"(${tipeAsTuple(att.tipe, world)})").mkString("(", ", ", ")")
      case BagType(innerType) => s"com.google.common.collect.ImmutableMultiset[${tipeAsTuple(innerType, world)}]"
      case ListType(innerType) => s"Seq[${tipeAsTuple(innerType, world)}]"
      case SetType(innerType) => s"Set[${tipeAsTuple(innerType, world)}]"
      case UserType(idn) => tipe(world.userTypes(idn), world)
      case TypeVariable(v) => ???
      case _: AnyType => ???
      case _: NothingType => ???
      case _: CollectionType => ???
    }
    logger.info(s"Type: $tt")
    tt
  }

  /** Build case classes that correspond to record types.
    */
  def buildCaseClasses(logicalTree: LogicalAlgebraNode, world: World, typer: Typer): Set[Tree] = {
    import org.kiama.rewriting.Rewriter.collect

    // Extractor for anonymous record types
    //    object IsAnonRecordType {
    //      def unapply(e: raw.algebra.Expressions.Exp): Option[RecordType] = typer.expressionType(e) match {
    //        case r@RecordType(_, None) => Some(r)
    //        case _ => None
    //      }
    //    }

    ///////////////////////
    object IsAnonRecordType2 {
      def unapply(t: raw.Type): Option[RecordType] = t match {
        case r@RecordType(_, None) => Some(r)
        case _ => None
      }
    }

    // Collect all record types from tree
    val collectAnonRecordTypes2 = collect[List, raw.RecordType] {
      case IsAnonRecordType2(t) => t
    }

    val resultType = typer.tipe(logicalTree)
    println(s"Result type: $resultType")
    val resultRecords = collectAnonRecordTypes2(resultType).toSet
    var i = 0;
    this.userCaseClassesMap = resultRecords.map(recType => {
      i = i + 1; recType -> s"UserRecord$i"
    }).toMap

    println("Record types:\n" + resultRecords.map(rec => rec + " " + System.identityHashCode(rec)).mkString("\n"))
    ///////////////////////

    // Collect all record types from tree
    //    val collectAnonRecordTypes = collect[List, raw.RecordType] {
    //      case IsAnonRecordType(t) => t
    //    }
    //    // Convert all collected record types to a set to remove repeated types, leaving only the structural types
    //    val anonRecordTypes = collectAnonRecordTypes(logicalTree).toSet
    //    println("All record types:\n" + anonRecordTypes.mkString("\n"))


    // Create corresponding case class
    //    val code = anonRecordTypes
    val code = resultRecords
      .map {
      case r@RecordType(atts: Seq[AttrType], None) =>
        val args = atts
          .sortBy(att => att.idn)
          .map(att => s"${att.idn}: ${tipe(att.tipe, world)}")
          .mkString(", ")
        //        val cl = s"""case class ${recordTypeSym(r)}($args)"""
        val cl = s"""case class ${recordTypeSym(r)}($args)"""
        cl
    }
    code.map(toolbox.parse)
  }

  /** Build code-generated query plan from logical algebra tree.
    */
  def buildCode(physicalTree: PhysicalAlgebraNode, world: World, typer: Typer): Tree = {

    def rawToScalaType(t: raw.Type): Tree = {
      logger.info(s"rawToScalaType: $t")
      val typeName: String = tipeAsTuple(t, world)
      logger.info(s"typeName: $typeName")
      val parsed: Tree = toolbox.parse(typeName)
      //      logger.info(s"Parsed: $parsed, ${showRaw(parsed)}")
      parsed match {
        case TypeApply(container, List(targ)) =>
          //          logger.info(s"Container: $container, ${showRaw(container)}, targs: $targ, ${showRaw(targ)}")
          targ
      }
    }

    def nodeScalaType(logicalNode: LogicalAlgebraNode): Tree = {
      logger.info(s"Algebra: ${logicalNode}")
      val scalaType = rawToScalaType(typer.tipe(logicalNode))
      logger.info(s"Scala type: $scalaType")
      scalaType
    }

    def expScalaType(expression: Exp): Tree = {
      logger.info(s"Expression: ${expression}")
      rawToScalaType(typer.expressionType(expression))
    }

    def exp(e: Exp, argType: Option[Tree] = None): Tree = {
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

      def recurse(e: Exp): String = {
        e match {
          case Null => "null"
          case BoolConst(v) => v.toString
          case IntConst(v) => v
          case FloatConst(v) => v
          case StringConst(v) => s""""$v""""
          case _: Arg => "arg"
          case RecordProj(e1, idn) => s"${recurse(e1)}.$idn"
          case RecordCons(atts) =>
            val tt = typer.expressionType(e).asInstanceOf[RecordType]
            val sym = userCaseClassesMap.get(tt) match {
              case Some(caseClassName) => logger.info(s"Record: ${tt} -> $caseClassName"); caseClassName
              case _ => ""
            }
            logger.info(s"Type: $tt, ${System.identityHashCode(tt)}")
            //            val sym = recordTypeSym(tt match {
            //              case r: RecordType => r
            //              case _ => ???
            //            })
            val vals = atts.sortBy(att => att.idn).map(att => recurse(att.e)).mkString(",")
            s"""$sym($vals)"""
          case IfThenElse(e1, e2, e3) => s"if (${recurse(e1)}) ${recurse(e2)} else ${recurse(e3)}"
          case BinaryExp(op, e1, e2) => s"${recurse(e1)} ${binaryOp(op)} ${recurse(e2)}"
          case MergeMonoid(m, e1, e2) => m match {
            case _: SumMonoid => s"${recurse(e1)} + ${recurse(e2)}"
            case _: MaxMonoid => ???
            case _: MultiplyMonoid => ???
            case _: AndMonoid => s"${recurse(e1)} && ${recurse(e2)}"
            case _: OrMonoid => ???
            case _: SetMonoid => s"${recurse(e1)} ++ ${recurse(e2)}"
            case _: BagMonoid => ???
            case _: ListMonoid => ???
          }
          case UnaryExp(op, e1) => op match {
            case _: Not => s"!${recurse(e1)}"
            case _: Neg => s"-${recurse(e1)}"
            case _: ToBool => s"${recurse(e1)}.toBoolean"
            case _: ToInt => s"${recurse(e1)}.toInt"
            case _: ToFloat => s"${recurse(e1)}.toFloat"
            case _: ToString => s"${recurse(e1)}.toString"
          }
          case ConsCollectionMonoid(m: SetMonoid, e) => s"Set(${recurse(e)})"
          case ConsCollectionMonoid(m: BagMonoid, e) => ???
          case ConsCollectionMonoid(m: ListMonoid, e) => s"List(${recurse(e)})"
          case ZeroCollectionMonoid(m: SetMonoid) => s"Set()"
          case ZeroCollectionMonoid(m: BagMonoid) => ???
          case ZeroCollectionMonoid(m: ListMonoid) => s"List()"
        }
      }
      val expression = recurse(e)
      val code = argType match {
        case Some(argName) => q"((arg:$argName) => ${toolbox.parse(expression)})"
        case None => toolbox.parse(s"(arg => ${expression})")
      }
      code
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
      case ScalaNest(logicalNode, m, e, f, p, g, child) => m match {
        case m1: PrimitiveMonoid =>
          val z1 = zeroExp(m1)
          //              logger.debug("grouped:\n{}", grouped.mkString("\n"))
          //              logger.debug("map1:\n{}", map1.mkString("\n"))
          //              logger.debug("map2:\n{}", map2.mkString("\n"))
          //              logger.debug("map3:\n{}", map3.mkString("\n"))
          val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
          q"""val child = ${build(child)}
              logger.debug("Child:\n{}", child.mkString("\n"))
              val grouped = child.groupBy(${exp(f)})
              val map1 = grouped.toList.map(v => (v._1, v._2.filter(${exp(p)})))
              val map2 = map1.map(v => (v._1, v._2.map(${exp(f1)})))
              val map3 = map2.map(v => (v._1, v._2.foldLeft(${zero(m1)})(${fold(m1)})))
              map3"""
        case m1: BagMonoid => ???
        case m1: ListMonoid => ???
        // TODO: similar implementation as for the spark operator
        //          val f1 = q"""(arg => if (${exp(g)}(arg) == null) List() else ${exp(e)}(arg))""" // TODO: Remove indirect function call
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
      case ScalaOuterJoin(logicalNode, p, left, right) =>
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
      case r@ScalaReduce(logicalNode, m, e, p, child) =>
        val eCode = exp(e)
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
            q"""val e = ${build(child)}.filter(${exp(p)}).map($eCode)
          com.google.common.collect.ImmutableMultiset.copyOf(scala.collection.JavaConversions.asJavaIterable(e))"""
          case _: ListMonoid =>
            q"""${build(child)}.filter(${exp(p)}).map($eCode).to[scala.collection.immutable.List]"""
          case _: SetMonoid =>
            q"""${build(child)}.filter(${exp(p)}).map($eCode).to[scala.collection.immutable.Set]"""
        }
        if (r eq physicalTree)
          code
        else
          q"""List($code)"""

      case ScalaSelect(logicalNode, p, child) => // The AST node Select() conflicts with built-in node Select() in c.universe
        q"""${build(child)}.filter(${exp(p)})"""
      case ScalaScan(logicalNode, name, _) =>
        Ident(TermName(name))

      case ScalaJoin(logicalNode, p, left, right) => ???
      case ScalaMerge(logicalNode, m, left, right) => ???
      case ScalaOuterUnnest(logicalNode, path, pred, child) => ???
      case ScalaToSparkNode(node) => ???
      case ScalaUnnest(logicalNode, path, pref, child) => ???


      /////////////////////////////////////////////////////
      // Spark
      /////////////////////////////////////////////////////
      // Spark operators
      case SparkScan(logicalNode, name, tipe) => Ident(TermName(name))
      //            q"""${accessPaths(name)}"""
      /*
      * Fegaras: Ch 6. Basic Algebra O1
      * Join(X, Y, p), joins the collections X and Y using the join predicate p.
      * This join is not necessarily between two sets; if, for example, X is a list
      * and Y is a bag, then, according to Eq. (D4), the output is a bag.
      */
      case r@SparkJoin(logicalNode, p, left, right) =>
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
      case SparkSelect(logicalNode, p, child: PhysicalAlgebraNode) =>
        val childTypeName = nodeScalaType(child.logicalNode)
        val pCode = exp(p, Some(childTypeName))
        logger.info(s"[SELECT] p: $pCode")
        q"""${build(child)}.filter($pCode)"""

      /*
      * Fegaras: Ch 6. Basic Algebra O3
      * unnest(X, path, p), returns the collection of all pairs (x, y) for each x \in X and for each y \in x.path
      * that satisfy the predicate p(x, y).
      */
      case SparkUnnest(logicalNode, path, pred, child) =>
        val childTypeName = nodeScalaType(child.logicalNode)
        val pathCode = exp(path, Some(childTypeName))
        val pathType = expScalaType(path)
        val predCode = exp(pred)
        logger.info(s"[UNNEST] path: $pathCode, pred: $predCode")
        val childCode = build(child)
        //            logger.debug("{}.path = {} \n{}", x, pathValues)
        val code = q"""
          $childCode.flatMap(x => {
            val pathValues = $pathCode(x)
            pathValues.map((y:$pathType) => (x,y)).filter($predCode)
            }
          )
        """

        q"""
        val start = "************ SparkUnnest ************"
        val res = $code
        val end= "************ SparkUnnest ************"
        res
        """
      /* Fegaras: Ch 6. Basic Algebra O4
      Reduce(X, Q, e, p), collects the values e(x) for all x \in X that satisfy p(x) using the accumulator Q.
      The reduce operator can be thought of as a generalized version of the relational projection operator.
      */
      case SparkReduce(logicalNode, m, e, p, child) =>
        val pCode = exp(p)
        val eCode = exp(e)
        logger.info(s"[REDUCE] $m, e: $eCode, p: $pCode")
        val childCode = build(child)
        val filterMapCode = q"""
     val childRDD = $childCode
     childRDD
       .filter($pCode)
       .map($eCode)
     """
        val code = m match {
          case m1: PrimitiveMonoid =>
            /* TODO: Use Spark implementations of monoids if supported. For instance, RDD has max and min actions.
             * Compare the Spark implementations versus a generic code generator based on fold
             */
            q"""$filterMapCode.fold(${zero(m1)})(${fold(m1)})"""

          case _: ListMonoid =>
            q"""$filterMapCode
          .toLocalIterator
          .to[scala.collection.immutable.List]"""

          case _: BagMonoid =>
            /* RDD.countByValue() should reduce the amount of data sent to the server by sending a single (v, count)
             * tuple for all values v in the RDD.
             */
            q"""
          val m = $filterMapCode.countByValue()
          val b = com.google.common.collect.ImmutableMultiset.builder[Any]()
          m.foreach( p => b.addCopies(p._1, p._2.toInt) )
          b.build()"""

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

      case SparkNest(logicalNode, m, e, f, p, g, child) =>
        val eCode = exp(e)
        val fCode = exp(f)
        val pCode = exp(p)
        logger.info(s"[NEST] m: $m, e: $eCode, f: $fCode, p: $pCode, g: ${exp(g)}")
        val t = typer.expressionType(e)
        val st: String = tipe(t, world)
        logger.info("Type of e: {}", st)
        val childTree: Tree = build(child)

        val tp = toolbox.parse(st)

        val code = m match {
          case m1: PrimitiveMonoid =>
            val z1 = zeroExp(m1)
            val f1 = IfThenElse(BinaryExp(Eq(), g, Null), z1, e)
            //  logger.debug("groupBy:\n{}", toString(grouped))
            //  logger.debug("filteredP:\n{}", toString(filteredP))
            //  logger.debug("mapped:\n{}", toString(mapped))
            //  logger.debug("folded:\n{}", toString(folded))
            q"""
            val grouped = $childTree.groupBy($fCode)
            val filteredP = grouped.mapValues(v => v.filter($pCode))
            val mapped = grouped.mapValues(v => v.map(${exp(f1)}))
            val folded = mapped.mapValues(v => v.fold(${zero(m1)})(${fold(m1)}))
            folded"""

          case m1: CollectionMonoid =>
            // TODO: Does not handle dotequality.
            /* TODO: The filter by p probably can be applied at the start of this operator implementation,
             * in a common code path before branching to handle the different monoids.
             */
            // Since the zero element of a collection monoid is an empty collection,
            // instead of using fold we can just filter any elements such that g(w) == null
            val filterGNulls = IfThenElse(BinaryExp(Eq(), g, Null), BoolConst(false), BoolConst(true))
            val commonCode =
            //            logger.debug("groupBy:\n{}", toString(grouped))
            //            logger.debug("filteredP:\n{}", toString(filteredP))
            //            logger.debug("filteredG:\n{}", toString(filteredG))
            //            logger.debug("mapped:\n{}", toString(mapped))
              q"""
            val grouped = $childTree.groupBy($fCode)
            val filteredP = grouped.mapValues(v => v.filter($pCode))
            val filteredG = filteredP.mapValues(v => v.filter(${exp(filterGNulls)}))
            val mapped = filteredG.mapValues(v => v.map($eCode))
            """
            //            val mapped = filteredG.mapValues((v:$tp)=> v.map(${exp(e)}))

            //            logger.debug("reduced:\n{}", toString(reduced))
            //            logger.debug("reduced:\n{}", toString(reduced))
            //            logger.debug("reduced:\n{}", toString(reduced))
            val reduceCode = m1 match {
              case m2: SetMonoid =>
                q"""
            val reduced = mapped.mapValues(v => v.toSet)
            reduced
              """

              case m1: BagMonoid =>
                q"""
            val reduced = mapped.mapValues(v => com.google.common.collect.ImmutableMultiset.copyOf(scala.collection.JavaConversions.asJavaIterable(v)))
            reduced
          """
              case m1: ListMonoid =>
                q"""
            val reduced = mapped.mapValues(v => v.toList)
            reduced
            """
            }

            q"""..$commonCode
                ..$reduceCode
              """
        }

        q"""
          val start = "************ SparkNest ************"
          def toString[R](rdd: RDD[R]): String = {
            rdd.collect().toList.mkString("\n")
          }
          val sparkNest = $code
          val end = "************ SparkNest ************"
          sparkNest"""

      case SparkMerge(logicalNode, m, left, right) => ???
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
      case SparkOuterJoin(logicalNode, p, left, right) =>
        val leftNodeType = nodeScalaType(left.logicalNode)
        val rightNodeType = nodeScalaType(right.logicalNode)

        val code = q"""
            val start = "************ SparkOuterJoin ************"
            val leftRDD:RDD[$leftNodeType] = ${build(left)}
            val rightRDD:RDD[$rightNodeType] = ${build(right)}
            val matching:RDD[($leftNodeType, $rightNodeType)] = leftRDD
              .cartesian(rightRDD)
              .filter(tuple => tuple._1 != null)
              .filter(${exp(p)})

            val resWithOption: RDD[($leftNodeType, ($leftNodeType, Option[$rightNodeType]))] = leftRDD
              .map(v => (v, v))
              .leftOuterJoin(matching)

            val res:RDD[($leftNodeType, $rightNodeType)] = resWithOption.map( {
              case (v1, (v2, None)) => (v1, null)
              case (v1, (v2, Some(w))) => (v1, w)
            })
            val end = "************ SparkOuterJoin ************"
            res
            """
        code
      case SparkOuterUnnest(logicalNode, path, pred, child) => ???
    }

    build(physicalTree)
  }


  def extractParams(tree: Tree): (Tree, Tree) = tree match {
    case q"new $name( ..$params )" =>
      logger.info(s"Extracted params: $params")
      params match {
        case List(queryTree, catalogTree) =>
          logger.info(s"query: $queryTree, catalog: $catalogTree")
          (queryTree.asInstanceOf[Tree], catalogTree.asInstanceOf[Tree])
        //        case List(queryTree:c.Expr[String], catalogTree:c.Expr[HList]) => (queryTree, catalogTree)
        //        case q"($query:String, $catalog:HList)"  List(queryTree:c.Expr[String], catalogTree:c.Expr[HList]) => (queryTree, catalogTree)
      }
  }

  case class AccessPath(name: String, rawType: raw.Type, isSpark: Boolean)

  sealed abstract class QueryLanguage

  case object Krawl extends QueryLanguage

  case object OQL extends QueryLanguage

  def extractQueryAndAccessPath(makro: Tree): (String, QueryLanguage, List[AccessPath]) = {
    /**
     * Check if the query string is known at compile time.
     * If so, build the algebra tree, then build the executable code, and return that to the user.
     * If not, build code that calls the interpreted executor and return that to the user.
     */
    // Typecheck the full tree to resolve references like List[Student] to scala.List[raw.Student]
    val typ = toolbox.typecheck(makro.duplicate)
    //    logger.debug("Typed tree:\n" + showRaw(typ))
    typ match {
      case ClassDef(_, className: TypeName, _, Template(_, _, body: List[Tree])) =>
        logger.info("Analysing class: " + className)
        var query: Option[String] = None
        var queryLanguage: Option[QueryLanguage] = None
        val accessPaths = new ArrayBuffer[AccessPath]()
        body.foreach(v => {
          logger.debug("Checking element: " + showCode(v))
          //          logger.debug("Tree: " + showRaw(v))
          v match {
            case ValDef(_, TermName("query "), _, Literal(Constant(queryString: String))) =>
              logger.info("Found monoid comprehension query: " + queryString)
              if (query.isDefined) {
                bail(s"Multiple queries found. Previous: ${query.get}, Current: $queryString")
              }
              query = Some(queryString)
              queryLanguage = Some(Krawl)

            case ValDef(_, TermName("oql "), _, Literal(Constant(queryString: String))) =>
              val trimmedString = queryString.trim
              logger.info("Found oql query: " + trimmedString)
              if (query.isDefined) {
                bail(s"Multiple queries found. Previous: ${query.get}, Current: $trimmedString")
              }
              query = Some(trimmedString)
              queryLanguage = Some(OQL)

            case ValDef(_, TermName(termName), typeTree, _) =>
              val inferredType = inferType(typeTree.tpe)
              logger.info("Found data source: " + termName + ", Raw type: " + inferredType.rawType + ", isSpark: " + inferredType.isSpark)
              accessPaths += AccessPath(termName.trim, inferredType.rawType, inferredType.isSpark)

            case _ =>
            //              logger.info("Ignoring element")
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

  def parseOqlQuery(query: String): Either[QueryError, LogicalAlgebra.LogicalAlgebraNode] = {
    // world is not used for the time being. Eventually, send it to the compilation server.
    OQLToPlanCompilerClient(query) match {
      case Right(logicalAlgebra) => Right(logicalAlgebra)
      case Left(error) => Left(new ParserError(error))
    }
  }

  def generateCodeNoMacros[T:TypeTag](query: String, queryLanguage: QueryLanguage, accessPaths: List[AccessPath], dataSource: T): () => Any = {
    val sources: Map[String, Type] = accessPaths.map(ap => (ap.name, ap.rawType)).toMap
    val world = new World(sources)

    // Parse the query, using the catalog generated from what the user gave.
    val parseResult: Either[QueryError, LogicalAlgebraNode] = queryLanguage match {
      case OQL => parseOqlQuery(query)
      case Krawl => Query(query, world)
    }

    parseResult match {
      case Right(logicalTree) =>
        var algebraStr = LogicalAlgebraPrettyPrinter(logicalTree)
        //        logger.info("Logical algebra: {}", algebraStr)
        val isSpark: Map[String, Boolean] = accessPaths.map(ap => (ap.name, ap.isSpark)).toMap
        val physicalTree = LogicalToPhysicalAlgebra(logicalTree, isSpark)
        logger.info("Physical algebra: {}", PhysicalAlgebraPrettyPrinter(physicalTree))

        val typer = new algebra.Typer(world)
        val caseClasses: Set[Tree] = buildCaseClasses(logicalTree, world, typer)
        logger.info("case classes:\n{}", caseClasses.map(showCode(_)).mkString("\n"))

        val generatedTree: Tree = buildCode(physicalTree, world, typer)
        //        logger.info("Query execution code:\n{}", showCode(generatedTree))
        //
        //        /* Generate the actual scala code. Need to extract the class parameters and the body from the annotated
        //        class. The body and the class parameters will be copied unmodified in the resulting code.
        //        */
//        val q"class $className(..$paramsAsAny) extends RawQuery { ..$body }" = makro

        /* Generating the expanded class plus the companion:
        http://stackoverflow.com/questions/21032869/create-or-extend-a-companion-object-using-a-macro-annotation-on-the-class
        */
//        val moduleName = tq"FooBarQuery"

        val companion = q"""
  import org.apache.spark.rdd.RDD
  import raw.publications.{Author, Publication}

  object FooBarQuery extends com.typesafe.scalalogging.StrictLogging {
    ..$caseClasses
    def query(authors:RDD[Author]) = {
      println("Executing: " + authors)
      $generatedTree
    }
    def foo = println("foo")
  }

  FooBarQuery
  """

        //        val clazz = q"""
        //  class $className(..$methodDefParameters) extends RawQuery {
        //    ..$body
        //    def computeResult = $moduleName.apply(..$methodCallParameters)
        //  }
        //"""
        //        val block = Block(List(companion, clazz), Literal(Constant(())))

        val scalaCode = showCode(companion)
        logger.info("Generated code:\n{}", scalaCode)

//        val compiled = toolbox.define(companion.asInstanceOf[ImplDef])
        val compiled = toolbox.compile(companion)
        logger.info(s"Compiled: $compiled")
        compiled

//        c.Expr[Any](block)

      case Left(err) => bail(err.err)
    }
  }

  def query[T:TypeTag](query:String, dataSource: T): () => Any = {
//    val rddAuthorTree: ru.Tree = tq"org.apache.spark.rdd.RDD[raw.executionserver.Author]"
//    dataSource.getC
    val tt: ru.Type = typeOf[T]
    val infered = inferType(tt)
    logger.info(s"Infered: $dataSource $infered")

    val authorsPath = new AccessPath("authors",  infered.rawType, infered.isSpark)
    var accessPaths:List[AccessPath] = List(authorsPath)
    generateCodeNoMacros(query, OQL, accessPaths, dataSource)
  }

  // TODO: Work in Progress
  def invoke(query:AnyRef, params:AnyRef*) = {

    println(s"Classload: ${mirror.classLoader}")


    val im: ru.InstanceMirror = mirror.reflect(query)
    println(s"Symbol: ${im.symbol}")
    val ccType: ru.Type = im.symbol.toType
    val com: ru.Type = ccType.companion
    println("Companion: " + com)
    println("Declaratiations: " + com.decls)

//    val queryMethod = ccType.decl(ru.TermName("query")).asMethod
    val queryMethod = ccType.decl(ru.TermName("foo")).asMethod
    println("Query method: " + queryMethod)
    val mm: ru.MethodMirror = im.reflectMethod(queryMethod)
    println("Query method mirror: " + mm)
    mm()
//    mm(params.head)

//    val innerMirror = im.reflectClass(im.symbol)
//    println(s"Inner: $innerMirror")
//    val cm: ru.ClassMirror = mirror.reflectClass(innerMirror.symbol)
//    val cType: ru.Type = innerMirror.symbol.toType
//
//    mm(params)
  }
}
