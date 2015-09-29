package raw

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import raw.calculus.SymbolTable.DataSourceEntity
import raw.calculus._
import raw.executor.{AbstractClosableIterator, ClosableIterator, RawScanner}

import scala.annotation.StaticAnnotation
import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.language.experimental.macros


class rawQueryAnnotation extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro RawImpl.query_impl
}


class RawIterable[T](rawScanner: RawScanner[T], iteratorBuffer: ArrayBuffer[AbstractClosableIterator[_]]) extends mutable.AbstractIterable[T] {
  override def iterator: Iterator[T] = {
    var newIter = rawScanner.iterator
    iteratorBuffer += newIter
    newIter
  }
}

abstract class RawQuery extends StrictLogging {
  val openIters = new ArrayBuffer[AbstractClosableIterator[_]]()

  def openScanner[T](scanner: RawScanner[T]): Iterable[T] = {
    logger.info("Opening iterator: " + scanner)
    new RawIterable(scanner, openIters)
  }

  def openScanner[T](iterable: Iterable[T]): Iterable[T] = {
    logger.info("Bypassing: " + iterable)
    iterable
  }


  def closeAllIterators(): Unit = {
    logger.info("Closing iterators: " + openIters)
    openIters.foreach(_.close())
    openIters.clear()
  }

  //  val query: String
  def computeResult: Any
}

object RawImpl {
  def toCannonicalForm(recordType: RecordType): Seq[AttrType] = {
    recordType.atts.sortBy(_.idn)
  }
}

object QueryLanguages {
  def apply(qlString:String): QueryLanguage = {
    qlString match {
      case "oql" => OQL
      case "plan" => LogicalPlan
      case "qrawl" => Qrawl
      case _ => throw new IllegalArgumentException(s"Unknown query language: $qlString. Expected one of ${names}")
    }
  }

  sealed trait QueryLanguage {
    /**
     * @param name Name of the query language. This is used as the name of the val holding the query in the macro
     *             generated for the query.
     */
    val name:String
  }

  case object Qrawl extends QueryLanguage {
    val name = "qrawl"
  }

  case object OQL extends QueryLanguage {
    val name = "oql"
  }

  case object LogicalPlan extends QueryLanguage {
    val name = "plan"
  }

  val names = List(Qrawl.name, OQL.name, LogicalPlan.name)
}


class RawImpl(val c: scala.reflect.macros.whitebox.Context) extends StrictLogging {
  val loggerQueries = LoggerFactory.getLogger("raw.queries")
  import QueryLanguages._

  import c.universe._

  /** Bail out during compilation with error message. */
  def bail(message: String) = c.abort(c.enclosingPosition, message)

  case class InferredType(rawType: raw.Type, isSpark: Boolean)

  /** Infer RAW's type from the Scala type. */
  def inferType(t: c.Type): InferredType = {
    val rawType = t match {
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Int" => raw.IntType()
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Any" => raw.TypeVariable(raw.calculus.SymbolTable.next())
      case TypeRef(_, sym, Nil) if sym.fullName == "scala.Predef.String" => raw.StringType()
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Predef.Set" => raw.CollectionType(SetMonoid(), inferType(t1).rawType)
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Seq" => raw.CollectionType(ListMonoid(), inferType(t1).rawType) // TODO can we pattern match on something else
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.List" || sym.fullName == "scala.collection.immutable.List" => raw.CollectionType(ListMonoid(), inferType(t1).rawType)
      case TypeRef(_, sym, List(t1)) if sym.fullName == "scala.Iterable" => raw.CollectionType(ListMonoid(), inferType(t1).rawType) // TODO: Can an Iterable be represented by a raw.List type?
      case TypeRef(_, sym, List(t1)) if sym.fullName == "raw.executor.RawScanner" => raw.CollectionType(ListMonoid(), inferType(t1).rawType) // TODO: Can an Iterable be represented by a raw.List type?
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
        raw.CollectionType(ListMonoid(), inferType(t1).rawType)

      case TypeRef(pre, sym, args) =>
        bail(s"Unsupported TypeRef($pre, $sym, $args)")
    }

    val isSpark = t.typeSymbol.fullName.equals("org.apache.spark.rdd.RDD")
    InferredType(rawType, isSpark)
  }

  //  var userCaseClassesMap: Map[RecordType, String] = _
  var userCaseClassesMap: Map[Seq[AttrType], String] = _

  def recordTypeSym(r: RecordType): Option[String] = r match {
    case RecordType(_, Some(symName)) => Some(symName)
    case RecordType(_, None) => userCaseClassesMap.get(RawImpl.toCannonicalForm(r))
  }

  def buildScalaType(t: raw.Type, world: World): String = {
    val baseType = t match {
      case _: BoolType => "Boolean"
      case FunType(t1, t2) => ???
      case _: StringType => "String"
      case _: IntType => "Int"
      case _: FloatType => "Float"
      case r@RecordType(atts, _) =>
        recordTypeSym(r) match {
          case Some(sym) => sym
          case None =>
            atts
              .map(att => s"(${buildScalaType(att.tipe, world)})")
              .mkString("(", ", ", ")")
        }
      //      case CollectionType(BagMonoid(), innerType) => s"com.google.common.collect.ImmutableMultiset[${buildScalaType(innerType, world)}]"
      //      case CollectionType(BagMonoid(), innerType) => s"scala.collection.immutable.Bag[${buildScalaType(innerType, world)}]"
      case CollectionType(BagMonoid(), innerType) => s"Iterable[${buildScalaType(innerType, world)}]"
      case CollectionType(ListMonoid(), innerType) => s"Iterable[${buildScalaType(innerType, world)}]"
      case CollectionType(SetMonoid(), innerType) => s"Iterable[${buildScalaType(innerType, world)}]"
      case UserType(idn) => buildScalaType(world.tipes(idn), world)
      case TypeVariable(v) => ???
      case _: AnyType => ???
      case _: NothingType => ???
      case _: CollectionType => ???
    }
    //    logger.info(s"Type: $tt")
    if (t.nullable)
      s"Option[$baseType]"
    else
      baseType
  }

  /** Collect all anonymous record types in the tree.
    */
  def collectAnonRecordTypes(tree: Calculus.Exp, world: World, analyzer: SemanticAnalyzer): Set[RecordType] = {
    import org.kiama.rewriting.Rewriter._

    val anonRecordTypes = scala.collection.mutable.Set[RecordType]()

    val queryAnonRecordTypes = everywhere(query[Calculus.Exp] {
      case e => logger.debug(s"Exp ${CalculusPrettyPrinter(e)} has type ${PrettyPrinter(analyzer.tipe(e))}"); analyzer.tipe(e) match {
        case r @ RecordType(_, None) => anonRecordTypes += r
        case _ =>
      }
    })

    queryAnonRecordTypes(tree)

    anonRecordTypes.toSet
  }

  /** Build case classes for the anonymous record types used to hold the results of Reduce nodes
    */
  def buildCaseClasses(tree: Calculus.Exp, world: World, analyzer: SemanticAnalyzer): Set[Tree] = {

    val resultRecords = collectAnonRecordTypes(tree, world, analyzer)
      logger.debug(s"buildCaseClasses $resultRecords")

    // Create a map between RecordType and case class names
    this.userCaseClassesMap = {
      var i = 0
      resultRecords.map(recType => {
        i = i + 1
        RawImpl.toCannonicalForm(recType) -> s"UserRecord$i"
      }).toMap
    }

    // Create corresponding case class
    val code: Set[String] = resultRecords
      .map(r => {
      val args = r.atts
        .map(att => s"${att.idn}: ${buildScalaType(att.tipe, world)}")
        .mkString(", ")
      logger.info(s"Build case class: $r.atts => $args")
      recordTypeSym(r) match {
        case Some(sym) => s"""case class ${sym}($args)"""
        case None => bail(s"No case class available for record: $r")
      }
    })

    code.map(c.parse)
  }

  /** Build code-generated query plan from logical algebra tree.
    */
  def buildCode(treeExp: Calculus.Exp, world: World, analyzer: PhysicalAnalyzer): Tree = {
    import Calculus._

    def rawToScalaType(t: raw.Type): c.universe.Tree = {
            logger.info(s"rawToScalaType: $t")
      val typeName: String = buildScalaType(t, world)
            logger.info(s"typeName: $typeName")
      val parsed: c.Tree = c.parse(typeName)
            logger.info(s"Parsed: $parsed, ${showRaw(parsed)}")
      parsed match {
        case TypeApply(container, List(targ)) =>
          //          logger.info(s"Container: $container, ${showRaw(container)}, targs: $targ, ${showRaw(targ)}")
          targ
        case _ => tq"$parsed"
      }
    }

    def nodeScalaType(logicalNode: LogicalAlgebraNode): c.universe.Tree = {
      //      logger.info(s"Algebra: ${logicalNode}")
      val scalaType = rawToScalaType(analyzer.tipe(logicalNode))
      //      logger.info(s"Scala type: $scalaType")
      scalaType
    }

    def expScalaType(expression: Exp): c.universe.Tree = {
      //      logger.info(s"Expression: ${expression}")
      rawToScalaType(analyzer.tipe(expression))
    }

    def exp(e: Exp): Tree = e match {
      case _: Null             => q"null"
      case BoolConst(v)        => q"$v"
      case IntConst(v)         => q"${v.toInt}"
      case FloatConst(v)       => q"${v.toFloat}"
      case StringConst(v)      => q"$v"
      case IdnExp(idn)         => Ident(TermName(idnName(idn)))
      case RecordProj(e1, idn) =>
        val id = TermName(idn)
        q"${build(e1)}.$id"
      case RecordCons(atts) =>
        val tt = analyzer.tipe(e).asInstanceOf[RecordType]
        val sym = userCaseClassesMap.get(RawImpl.toCannonicalForm(tt)) match {
          case Some(caseClassName) => logger.info(s"Record: $tt -> $caseClassName"); caseClassName
          case _ => ""
        }
        val vals = atts
          .map(att => build(att.e))
          .mkString(",")
        //            logger.info(s"exp(): $atts => $vals")
        c.parse(s"""$sym($vals)""")
      case IfThenElse(e1, e2, e3) => q"if (${build(e1)}) ${build(e2)} else ${build(e3)}"
      case BinaryExp(op, e1, e2) => op match {
        case _: Eq  => q" ${build(e1)} == ${build(e2)}"
        case _: Neq => q" ${build(e1)} != ${build(e2)}"
        case _: Ge  => q" ${build(e1)} >= ${build(e2)}"
        case _: Gt  => q" ${build(e1)} > ${build(e2)}"
        case _: Le  => q" ${build(e1)} <= ${build(e2)}"
        case _: Lt  => q" ${build(e1)} < ${build(e2)}"
        case _: Sub => q" ${build(e1)} - ${build(e2)}"
        case _: Div => q" ${build(e1)} / ${build(e2)}"
        case _: Mod => q" ${build(e1)} % ${build(e2)}"
      }
      case MergeMonoid(m, e1, e2) => m match {
        case _: SumMonoid => q"${build(e1)} + ${build(e2)}"
        case _: MaxMonoid => ???
        case _: MultiplyMonoid => ???
        case _: AndMonoid => q"${build(e1)} && ${build(e2)}"
        case _: OrMonoid => q"${build(e1)} || ${build(e2)}"
        case _: SetMonoid => q"${build(e1)} ++ ${build(e2)}"
        case _: BagMonoid => ???
        case _: ListMonoid => ???
      }
      case UnaryExp(op, e1) => op match {
        case _: Not => q"!${build(e1)}"
        case _: Neg => q"-${build(e1)}"
        case _: ToBool => q"${build(e1)}.toBoolean"
        case _: ToInt => q"${build(e1)}.toInt"
        case _: ToFloat => q"${build(e1)}.toFloat"
        case _: ToString => q"${build(e1)}.toString"
        case _: ToBag => q"${build(e1)}" // .toList" // TODO
        case _: ToList => q"${build(e1)}.toList"
        case _: ToSet => q"${build(e1)}.toSet"
      }
      case ConsCollectionMonoid(m, e1) => m match {
        case _: SetMonoid => q"Set(${build(e1)})"
        case _: BagMonoid => ???
        case _: ListMonoid => q"List(${build(e1)})"
      }
      case ZeroCollectionMonoid(m) => m match {
        case _: SetMonoid => q"Set()"
        case _: BagMonoid => ???
        case _: ListMonoid => q"List()"
      }
      case ExpBlock(bs, e1) =>
        val vals = bs.map { case Bind(PatternIdn(idn), e1) => q"val ${TermName(idnName(idn))} = ${{build(e1)}}"}
        q"""
        {
          ..$vals
          ${build(e1)}
        }
        """
    }

    def patternTerms(p: Pattern): Seq[Tree] = p match {
      case PatternProd(ps)         => ps.flatMap(patternTerms)
      case PatternIdn(idn: IdnDef) => Seq(pq"${TermName(idnName(idn))}")
    }

    def patternIdents(p: Pattern): Seq[Tree] = p match {
      case PatternProd(ps)         => ps.flatMap(patternTerms)
      case PatternIdn(idn: IdnDef) => Seq(q"${Ident(TermName(idnName(idn)))}")
    }

    def patternType(p: Pattern) = p match {
      case PatternIdn(idn) => buildScalaType(analyzer.idnDefType(idn), world)
      case p: PatternProd  => buildScalaType(analyzer.patternType(p), world)
    }

    /** Get the nullable identifiers from a pattern.
      * Used by the Nest to filter out Option[...]
      */
    def patternNullable(p: Pattern): Seq[Tree] = {
      def recurse(p: Pattern, t: raw.Type): Seq[Option[Tree]] = p match {
        case PatternProd(ps) =>
          val t1 = t.asInstanceOf[RecordType]
          ps.zip(t1.atts).flatMap { case (p1, att) => recurse(p1, att.tipe) }
        case PatternIdn(idn: IdnDef) if t.nullable =>
          Seq(Some(q"${Ident(TermName(idnName(idn)))}"))
        case _ =>
          Seq(None)
      }

      recurse(p, analyzer.patternType(p)).flatten
    }

    /** Return the de-nulled identifiers.
      * e.g. given a `child` with type record(name: string, age: option[int]) returns
      *   val name = child._1
      *   val age = child._2.get
      */
    def denulledIdns(parent: String, p: Pattern): Seq[Tree] = {
      def projIdx(idxs: Seq[Int]): String =
        s"$parent.${idxs.map { case idx => s"_${idx + 1}" }.mkString(".")}"

      def recurse(p: Pattern, t: raw.Type, idxs: Seq[Int]): Seq[Tree] = p match {
        case PatternProd(ps) =>
          val t1 = t.asInstanceOf[RecordType]
          ps.zip(t1.atts).zipWithIndex.flatMap { case ((p1, att), idx) => recurse(p1, att.tipe, idxs :+ idx) }
        case PatternIdn(idn) =>
          if (t.nullable)
            Seq(q"val ${TermName(idnName(idn))} = ${c.parse(projIdx(idxs))}.get")
          else
            Seq(q"val ${TermName(idnName(idn))} = ${c.parse(projIdx(idxs))}")
      }

      recurse(p, analyzer.patternType(p), Seq())
    }

    /** Return the denullable for a pattern.
     */
    def patternDenullable(p: Pattern): Tree = {
      def recurse(p: Pattern, t: raw.Type): Tree = p match {
        case PatternProd(ps) =>
          val t1 = t.asInstanceOf[RecordType]
          q"(..${ps.zip(t1.atts).map { case (p1, att) => recurse(p1, att.tipe) }})"
        case PatternIdn(idn) =>
          if (t.nullable)
            q"${Ident(TermName(idnName(idn)))}.get"
          else
            q"${Ident(TermName(idnName(idn)))}"
      }

      recurse(p, analyzer.patternType(p))
    }

    def lambda1(p: Pattern, e: Exp): Tree = {
      p match {
        // Handle case of single pattren idn separately to generate more readable code
        case PatternIdn(idn) =>
          val arg = c.parse(s"${idnName(idn)}: ${patternType(p)}")
          q"($arg => ${build(e)})"
        case p: PatternProd =>
          val arg = c.parse(s"__arg: ${patternType(p)}")
          val pcase = pq"(..${patternTerms(p)})" // pq"(..${patternTerms(p)})"
          q"($arg => __arg match { case $pcase => ${build(e)} })"
      }
    }

    def lambda2(p1: Pattern, p2: Pattern, e: Exp): Tree = {
      val arg = c.parse(s"__arg: (${patternType(p1)}, ${patternType(p2)})")
      val pcase1 = pq"(..${patternTerms(p1)})"
      val pcase2 = pq"(..${patternTerms(p2)})"
      val pcase = pq"($pcase1, $pcase2)"
      q"($arg => (__arg._1, __arg._2) match { case $pcase => ${build(e)} })"
    }

    /** Zero of a primitive monoid.
      */
    def zero(m: PrimitiveMonoid): Tree = m match {
      case _: AndMonoid => q"true"
      case _: OrMonoid => q"false"
      case _: SumMonoid => q"0"
      case _: MultiplyMonoid => q"1"
      case _: MaxMonoid | _: MinMonoid => throw new UnsupportedOperationException(s"$m has no zero")
    }

    /** Merge/Fold of two primitive monoids.
      */
    def fold(m: PrimitiveMonoid): Tree = m match {
      case _: AndMonoid => q"((a, b) => a && b)"
      case _: OrMonoid => q"((a, b) => a || b)"
      case _: SumMonoid => q"((a, b) => a + b)"
      case _: MultiplyMonoid => q"((a, b) => a * b)"
      //      case _: MaxMonoid => q"((a, b) => if (a > b) a else b)"
      case _: MaxMonoid | _: MinMonoid => throw new UnsupportedOperationException(s"$m should be not be computed with fold. Use native support for operation.")
    }

    def zeroExp(m: PrimitiveMonoid): Const = m match {
      case _: AndMonoid => BoolConst(true)
      case _: OrMonoid => BoolConst(false)
      case _: SumMonoid => IntConst("0")
      case _: MultiplyMonoid => IntConst("1")
      case _: MaxMonoid | _: MinMonoid => throw new UnsupportedOperationException(s"$m has no zero")
    }

    /** Get identifier name
      */
    def idnName(idnNode: IdnNode) = {
      // TODO: Improve generation of identifier names to never conflict with user-defined names
      val idn = idnNode.idn
      if (idn.startsWith("$"))
        s"___arg${idn.drop(1)}"
      else
        idn
    }

    /** Build code for scanner nodes
      */
    def buildScan(e: IdnExp) = e match {

      // Scanner for Scala data
      case IdnExp(IdnUse(idn)) if !analyzer.spark(e) =>
        val ident: c.universe.Ident = Ident(TermName(idn))
        q"""openScanner($ident)"""

      // Scanner for Spark data
      case IdnExp(IdnUse(idn)) =>
        Ident(TermName(idn))

    }

    /** Build code for Scala algebra nodes
      */
    def buildScala(a: AlgebraNode) = a match {

      /** Scala Filter
        */
      case Filter(Gen(pat, child), pred) =>
        q"""${build(child)}.filter(${lambda1(pat, pred)})"""

      /** Scala Unnest
        */
      case Unnest(Gen(patChild, child), Gen(patPath, path), pred) =>
        val childArg = c.parse(s"child: ${patternType(patChild)}")
        val pathArg = c.parse(s"path: ${patternType(patPath)}")
        q"""
        val start = "************ Unnest (Scala) ************"
        val res =
          ${build(child)}
            .flatMap($childArg =>
              child match { case (..${patternTerms(patChild)}) =>
                ${build(path)}
                  .map($pathArg =>
                    path match { case (..${patternTerms(patPath)}) =>
                      ( (..${patternIdents(patChild)}), (..${patternIdents(patPath)}) ) }) })
            .filter(${lambda2(patChild, patPath, pred)})
        val end = "************ Unnest (Scala) ************"
        res
        """

      /** Scala OuterUnnest
        */
      case OuterUnnest(Gen(patChild, child), Gen(patPath, path), pred) =>
        val childArg = c.parse(s"child: ${patternType(patChild)}")
        val pathArg = c.parse(s"path: ${patternType(patPath)}")
        q"""
        val start = "************ OuterUnnest (Scala) ************"
        val res =
          ${build(child)}
            .flatMap($childArg =>
              child match { case (..${patternTerms(patChild)}) =>
                val matches =
                  ${build(path)}
                  .map($pathArg =>
                    path match { case (..${patternTerms(patPath)}) =>
                      ( (..${patternIdents(patChild)}), (..${patternIdents(patPath)}) ) })
                  .filter(${lambda2(patChild, patPath, pred)})
                if (matches.isEmpty)
                  None
                else
                  matches.map(Some(_)) })
        val end = "************ OuterUnnest (Scala) ************"
        res
        """

      /** Scala Join
        */
      case Join(Gen(leftPat, leftChild), Gen(rightPat, rightChild), p) =>
        q"""
        val start = "************ Join (Scala) ************"
        val left = ${build(leftChild)}
        val right = ${build(rightChild)}.toSeq
        val res = left.flatMap(x1 => right.map(x2 => (x1, x2))).filter(${lambda2(leftPat, rightPat, p)})
        val end = "************ Join (Scala)************"
        res
        """

      /** Scala OuterJoin
        */
      case OuterJoin(Gen(leftPat, leftChild), Gen(rightPat, rightChild), p) =>
        val leftArg = c.parse(s"l: ${patternType(leftPat)}")
        val rightArg = c.parse(s"r: ${patternType(rightPat)}")
        val bothArg = c.parse(s"arg: (${patternType(leftPat)}, ${patternType(rightPat)})")
        q"""
        val start = "************ OuterJoin (Scala) ************"
        val lhs = ${build(leftChild)}
        val rhs = ${build(rightChild)}.toSeq
        val res = lhs.flatMap($leftArg => {
          val ok = rhs.map($rightArg => (l, r)).filter(${lambda2(leftPat, rightPat, p)})
          if (ok.isEmpty)
            List((l, None))
          else
            ok.map($bothArg => arg match { case (l, r) => (l, Some(r)) })
        })
        val end = "************ OuterJoin (Scala)************"
        res
        """

      /** Scala Reduce
        */
      case Reduce(m, Gen(pat, child), e) =>
        val childCode = q"""${build(child)}.map(${lambda1(pat, e)})"""
        val code = m match {
          case _: MaxMonoid => q"""$childCode.max"""
          case _: MinMonoid => q"""$childCode.min"""
          case m1: PrimitiveMonoid => q"""$childCode.foldLeft(${zero(m1)})(${fold(m1)})"""
          case _: BagMonoid => q"""$childCode.toList.toIterable"""
          case _: ListMonoid => q"""$childCode.toList.toIterable"""
          case _: SetMonoid => q"""$childCode.toSet.toIterable"""
        }
        q"""
        val start = "************ Reduce (Scala) ************"
        val res = $code
        val end = "************ Reduce (Scala) ************"
        res
        """

      /** Scala Nest
        */
      case Nest(m: PrimitiveMonoid, Gen(pat, child), k, p, e) =>
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world)}, ${buildScalaType(analyzer.tipe(child), world)})")
        val nullables = patternNullable(pat)
        val nullableFilter =
          if (nullables.isEmpty)
            q"true"
          else {
            nullables.tail.fold(q"${nullables.head}.isDefined")((a, b) => q"$a.isDefined && $b.isDefined")
          }
        val code = q"""
        ${build(child)}
          .groupBy($childArg => child match { case (..${patternTerms(pat)}) => ${build(k)} })
          .map($groupedArg =>
              ( arg._1,
                arg._2
                  .filter($childArg => child match { case (..${patternTerms(pat)}) => $nullableFilter })
                  .filter($childArg => {
                    ..${denulledIdns("child", pat)}
                    ${build(p)} })
                  .map($childArg => {
                    ..${denulledIdns("child", pat)}
                    ${build(e)} })
                  .fold(${zero(m)})(${fold(m)}) ))
          .toIterable
        """
        q"""
        val start = "************ Nest Primitive Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest Primitive Monoid (Scala) ************"
        res"""

// TODO: test case infrastructure generating the expected XML file when the result tag is present
// TODO: Fix Nest handling of user yype... is this a general problem?
//       e.g. when should there be user types, and when not?

      case Nest(m: SetMonoid, Gen(pat, child), k, p, e) =>
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world)}, ${buildScalaType(analyzer.tipe(child), world)})")
        val nullables = patternNullable(pat)
        val nullableFilter =
          if (nullables.isEmpty)
            q"true"
          else {
            nullables.tail.fold(q"${nullables.head}.isDefined")((a, b) => q"$a.isDefined && $b.isDefined")
          }
        val code = q"""
        ${build(child)}
          .groupBy($childArg => child match { case (..${patternTerms(pat)}) => ${build(k)} })
          .map($groupedArg =>
              ( arg._1,
                arg._2
                  .filter($childArg => child match { case (..${patternTerms(pat)}) => $nullableFilter })
                  .filter($childArg => {
                    ..${denulledIdns("child", pat)}
                    ${build(p)} })
                  .map($childArg => {
                    ..${denulledIdns("child", pat)}
                    ${build(e)} })
                  .toSet
                  .toIterable ))
          .toIterable
        """
        q"""
        val start = "************ Nest Set Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest Set Monoid (Scala) ************"
        res"""

      case Nest((_: BagMonoid | _: ListMonoid), Gen(pat, child), k, p, e) =>
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world)}, ${buildScalaType(analyzer.tipe(child), world)})")
        val nullables = patternNullable(pat)
        val nullableFilter =
          if (nullables.isEmpty)
            q"true"
          else {
            nullables.tail.fold(q"${nullables.head}.isDefined")((a, b) => q"$a.isDefined && $b.isDefined")
          }
        val code = q"""
        ${build(child)}
          .groupBy($childArg => child match { case (..${patternTerms(pat)}) => ${build(k)} })
          .map($groupedArg =>
              ( arg._1,
                arg._2
                  .filter($childArg => child match { case (..${patternTerms(pat)}) => $nullableFilter })
                  .filter($childArg => {
                    ..${denulledIdns("child", pat)}
                    ${build(p)} })
                  .map($childArg => {
                    ..${denulledIdns("child", pat)}
                    ${build(e)} })
                  .toList
                  .toIterable ))
          .toIterable
        """
        q"""
        val start = "************ Nest Bag/List Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest Bag/List Monoid (Scala) ************"
        res"""
    }

    /** Build code for Spark algebra nodes
      */
    def buildSpark(a: AlgebraNode) = ??? //a match {
    //      //            q"""${accessPaths(name)}"""
    //      /*
    //      * Fegaras: Ch 6. Basic Algebra O1
    //      * Join(X, Y, p), joins the collections X and Y using the join predicate p.
    //      * This join is not necessarily between two sets; if, for example, X is a list
    //      * and Y is a bag, then, according to Eq. (D4), the output is a bag.
    //      */
    //      case r@SparkJoin(logicalNode, p, left, right) =>
    //        val leftCode = build(left)
    //        val rightCode = build(right)
    //        q"""
    //     val rddLeft = $leftCode
    //     val rddRight = $rightCode
    //     val res = rddLeft.cartesian(rddRight).filter(${exp(p)})
    //     res
    //   """
    //
    //      /* Fegaras: Ch 6. Basic Algebra O2
    //      * Select(X, p), selects all elements of X that satisfy the predicate p.
    //      */
    //      case SparkSelect(logicalNode, p, child: PhysicalAlgebraNode) =>
    //        val childTypeName = nodeScalaType(child.logicalNode)
    //        val pCode = exp(p, Some(childTypeName))
    //        logger.info(s"[SELECT] p: $pCode")
    //        q"""${build(child)}.filter($pCode)"""
    //
    //      /*
    //      * Fegaras: Ch 6. Basic Algebra O3
    //      * unnest(X, path, p), returns the collection of all pairs (x, y) for each x \in X and for each y \in x.path
    //      * that satisfy the predicate p(x, y).
    //      */
    //      case SparkUnnest(logicalNode, path, pred, child) =>
    //        val childTypeName = nodeScalaType(child.logicalNode)
    //        val pathCode = exp(path, Some(childTypeName))
    //        val pathType = expScalaType(path)
    //        //        val pathType = exp(path)
    //        val predCode = exp(pred)
    //        logger.info(s"[UNNEST] path: $pathCode, pred: $predCode")
    //        val childCode = build(child)
    //        //            logger.debug("{}.path = {} \n{}", x, pathValues)
    //        val code = q"""
    //          $childCode.flatMap(x => {
    //            val pathValues = $pathCode(x)
    //            pathValues.map((y:$pathType) => (x,y)).filter($predCode)
    //            }
    //          )
    //        """
    //
    //        q"""
    //        val start = "************ SparkUnnest ************"
    //        val res = $code
    //        val end= "************ SparkUnnest ************"
    //        res
    //        """
    //      /* Fegaras: Ch 6. Basic Algebra O4
    //      Reduce(X, Q, e, p), collects the values e(x) for all x \in X that satisfy p(x) using the accumulator Q.
    //      The reduce operator can be thought of as a generalized version of the relational projection operator.
    //      */
    //      case SparkReduce(logicalNode, m, e, p, child) =>
    //        val pCode = exp(p)
    //        val eCode = exp(e)
    //        val childCode = build(child)
    //        logger.info(s"[SparkReduce] $m, e: $eCode, p: $pCode")
    //        val filterMapCode = q"""
    //     val childRDD = $childCode
    //     childRDD
    //       .filter($pCode)
    //       .map($eCode)
    //     """
    //        val code = m match {
    //          case m1: MaxMonoid => q"""$filterMapCode.max()"""
    //          case m1: MinMonoid => q"""$filterMapCode.min()"""
    //          case m1: PrimitiveMonoid =>
    //            /* TODO: Use Spark implementations of monoids if supported. For instance, RDD has max and min actions.
    //             * Compare the Spark implementations versus a generic code generator based on fold
    //             */
    //            q"""$filterMapCode.fold(${zero(m1)})(${fold(m1)})"""
    //
    //          case _: ListMonoid => q"""$filterMapCode"""
    //          case _: BagMonoid => q"""$filterMapCode"""
    //          case _: SetMonoid => q"""$filterMapCode.distinct"""
    //        }
    //        q"""
    //        val start = "************ SparkReduce ************"
    //        val res = $code
    //        val end= "************ SparkReduce ************"
    //        res
    //        """
    //
    //      case SparkNest(logicalNode, m, e, f, p, g, child) =>
    //        val eCode = exp(e)
    //        val fCode = exp(f)
    //        val pCode = exp(p)
    //        logger.info(s"[SparkNEST] m: $m, e: $eCode, f: $fCode, p: $pCode, g: ${exp(g)}")
    //        val t = analyzer.expressionType(e)
    //        val st: String = buildScalaType(t, world)
    //        val childTree: Tree = build(child)
    //
    //        //        val tp = c.parse(st)
    //
    //        def computeG(l: Seq[Exp]): Exp = l match {
    //          case Nil => BoolConst(false)
    //          case h :: t => MergeMonoid(OrMonoid(), BinaryExp(Eq(), h, Null), computeG(t))
    //        }
    //
    //        val gExp = g match {
    //          case RecordCons(attrs) => computeG(attrs.map { a: AttrCons => a.e })
    //          case _ => {
    //            assert(false);
    //            BoolConst(true)
    //          }
    //        }
    //
    //        val code = m match {
    //          case m1: MinMonoid =>
    //            q"""
    //            val grouped = $childTree.groupBy($fCode)
    //            grouped.mapValues(v => v
    //                .filter($pCode)
    //                .map($eCode)
    //                .min)
    //            """
    //          case m1: MaxMonoid =>
    //            q"""
    //            val grouped = $childTree.groupBy($fCode)
    //            grouped.mapValues(v => v
    //                .filter($pCode)
    //                .map($eCode)
    //                .max)
    //            """
    //
    //          case m1: PrimitiveMonoid =>
    //            val z1 = zeroExp(m1)
    //            val f1 = IfThenElse(gExp, z1, e)
    //            //  logger.debug("groupBy:\n{}", toString(grouped))
    //            //  logger.debug("filteredP:\n{}", toString(filteredP))
    //            //  logger.debug("mapped:\n{}", toString(mapped))
    //            //  logger.debug("folded:\n{}", toString(folded))
    //            q"""
    //            val grouped = $childTree.groupBy($fCode)
    //            grouped.mapValues(v => v
    //                .filter($pCode)
    //                .map(${exp(f1)})
    //                .fold(${zero(m1)})(${fold(m1)}))
    //            """
    //          case m1: CollectionMonoid =>
    //            // TODO: Does not handle dotequality.
    //            /* TODO: The filter by p probably can be applied at the start of this operator implementation,
    //             * in a common code path before branching to handle the different monoids.
    //             */
    //            // Since the zero element of a collection monoid is an empty collection,
    //            // instead of using fold we can just filter any elements such that g(w) == null
    //            val filterGNulls = IfThenElse(gExp, BoolConst(false), BoolConst(true))
    //            val monoidReduceCode = m1 match {
    //              case m2: SetMonoid =>
    //                q"""mappedByE.toSet"""
    //              //              case m1: BagMonoid =>
    //              //                q"""com.google.common.collect.ImmutableMultiset.copyOf(scala.collection.JavaConversions.asJavaIterable(mappedByE))"""
    //              case m1: BagMonoid =>
    //                // TODO: Lists are semantically equivalent to Bags but potentially less efficient.
    //                q"""mappedByE.toList"""
    //              //              case m1: BagMonoid =>
    //              //                q"""
    //              //                def bagBuilderIter[T:scala.reflect.ClassTag](iter:Iterable[T]): scala.collection.immutable.Bag[T] = {
    //              //                  import scala.collection.immutable.Bag
    //              //                  Bag.newBuilder(Bag.configuration.compact[T])
    //              //                    .++=(iter)
    //              //                    .result()
    //              //                }
    //              //                bagBuilderIter(mappedByE)
    //              //                """
    //              case m1: ListMonoid =>
    //                q"""mappedByE.toList"""
    //            }
    //            val commonCode =
    //            //            logger.debug("groupBy:\n{}", toString(grouped))
    //            //            logger.debug("filteredP:\n{}", toString(filteredP))
    //            //            logger.debug("filteredG:\n{}", toString(filteredG))
    //            //            logger.debug("mapped:\n{}", toString(mapped))
    //              q"""
    //            val grouped = $childTree.groupBy($fCode)
    //            grouped.mapValues(v => {
    //                val filteredByP = v.filter($pCode)
    //                val filteredByG = filteredByP.filter(${exp(filterGNulls)})
    //                val mappedByE = filteredByG.map($eCode)
    //                ${monoidReduceCode}
    //            })
    //            """
    //            commonCode
    //        }
    //
    //        q"""
    //          val start = "************ SparkNest ************"
    //          val sparkNest = $code
    //          val end = "************ SparkNest ************"
    //          sparkNest"""
    //
    //      case SparkMerge(logicalNode, m, left, right) => ???
    //      /*
    //      outer-join, X OJ(p) Y, is a left outer-join between X and Y using the join
    //      predicate p. The domain of the second generator (the generator of w) in
    //      Eq. (O5) is always nonempty. If Y is empty or there are no elements that
    //      can be joined with v (this condition is tested by universal quantification),
    //      then the domain is the singleton value [NULL], i.e., w becomes null.
    //      Otherwise each qualified element w of Y is joined with v.
    //
    //      Delegates to PairRDDFunctions#leftOuterJoin. We cannot use this method directly, because
    //      it takes RDDs in the following form: (k, v), (k, w) => (k, (v, Option(w)), using k to make the
    //      matching. While we have (p, left, right) and want  (v, w) with p used to match the elements.
    //      The code bellow does the following transformations:
    //      1. Compute RDD(v, w) such that v!=null and p(v, w) is true.
    //      2. Apply PairRDDFunctions#leftOuterJoin.
    //      RDD(v, w).leftOuterJoin( RDD(v, v) ) => (v, (v, Option[w]))
    //      3. Transform in the output format of this operator.
    //      (v, (v, Some[w])) -> (v, w)
    //      (v, (v, None)) -> (v, null)
    //      */
    //
    //      // TODO: Using null below can be problematic with RDDs of value types (AnyVal)?
    //      case SparkOuterJoin(logicalNode, p, left, right) =>
    //        val leftNodeType = nodeScalaType(left.logicalNode)
    //        val rightNodeType = nodeScalaType(right.logicalNode)
    //        val expP: c.universe.Tree = exp(p)
    //        logger.info(s"[SparkOuterJoin] exp(p) = ${showCode(expP)}")
    //
    //        val code = q"""
    //            val start = "************ SparkOuterJoin ************"
    //            val leftRDD:RDD[$leftNodeType] = ${build(left)}
    //            queryLogger.debug("leftRDD:\n"+ raw.QueryHelpers.toString(leftRDD))
    //
    //            val rightRDD:RDD[$rightNodeType] = ${build(right)}
    //            queryLogger.debug("rightRDD:\n"+ raw.QueryHelpers.toString(rightRDD))
    //
    //            val matching:RDD[($leftNodeType, $rightNodeType)] = leftRDD
    //              .cartesian(rightRDD)
    //              .filter(tuple => tuple._1 != null)
    //              .filter($expP)
    //            queryLogger.debug("matching:\n"+ raw.QueryHelpers.toString(matching))
    //
    //            val resWithOption: RDD[($leftNodeType, ($leftNodeType, Option[$rightNodeType]))] = leftRDD
    //              .map(v => (v, v))
    //              .leftOuterJoin(matching)
    //            queryLogger.debug("resWithOption:\n"+ raw.QueryHelpers.toString(resWithOption))
    //
    //            val res:RDD[($leftNodeType, $rightNodeType)] = resWithOption.map( {
    //              case (v1, (v2, None)) => (v1, null)
    //              case (v1, (v2, Some(w))) => (v1, w)
    //            })
    //            queryLogger.debug("res:\n"+ raw.QueryHelpers.toString(res))
    //            val end = "************ SparkOuterJoin ************"
    //            res
    //            """
    //        code
    //      case SparkOuterUnnest(logicalNode, path, pred, child) => ???
    //      /*
    //      The assign node contains one more more assignments. Each is transformed into
    //      a statement in the form: val key = `build(code)`
    //       */
    //      case SparkAssign(logicalNode, assigns, child) => {
    //        var block: c.universe.Tree = q""
    //        assigns.foreach({ case (key, node) => {
    //          val rddName = TermName(key)
    //          val rddCode = build(node)
    //          // Appends the additional statements to the block. Without the "..$", this would generate independent blocks:
    //          // "{val rdd1 =...; rdd1.cache()}; {val rdd2 =...; rdd2.cache()}" , and the RDDs defined in one block would
    //          // not be visible outside. The "..$" creates a single block with the concatenation of all the expressions.
    //          block = q"..$block; val $rddName = $rddCode; $rddName.cache()"
    //        }
    //        })
    //        q"""..$block; ${build(child)}"""
    //      }
    //    }

    def build(e: Exp): Tree = e match {
      case a: AlgebraNode if !analyzer.spark(a) => buildScala(a)
      case a: AlgebraNode => buildSpark(a)
      case i: IdnExp if analyzer.isSource(i) => buildScan(i)
      case _ => exp(e)
    }

    logger.info("Building code. User types: " + world.tipes.mkString("\n"))
    val tree = build(treeExp)

    val treeType = analyzer.tipe(treeExp)
    logger.info(s"TreeType: $treeType")
    val reducedTree = tree match {
      case a: AlgebraNode if analyzer.spark(a) => reduceSpark(tree, treeType)
      case a: AlgebraNode => reduceScala(tree, treeType)
      case _ => tree
    }

    reducedTree
  }

  def reduceScala(tree: Tree, treeType: raw.Type): Tree = {
    treeType match {
      case CollectionType(BagMonoid(), _) => tree
      case CollectionType(ListMonoid(), _) => tree
      case n@CollectionType(SetMonoid(), _) => tree
      case _ => tree
    }
  }

  def reduceSpark(tree: Tree, treeType: raw.Type): Tree = {
    treeType match {
      /*
       Currently, we do not take advantage of the Bag semantics to optimize an RDD representing a BagType. So an RDD
       representing a Bag containes multiple copies of equivalent elements. A possible optimization is to represent
       a Bag as a PairRDD[Elem, Count], but this would require special handling to deal with the multiple representations
        */
      case CollectionType(BagMonoid(), _) =>
        q"""$tree.collect"""
      //        q"""
      //        val m = $tree.countByValue()
      //        raw.QueryHelpers.bagBuilder(m)
      //        """

      /*
       * - toLocalIterator() retrieves one partition at a time by the driver, which requires less memory than
       * collect(), which first gets all results.
       *
       * - toSet is a Scala local operation.
       */
      case CollectionType(ListMonoid(), _) =>
        q"""$tree
            .toLocalIterator
            .to[scala.collection.immutable.List]"""

      case n@CollectionType(SetMonoid(), _) =>
        logger.info(s"Node: $n")
        q"""$tree.collect"""
      //        q"""$tree
      //                .collect
      //                .to[scala.collection.immutable.Set]"""

      case _ => tree
    }
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

  def extractQueryAndAccessPath(makro: Tree): (String, QueryLanguage, List[AccessPath]) = {
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
        var queryLanguage: Option[QueryLanguage] = None
        val accessPaths = new ArrayBuffer[AccessPath]()
        body.foreach(v => {
          logger.debug("Checking element: " + showCode(v))
          //          logger.debug("Tree: " + showRaw(v))
          v match {
            case ValDef(_, TermName("qrawl "), _, Literal(Constant(queryString: String))) =>
              logger.info("Found qrawl query: " + queryString)
              if (query.isDefined) {
                bail(s"Multiple queries found. Previous: ${query.get}, Current: $queryString")
              }
              query = Some(queryString)
              queryLanguage = Some(Qrawl)

            case ValDef(_, TermName("oql "), _, Literal(Constant(queryString: String))) =>
              val trimmedString = queryString.trim
              logger.info("Found oql query: " + trimmedString)
              if (query.isDefined) {
                bail(s"Multiple queries found. Previous: ${query.get}, Current: $trimmedString")
              }
              query = Some(trimmedString)
              queryLanguage = Some(OQL)

            case ValDef(_, TermName("plan "), _, Literal(Constant(queryString: String))) =>
              val trimmedString = queryString.trim
              logger.info("Found logical plan: " + trimmedString)
              if (query.isDefined) {
                bail(s"Multiple queries found. Previous: ${query.get}, Current: $trimmedString")
              }
              query = Some(trimmedString)
              queryLanguage = Some(LogicalPlan)

            case ValDef(_, TermName(termName), typeTree: c.universe.Tree, _) =>
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

  def generateCode(makro: Tree, query: String, queryLanguage: QueryLanguage, accessPaths: List[AccessPath]): c.Expr[Any] = {
    val sources = accessPaths.map(ap => (ap.name, ap.rawType)).toMap
    val world = new World(sources)

    loggerQueries.info(s"Query ($queryLanguage):\n$query")
    // Parse the query, using the catalog generated from what the user gave.
    val parseResult: Either[QueryError, Calculus.Calculus] = queryLanguage match {
      case OQL => ???
      case Qrawl => Query(query, world)
      case LogicalPlan => ???
    }

    parseResult match {
      case Right(tree) =>
        val treeExp = tree.root

        var expStr = CalculusPrettyPrinter(treeExp)
        loggerQueries.info(s"Logical tree:\n$expStr")

        val isSpark: Map[String, Boolean] = accessPaths.map(ap => (ap.name, ap.isSpark)).toMap

        val physicalAnalyzer = new PhysicalAnalyzer(tree, world, isSpark)
        physicalAnalyzer.tipe(tree.root)  // Type the root of the tree to force all nodes to be typed

        val caseClasses: Set[Tree] = buildCaseClasses(treeExp, world, physicalAnalyzer)
        logger.info("case classes:\n{}", caseClasses.map(showCode(_)).mkString("\n"))

        val generatedTree: Tree = buildCode(treeExp, world, physicalAnalyzer)
        //        logger.info("Query execution code:\n{}", showCode(generatedTree))
        loggerQueries.info(s"Scala code:\n${showCode(generatedTree)}")

        /* Generate the actual scala code. Need to extract the class parameters and the body from the annotated
        class. The body and the class parameters will be copied unmodified in the resulting code.
        */
        val q"class $className(..$paramsAsAny) extends ..$baseClass { ..$body }" = makro


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
  object $moduleName {
    ..$caseClasses
  }"""

        val clazz = q"""
  class $className(..$methodDefParameters) extends RawQuery {
    val queryLogger = org.slf4j.LoggerFactory.getLogger("raw.queries")
    ..$body

    import ${moduleName}._
    private[this] def executeQuery(..$methodDefParameters) = {
      $generatedTree
    }

    def computeResult = {
       try {
          executeQuery(..$methodCallParameters)
       } finally {
         closeAllIterators()
       }
    }
  }
"""
        val block = Block(List(companion, clazz), Literal(Constant(())))

        val scalaCode = showCode(block)
        logger.info("Generated code:\n{}", scalaCode)
        QueryLogger.log(query, expStr, scalaCode)
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

    val (query: String, language: QueryLanguage, accessPaths: List[AccessPath]) = extractQueryAndAccessPath(makro)
    logger.info("Access paths: {}", accessPaths.map(ap => ap.name + ", isSpark: " + ap.isSpark).mkString("; "))

    generateCode(makro, query, language, accessPaths)
  }
}
