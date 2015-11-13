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
import scala.reflect.macros.runtime.AbortMacroException


// TODO: Make sure we are using the immutable Seq/List, etc

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
    new RawIterable(scanner, openIters).view
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

object QueryLanguages {
  def apply(qlString: String): QueryLanguage = {
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
    val name: String
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


object RawImpl {
  /* Used to pass error information between the macro expansion code executed by the runtime compiler and the
   code which is invoking the compiler (eg., the rest server handler). This is a workaround at the limitation that
   the compiler framework only allows passing errors as a list of strings collected in a Reporter, there is no
   way of passing a structured object (AFAIK). To avoid string parsing, we stored in this thread local object the
   error. This is safe since the compiler is single threaded and is run on the same thread as the one running the
   RawCompiler instance which is invoking the compiler.
    */
  val queryError = new ThreadLocal[Option[AnyRef]] {
    override def initialValue(): Option[AnyRef] = None
  }
}

class RawImpl(val c: scala.reflect.macros.whitebox.Context) extends StrictLogging {
  val loggerQueries = LoggerFactory.getLogger("raw.queries")

  import QueryLanguages._

  import c.universe._

  /** Map the canonical form of a record type to a user case class name.
    */
  val classesMap = scala.collection.mutable.HashMap[String, String]()

  /** Bail out during compilation with error message.
    * Used for internal errors.
    */
  def bail(message: String) = {
    RawImpl.queryError.set(Some(InternalError(message)))
    c.abort(c.enclosingPosition, message)
  }

  /** Bail out with a query error.
    */
  def bail(queryError: QueryError) = {
    RawImpl.queryError.set(Some(queryError))
    c.abort(c.enclosingPosition, "Compilation failed.")
  }

  /** Bail out with an exception.
    */
  def bail(throwable: Throwable) = {
    RawImpl.queryError.set(Some(throwable))
    c.abort(c.enclosingPosition, "Compilation failed.")
  }

  case class InferredType(rawType: raw.Type, isSpark: Boolean)

  /** Infer RAW's type from the Scala type.
    * Enforces that the Scala class names used to represent a record type are unique.
    */
  // TODO: isSpark in the midst of type inference sounds hacky. Move it to a separate phase/method, also to make sure
  //       that info propagates properly across e.g. FunTypes
  def inferType(t: c.Type): InferredType = {
    def isAny(sym: String) =
      sym == "scala.Any"

    def isInt(sym: String) =
      sym == "scala.Int"

    def isString(sym: String) =
      sym == "scala.Predef.String" || sym == "java.lang.String"

    def isSet(sym: String) =
      sym == "scala.Predef.Set"

    def isList(sym: String) =
      sym == "scala.Iterable" || sym == "scala.Seq" || sym == "scala.List" || sym == "scala.collection.immutable.List"

    def isBag(sym: String) =
      sym == "raw.executor.RawScanner" || sym == "org.apache.spark.rdd.RDD"

    def addToClassesMap(r: RecordType, name: String) = {
      val canonicalForm = toCanonicalForm(r)
      if (classesMap.contains(canonicalForm) && classesMap(canonicalForm) != name) {
        val oldName = classesMap(canonicalForm)
        bail(s"Non-unique name for a record type: processing record with name '$name' when the same record type is already defined with name '${classesMap(canonicalForm)}'. Consider using 'type $name = $oldName' instead?")
      } else {
        classesMap.put(canonicalForm, name)
      }
    }

    val rawType = t match {
      case TypeRef(_, sym, Nil) if isInt(sym.fullName) =>
        raw.IntType()
      case TypeRef(_, sym, Nil) if isAny(sym.fullName) =>
        raw.TypeVariable()
      case TypeRef(_, sym, Nil) if isString(sym.fullName) =>
        raw.StringType()
      case TypeRef(_, sym, t1 :: Nil) if isSet(sym.fullName) =>
        raw.CollectionType(SetMonoid(), inferType(t1).rawType)
      case TypeRef(_, sym, t1 :: Nil) if isList(sym.fullName) =>
        raw.CollectionType(ListMonoid(), inferType(t1).rawType)
      case TypeRef(_, sym, t1 :: Nil) if isBag(sym.fullName) =>
        raw.CollectionType(BagMonoid(), inferType(t1).rawType)
      case TypeRef(_, sym, ts) if sym.fullName.startsWith("scala.Tuple") =>
        val regex = """scala\.Tuple(\d+)""".r
        sym.fullName match {
          case regex(n) =>
            val r = raw.RecordType(Attributes(List.tabulate(n.toInt) { case i => raw.AttrType(s"_${i + 1}", inferType(ts(i)).rawType) }))
            addToClassesMap(r, sym.fullName)
            r
        }
      case t @ TypeRef(_, sym, Nil) =>
        val ctor = t.decl(termNames.CONSTRUCTOR).asMethod
        val r = raw.RecordType(Attributes(ctor.paramLists.head.map { case sym1 => raw.AttrType(sym1.name.toString, inferType(sym1.typeSignature).rawType) }))
        addToClassesMap(r, sym.fullName)
        r
      case TypeRef(_, sym, t1 :: t2 :: Nil) if sym.fullName.startsWith("scala.Function") =>
        val regex = """scala\.Function(\d+)""".r
        sym.fullName match {
          case regex(n) => raw.FunType(Seq(inferType(t1).rawType), inferType(t2).rawType)
        }
      case TypeRef(pre, sym, args) =>
        bail(s"Unsupported TypeRef($pre, $sym, $args)")
    }

    val isSpark = t.typeSymbol.fullName.equals("org.apache.spark.rdd.RDD")
    InferredType(rawType, isSpark)
  }

  /** Return a unique representation for a record type, excluding its nullable information.
    * (Nullable info is excluded since the Scala Option[...] is "outside" the record.
    */
  def toCanonicalForm(recordType: RecordType): String =
    PrettyPrinter(recordType.recAtts)

  /** Return a user record for the given record type, otherwise use a Scala Tuple.
    */
  def tupleSym(r: RecordType): String = {
    val n = r.recAtts.atts.size
    classesMap.getOrElse(toCanonicalForm(r), s"Tuple$n")
  }

  def buildScalaType(t: raw.Type, world: World, analyzer: SemanticAnalyzer): String = t match {
    case _: BoolType => "scala.Boolean"
    case _: StringType => "String"
    case _: IntType => "scala.Int"
    case _: FloatType => "scala.Float"
    case OptionType(t1) => s"Option[${buildScalaType(t1, world, analyzer)}"
    case FunType(t1, t2) =>
      val args = t1.map{arg => s"${buildScalaType(arg, world, analyzer)}"}
      s"${args.mkString("(", ",", ")")} => ${buildScalaType(t2, world, analyzer)}"
    case r @ RecordType(Attributes(atts)) =>
      // Return the case class name if it exists; otherwise, it is a Tuple
      classesMap.getOrElse(
        toCanonicalForm(r),
        atts
          .map(att => s"(${buildScalaType(att.tipe, world, analyzer)})")
          .mkString("(", ", ", ")"))
    case CollectionType(_: CollectionMonoid, innerType) => s"scala.collection.Iterable[${buildScalaType(innerType, world, analyzer)}]"
    //      case CollectionType(_: BagMonoid, innerType) => s"List[${buildScalaType(innerType, world, analyzer)}]"
    //      case CollectionType(_: ListMonoid, innerType) => s"List[${buildScalaType(innerType, world, analyzer)}]"
    //      case CollectionType(_: SetMonoid, innerType) => s"Set[${buildScalaType(innerType, world, analyzer)}]"
    //      case CollectionType(m: MonoidVariable, innerType) =>
    //        analyzer.looksLikeMonoid(m) match {
    //          case _: SetMonoid => s"Set[${buildScalaType(innerType, world, analyzer)}]"
    //          case _ => s"List[${buildScalaType(innerType, world, analyzer)}]"
    //        }
    case UserType(idn) => buildScalaType(world.tipes(idn), world, analyzer)
    case _: AnyType => "Any"
    case _: NothingType => "Nothing"
    case _: VariableType => throw new UnsupportedOperationException(s"type variables not supported")
  }

  /** Collect all record types in the tree.
    * The record types are converted to their Scala string descriptions for two reasons:
    * 1) they become naturally unique when we turn it into a set
    * 2) the nullables are present in the type explicitely, not as an implicit attribute.
    */
  def collectRecordTypes(tree: Calculus.Exp, world: World, analyzer: SemanticAnalyzer): Seq[RecordType] = {
    import org.kiama.rewriting.Rewriter._

    val recordTypes = scala.collection.mutable.ListBuffer[RecordType]()

    val queryRecordTypes = everywhere(query[Calculus.Exp] {
      case e => analyzer.tipe(e) match {
        case r: RecordType => recordTypes += r
        case t             =>
      }
    })

    queryRecordTypes(tree)

    recordTypes.to
  }

  /** Build case classes for the anonymous record types used to hold the results of Reduce nodes.
    */
  def buildCaseClasses(tree: Calculus.Exp, world: World, analyzer: SemanticAnalyzer): Set[Tree] = {

    val recordTypes: Seq[RecordType] = collectRecordTypes(tree, world, analyzer)

    var i = 0

    // First, add all classes to the classesMap.
    // (We only build the code later since some classes may use other classes, and those dependencies need to be in the
    // map already so we know their generated names.)

    val toBuild = scala.collection.mutable.HashMap[String, RecordType]()
    for (r <- recordTypes) {
      val canonicalForm = toCanonicalForm(r)
      if (!classesMap.contains(canonicalForm)) {
        // Generate new case class name
        i += 1
        val name = s"UserRecord$i"

        // Add it to the map
        classesMap.put(canonicalForm, name)
        logger.debug(s"Mapping type $canonicalForm to case class $name")

        toBuild.put(name, r)
      }
    }

    // Generate code.

    val code = scala.collection.mutable.Set[String]()
    for ((name, r) <- toBuild) {
      val args = r.recAtts.atts.map(att => s"${att.idn}: ${buildScalaType(att.tipe, world, analyzer)}").mkString(", ")
      logger.info(s"Build case class: $r.atts => $args")
      code += s"case class $name($args)"
    }

    code.map(c.parse).to
  }

  /** Build code-generated query plan from logical algebra tree.
    */
  def buildCode(treeExp: Calculus.Exp, world: World, analyzer: PhysicalAnalyzer): Tree = {
    import Calculus._

    def rawToScalaType(t: raw.Type): c.universe.Tree = {
      logger.info(s"rawToScalaType: $t")
      val typeName: String = buildScalaType(t, world, analyzer)
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
      case _: Null => q"null"
      case BoolConst(v) => q"$v"
      case IntConst(v) => q"${v.toInt}"
      case FloatConst(v) => q"${v.toFloat}"
      case StringConst(v) => q"$v"
      case IdnExp(idn) => Ident(TermName(idnName(idn)))
      case RecordProj(e1, idn) =>
        val id = TermName(idn)
        q"${build(e1)}.$id"
      case RecordCons(atts) =>
        val t = analyzer.tipe(e).asInstanceOf[RecordType]
        val className = classesMap.getOrElse(toCanonicalForm(t), "")
        val vals = atts
          .map(att => build(att.e))
          .mkString(",")
        c.parse( s"""$className($vals)""")
      case IfThenElse(e1, e2, e3) =>
        q"if (${build(e1)}) ${build(e2)} else ${build(e3)}"
      case BinaryExp(op, e1, e2) => op match {
        case _: Eq => q" ${build(e1)} == ${build(e2)}"
        case _: Neq => q" ${build(e1)} != ${build(e2)}"
        case _: Ge => q" ${build(e1)} >= ${build(e2)}"
        case _: Gt => q" ${build(e1)} > ${build(e2)}"
        case _: Le => q" ${build(e1)} <= ${build(e2)}"
        case _: Lt => q" ${build(e1)} < ${build(e2)}"
        case _: Plus => q"${build(e1)} + ${build(e2)}"
        case _: Sub => q" ${build(e1)} - ${build(e2)}"
        case _: Mult => q"${build(e1)} * ${build(e2)}"
        case _: Div => q" ${build(e1)} / ${build(e2)}"
        case _: Mod => q" ${build(e1)} % ${build(e2)}"
        case _: MaxOp => q"val e1 = ${build(e1)}; val e2 = ${build(e2)}; if (e1 > e2) e1 else e2"
        case _: MinOp => q"val e1 = ${build(e1)}; val e2 = ${build(e2)}; if (e1 < e2) e1 else e2"
        case _: And => q"${build(e1)} && ${build(e2)}"
        case _: Or => q"${build(e1)} || ${build(e2)}"
        case _: Union => q"${build(e1)} ++ ${build(e2)}"
        case _: BagUnion => q"${build(e1)} ++ ${build(e2)}"
        case _: Append => q"${build(e1)} ++ ${build(e2)}"
      }
      case FunApp(f, e) =>
        q"""${build(f)}(..${e.map(build)})"""
      case ZeroCollectionMonoid(m) => m match {
        case _: SetMonoid => q"Set().toIterable"
        case _: BagMonoid => q"List().toIterable"
        case _: ListMonoid => q"List().toIterable"
      }
      case ConsCollectionMonoid(m, e1) => m match {
        case _: SetMonoid => q"Set(${build(e1)}).toIterable"
        case _: BagMonoid => q"List(${build(e1)}).toIterable"
        case _: ListMonoid => q"List(${build(e1)}).toIterable"
      }
      case UnaryExp(op, e1) => op match {
        case _: Not => q"!${build(e1)}"
        case _: Neg => q"-${build(e1)}"
        case _: ToBool => q"${build(e1)}.toBoolean"
        case _: ToInt => q"${build(e1)}.toInt"
        case _: ToFloat => q"${build(e1)}.toFloat"
        case _: ToString => q"${build(e1)}.toString"
        case _: ToBag => q"${build(e1)}.toList.toIterable"
        case _: ToList => q"${build(e1)}.toList.toIterable"
        case _: ToSet => q"${build(e1)}.toSet.toIterable"
      }
      case ExpBlock(bs, e1) =>
        val vals = bs.map { case Bind(PatternIdn(idn), be) => q"val ${TermName(idnName(idn))} = ${
          {
            build(be)
          }
        }"
        }
        q"""
        {
          ..$vals
          ${build(e1)}
        }
        """
      // TODO: Implement remaining ParseAs strategies
      case a @ ParseAs(e1, r, _) =>
        val regex = analyzer.scalaRegex(r).get
        val hack = s"regex||||${r.value}||||"
        analyzer.regexType(r) match {
          case t @ RecordType(Attributes(atts)) =>
            val rt = q"${Ident(TermName(tupleSym(t)))}"
            val pnames = atts.zipWithIndex.map { case (att, idx) => pq"${TermName(s"arg$idx")}"}
            val names = atts.zipWithIndex.map { case (att, idx) => q"${Ident(TermName(s"arg$idx"))}"}
            q"""
            {
              val regex = $regex.r
              val input = ${build(e1)}
              input match {
                case regex(..$pnames) => $rt(..$names)
                case _ => throw new RuntimeException($hack + "incompatible input: " + input)
              }
            }
            """
          case _: StringType =>
            q"""
            {
              val regex = $regex.r
              val input = ${build(e1)}
              input match {
                case regex(m) => m
                case _ => throw new RuntimeException($hack + "incompatible input: " + input)
              }
            }
            """
        }
      case ToEpoch(e1, fmt) =>
        q"""
        val strDate = ${build(e1)}
        val date = new java.text.SimpleDateFormat($fmt).parse(strDate)
        date.toInstant.getEpochSecond.toInt
        """
    }

    def patternType(p: Pattern) =
      buildScalaType(analyzer.patternType1(p), world, analyzer)

    /** Get the nullable identifiers from a pattern.
      * Used by the Nest to filter out Option[...]
      */
    def patternNullable(p: Pattern): Seq[Tree] = {
      def recurse(p: Pattern, t: raw.Type): Seq[Option[Tree]] = p match {
        case PatternProd(ps) =>
          val t1 = t.asInstanceOf[RecordType]
          ps.zip(t1.recAtts.atts).flatMap { case (p1, att) => recurse(p1, att.tipe) }
        //        TODO only take the nullable idn
        case PatternIdn(idn: IdnDef) =>
          Seq(Some(q"${Ident(TermName(idnName(idn)))}"))
        case _ =>
          Seq(None)
      }

      recurse(p, analyzer.patternType1(p)).flatten
    }

    /** Return the identifiers, optionally de-nullable.
      * e.g. given a `child` with type record(name: string, age: option[int]) returns:
      * val name = child._1
      * val age = child._2.get
      * Used by the Nest to handle Option[...]
      */
    def idnVals(parent: String, p: Pattern): Seq[Tree] = {
      def projIdx(idxs: Seq[Int]): String =
        if (idxs.isEmpty)
          parent
        else
          s"$parent.${idxs.map { case idx => s"_${idx + 1}" }.mkString(".")}"

      def recurse(p: Pattern, t: raw.Type, idxs: Seq[Int]): Seq[Tree] = p match {
        case PatternProd(ps) =>
          val t1 = t.asInstanceOf[RecordType]
          ps.zip(t1.recAtts.atts).zipWithIndex.flatMap { case ((p1, att), idx) => recurse(p1, att.tipe, idxs :+ idx) }
        case PatternIdn(idn) =>
          Seq(q"val ${TermName(idnName(idn))} = ${c.parse(projIdx(idxs))}")
      }

      recurse(p, analyzer.patternType1(p), Seq())
    }

    /** Zero of a primitive monoid.
      */
    def zero(m: PrimitiveMonoid): Tree = m match {
      case _: AndMonoid => q"true"
      case _: OrMonoid => q"false"
      case _: SumMonoid => q"0"
      case _: MultiplyMonoid => q"1"
      case _: MaxMonoid => q"-2147483648"
      case _: MinMonoid => q"2147483647" // TODO
      //      case _: MaxMonoid | _: MinMonoid => throw new UnsupportedOperationException(s"$m has no zero")
    }

    /** Merge/Fold of two primitive monoids.
      */
    def fold(m: PrimitiveMonoid): Tree = m match {
      case _: AndMonoid => q"((a, b) => a && b)"
      case _: OrMonoid => q"((a, b) => a || b)"
      case _: SumMonoid => q"((a, b) => a + b)"
      case _: MultiplyMonoid => q"((a, b) => a * b)"
      case _: MaxMonoid => q"((a, b) => if (a > b) a else b)"
      case _: MinMonoid => q"((a, b) => if (a < b) a else b)"
      //      case _: MaxMonoid | _: MinMonoid => throw new UnsupportedOperationException(s"$m should be not be computed with fold. Use native support for operation.")
    }

    /** Get identifier name
      */
    def idnName(idnNode: IdnNode) = {
      // TODO: Improve generation of identifier names to never conflict with user-defined names
      val idn = idnNode.idn
      if (idn.startsWith("$"))
        s"___anon${idn.drop(1)}"
      else if (idn.contains("$"))
        s"___${idn.replace("$", "___")}"
      else
        idn
    }

    /** Build nullable filter.
      */
    def nullableFilter(p: Pattern): Tree = {
      val nullables = patternNullable(p)
      if (nullables.isEmpty)
        q"true"
      else if (nullables.size == 1)
        q"${nullables.head} != null"
      else {
        // TODO is it possible to get rid of the "true" by using optOuts.head? I didn't manage
        val optOuts = nullables.map { n => q"$n != null" }
        optOuts.fold(q"true"){case (a, b) => q"$a && $b"}
      }
    }

    /** Build nullable condition.
      */
    def nullableCond(e: Exp, p: Pattern): Tree = {
      def collectNullableIdns(p: Pattern): Seq[Tree] = p match {
        //      TODO only take nullables
        case PatternIdn(idnDef) => Seq(q"${Ident(TermName(idnName(idnDef)))}")
        case PatternProd(ps) => ps.flatMap(collectNullableIdns)
        case _               => Seq()
      }

      val idns = collectNullableIdns(p)
      if (idns.isEmpty)
        q"true"
      else if (idns.size == 1)
        q"${idns.head} != null"
      else {
        // TODO is it possible to get rid of the "true" by using optOuts.head? I didn't manage
        val optOuts = idns.map { n => q"$n != null" }
        optOuts.fold(q"true"){case (a, b) => q"$a && $b"}
      }
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
      case n @ Filter(Gen(Some(pat), child), pred) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val code = q"""
        ${build(child)}.filter($childArg => {
          ..${idnVals("child", pat)}
          ${build(pred)} })
        """
        q"""
        val start = "************ Filter (Scala) ************"
        val res = $code
        val end = "************ Filter (Scala) ************"
        val endType = $endType
        res
        """

      /** Scala Unnest
        */
      case n @ Unnest(Gen(Some(patChild), child), Gen(Some(patPath), path), pred) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(patChild)}")
        val pathArg = c.parse(s"path: ${patternType(patPath)}")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code =
          q"""
        ${build(child)}
          .flatMap($childArg => {
            ..${idnVals("child", patChild)}
              ${build(path)}
                .toIterable
                .filter($pathArg => {
                  ..${idnVals("path", patPath)}
                  ${build(pred)} })
                .map($pathArg => {
                  $rt(child, path) })})
        """
        q"""
        val start = "************ Unnest (Scala) ************"
        val res = $code
        val end = "************ Unnest (Scala) ************"
        val endType = $endType
        res
        """

      /** Scala OuterUnnest
        */
      case n @ OuterUnnest(Gen(Some(patChild), child), Gen(Some(patPath), path), pred) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(patChild)}")
        val pathArg = c.parse(s"path: ${patternType(patPath)}")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code =
          q"""
        ${build(child)}
          .flatMap($childArg => {
            ..${idnVals("child", patChild)}
            if (!${nullableCond(pred, patChild)}) {
              scala.collection.Iterable( $rt(child, null) )
            } else {
              ..${idnVals("child", patChild)}
              val matches =
                ${build(path)}
                  .toIterable
                  .filter($pathArg => {
                    ..${idnVals("path", patPath)}
                    ${nullableFilter(patPath)} })
                  .filter($pathArg => {
                    ..${idnVals("path", patPath)}
                    ${build(pred)} })
              if (matches.isEmpty)
                scala.collection.Iterable( $rt(child, null) )
              else
                matches.map(pathElement => $rt(child, pathElement)) }})
        """
        q"""
        val start = "************ OuterUnnest (Scala) ************"
        val res = $code
        val end = "************ OuterUnnest (Scala) ************"
        val endType = $endType
        res
        """

      /** Scala Join
        */
      case n @ Join(Gen(Some(patLeft), childLeft), Gen(Some(patRight), childRight), p) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val leftArg = c.parse(s"left: ${patternType(patLeft)}")
        val rightArg = c.parse(s"right: ${patternType(patRight)}")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code =
          q"""
        val rightCode = ${build(childRight)}.toList.toIterable
        ${build(childLeft)}
          .flatMap($leftArg => {
            ..${idnVals("left", patLeft)}
            rightCode
              .filter($rightArg => {
                ..${idnVals("right", patRight)}
                ${build(p)} })
              .map($rightArg => {
                $rt(left, right) })})
        """
        q"""
        val start = "************ Join (Scala) ************"
        val res = $code
        val end = "************ Join (Scala)************"
        val endType = $endType
        res
        """

      /** Scala OuterJoin
        */
      case n @ OuterJoin(Gen(Some(patLeft), childLeft), Gen(Some(patRight), childRight), p) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val leftArg = c.parse(s"left: ${patternType(patLeft)}")
        val rightArg = c.parse(s"right: ${patternType(patRight)}")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code =
          q"""
        val rightCode = ${build(childRight)}.toList.toIterable
        ${build(childLeft)}
          .flatMap($leftArg => {
            ..${idnVals("left", patLeft)}
            if (!${nullableCond(p, patLeft)}) {
              scala.collection.Iterable( $rt(left, null) )
            } else {
              ..${idnVals("left", patLeft)}
              val matches =
                rightCode
                  .filter($rightArg => {
                    ..${idnVals("right", patRight)}
                    ${nullableFilter(patRight)} })
                  .filter($rightArg => {
                    ..${idnVals("right", patRight)}
                    ${build(p)} })
              if (matches.isEmpty)
                scala.collection.Iterable( $rt(left, null) )
              else
                matches.map(right => $rt(left, right)) }})
        """
        q"""
        val start = "************ OuterJoin (Scala) ************"
        val res = $code
        val end = "************ OuterJoin (Scala)************"
        val endType = $endType
        res
        """

      /** Scala Reduce
        */
      case n @ Reduce(m, Gen(Some(pat), child), e) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val projected = q"""
        ${build(child)}.map($childArg => {
          ..${idnVals("child", pat)}
          ${build(e)} })
        """
        val code = m match {
          case _: MaxMonoid => q"""$projected.max"""
          case _: MinMonoid => q"""$projected.min"""
          case m1: PrimitiveMonoid => q"""$projected.foldLeft(${zero(m1)})(${fold(m1)})""" // TODO: fold vs foldLeft?
          case _: SetMonoid => q"""$projected.toSet.toIterable"""
          case _: BagMonoid => q"""$projected.toList.toIterable"""
          case _: ListMonoid => q"""$projected.toList.toIterable"""
        }
        q"""
        val start = "************ Reduce (Scala) ************"
        val res = $code
        val end = "************ Reduce (Scala) ************"
        val endType = $endType
        res
        """

      /** Scala Nest2
        */
      case n @ Nest2(m: PrimitiveMonoid, Gen(Some(pat), child), k, p, e) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        var keys1 = c.parse(s"keys1: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)}]")
        var keys2 = c.parse(s"keys2: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(e), world, analyzer)}]")
        val code = q"""{
        val $keys1 = ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} })

         val $keys2 =
          keys1.map($groupedArg =>
            (
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .fold(${zero(m)})(${fold(m)}) ))
        keys1.toIterable.flatMap{case (k, items) => val r = keys2(k) ; items.map{x => $rt(x, r)}}
        }"""
        q"""
        val start = "************ Nest2 Primitive Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest2 Primitive Monoid (Scala) ************"
        val endType = $endType
        res"""

      /** Scala Nest
        */
      case n @ Nest(m: PrimitiveMonoid, Gen(Some(pat), child), k, p, e) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code = q"""
        ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} }).toIterable
          .map($groupedArg =>
            $rt(
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .fold(${zero(m)})(${fold(m)}) ))
        """
        q"""
        val start = "************ Nest Primitive Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest Primitive Monoid (Scala) ************"
        val endType = $endType
        res"""

      case n @ Nest2(m: SetMonoid, Gen(Some(pat), child), k, p, e) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        var keys1 = c.parse(s"keys1: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)}]")
        var keys2 = c.parse(s"keys2: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(CollectionType(SetMonoid(), analyzer.tipe(e)), world, analyzer)}]")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code = q"""{
        val keys1 = ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} })

         val keys2 =
          keys1.map($groupedArg =>
            (
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .toSet.toIterable ))
        keys1.toIterable.flatMap{case (k, items) => val r = keys2(k) ; items.map{x => $rt(x, r)}}
        }
        """
        q"""
        val start = "************ Nest2 Set Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest2 Set Monoid (Scala) ************"
        val endType = $endType
        res"""

      case n @ Nest(m: SetMonoid, Gen(Some(pat), child), k, p, e) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code = q"""
        ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} }).toIterable
          .map($groupedArg =>
            $rt(
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .toSet.toIterable ))
        """
        q"""
        val start = "************ Nest Set Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest Set Monoid (Scala) ************"
        val endType = $endType
        res"""

      case n @ Nest2(_: ListMonoid, Gen(Some(pat), child), k, p, e) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        var keys1 = c.parse(s"keys1: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)}]")
        var keys2 = c.parse(s"keys2: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(CollectionType(ListMonoid(), analyzer.tipe(e)), world, analyzer)}]")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code = q"""{
        val keys1 = ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} })

         val keys2 =
          keys1.map($groupedArg =>
            (
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .toList.toIterable ))
        keys1.toIterable.flatMap{case (k, items) => val r = keys2(k) ; items.map{x => $rt(x, r)}}
        }
        """
        q"""
        val start = "************ Nest2 List Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest2 List Monoid (Scala) ************"
        val endType = $endType
        res"""


      case n @ Nest(_: ListMonoid, Gen(Some(pat), child), k, p, e) =>
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code = q"""
        ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} }).toIterable
          .map($groupedArg =>
            $rt(
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .toList.toIterable ))
        """
        q"""
        val start = "************ Nest List Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest List Monoid (Scala) ************"
        val endType = $endType
        res"""

      case n @ Nest2(_: BagMonoid, Gen(Some(pat), child), k, p, e) =>
        // TODO: This is doing sortBy w/ a toString (!!!!). Super-slow.
        // TODO: Instead, implement a real Bag type in Scala and use it.
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        var keys1 = c.parse(s"keys1: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)}]")
        var keys2 = c.parse(s"keys2: Map[${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(CollectionType(BagMonoid(), analyzer.tipe(e)), world, analyzer)}]")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code = q"""{
        val $keys1 = ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} })

         val $keys2 =
          keys1.map($groupedArg =>
            (
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .toList.sortBy(x => x.toString).toIterable ))
        keys1.toIterable.flatMap{case (k, items) => val r = keys2(k) ; items.map{x => $rt(x, r)}}
        }
        """
        q"""
        val start = "************ Nest2 Bag Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest2 Bag Monoid (Scala) ************"
        val endType = $endType
        res"""

      case n @ Nest(_: BagMonoid, Gen(Some(pat), child), k, p, e) =>
        // TODO: This is doing sortBy w/ a toString (!!!!). Super-slow.
        // TODO: Instead, implement a real Bag type in Scala and use it.
        val endType = PrettyPrinter(analyzer.tipe(n))
        val childArg = c.parse(s"child: ${patternType(pat)}")
        val groupedArg = c.parse(s"arg: (${buildScalaType(analyzer.tipe(k), world, analyzer)}, ${buildScalaType(analyzer.tipe(child), world, analyzer)})")
        val rt = q"${Ident(TermName(tupleSym(analyzer.tipe(n).asInstanceOf[CollectionType].innerType.asInstanceOf[RecordType])))}"
        val code = q"""
        ${build(child)}
          .groupBy($childArg => {
            ..${idnVals("child", pat)}
            ${build(k)} }).toIterable
          .map($groupedArg =>
            $rt(
              arg._1,
              arg._2
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${nullableFilter(pat)} })
                .filter($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(p)} })
                .map($childArg => {
                  ..${idnVals("child", pat)}
                  ${build(e)} })
                .toList.sortBy(x => x.toString).toIterable ))
        """
        q"""
        val start = "************ Nest Bag Monoid (Scala) ************"
        val res = $code
        val end = "************ Nest Bag Monoid (Scala) ************"
        val endType = $endType
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

    val tree = build(treeExp)

    val treeType = analyzer.tipe(treeExp)
    logger.info(s"Result type of query: $treeType")

    val collectedTree = tree match {
      case a: AlgebraNode if analyzer.spark(a) => collectSpark(tree, treeType)
      case _ => collectScala(tree, treeType)
    }

    collectedTree
  }

  /** Collect results from in-memory Scala.
    */
  def collectScala(tree: Tree, treeType: raw.Type): Tree = treeType match {
    case _: CollectionType => tree // q"$tree.toList"
    case _ => tree
  }

  /** Collect results from Spark.
    */
  def collectSpark(tree: Tree, treeType: raw.Type): Tree = {
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
    logger.info(s"World source are: ${world.sources}")

    loggerQueries.info(s"Query ($queryLanguage):\n$query")
    // Parse the query, using the catalog generated from what the user gave.
    val parseResult: Either[QueryError, Calculus.Calculus] = queryLanguage match {
      case Qrawl => Query(query, world)
      case _ => throw new UnsupportedOperationException(s"$queryLanguage not yet supported")
    }

    parseResult match {
      case Right(tree) =>
        val treeExp = tree.root

        var expStr = CalculusPrettyPrinter(treeExp)
        loggerQueries.info(s"Logical tree:\n$expStr")

        val isSpark: Map[String, Boolean] = accessPaths.map(ap => (ap.name, ap.isSpark)).toMap

        val physicalAnalyzer = new PhysicalAnalyzer(tree, world, query, isSpark)
        physicalAnalyzer.tipe(tree.root) // Type the root of the tree to force all nodes to be typed

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
          val res = executeQuery(..$methodCallParameters)
          raw.utils.RawUtils.convertIterablesToList(res)
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

      case Left(err) => {
        bail(err)
      }
    }
  }

  def query_impl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    RawImpl.queryError.remove()
    if (annottees.size > 1) {
      bail(s"Expected a single annottated element. Found: ${annottees.size}\n" + annottees.map(expr => showCode(expr.tree)).mkString("\n"))
    }
    try {
      val annottee: Expr[Any] = annottees.head
      val annottation = c.prefix.tree
      val makro = annottee.tree
      //    logger.debug("Annotation: " + annottation)
      logger.info("Expanding annotated target:\n{}", showCode(makro))
      //    logger.debug("Tree:\n" + showRaw(makro))

      val (query: String, language: QueryLanguage, accessPaths: List[AccessPath]) = extractQueryAndAccessPath(makro)
      logger.info("Access paths: {}", accessPaths.map(ap => ap.name + ", isSpark: " + ap.isSpark).mkString("; "))
      generateCode(makro, query, language, accessPaths)
    } catch {
      // raised by calling c.bail(). Just passthrough because we already set the error information in RawImpl.queryError
      case t: AbortMacroException => throw t
      case t: Throwable => bail(t) // Other unexpected problems, like MatchError. Call bail to set the error information
    }
  }
}
