package raw.executor

import org.kiama.attribution.Attribution
import raw._
import raw.algebra._

case class PhysicalPlanRewriterError(err: String) extends RawException(err)

object PhysicalAlgebra {


  sealed abstract class PhysicalNode
  sealed abstract class LLVMNode extends PhysicalNode
  sealed abstract class SparkNode extends PhysicalNode
  sealed abstract class LLVMOperator extends LLVMNode
  sealed abstract class SparkOperator extends SparkNode

  /**
   * 'Primitive' blocks
   */
  case class ForBlock(condBlock: LLVMNode, bodyActions: Seq[LLVMNode], incBlock: Seq[LLVMNode],
                      endBlock: Seq[LLVMNode], insertBeforeBlock: Option[LLVMNode]) extends LLVMNode
  case class IfBlock(condBlock: LLVMNode, thenActions: Seq[LLVMNode], mergeActions: Seq[LLVMNode])
    extends LLVMNode
  case class IfElseBlock(condBlock: LLVMNode, thenActions: Seq[LLVMNode], elseActions: Seq[LLVMNode],
                         mergeActions: Seq[LLVMNode]) extends LLVMNode

  //Hashing not yet relevant / used, not until we push Joins/Nests in LLVM
  case class HashBlock(exprEval: PhysicalNode) extends LLVMNode
  case class FlushBlock(exprEval: PhysicalNode) extends LLVMNode
  case class SerializeBlock(flushingBLock: FlushBlock) extends LLVMNode

  /**
   * Plugin-specific stubs
   */
  sealed abstract class Plugin
  case class CSVPlugin(src: Source) extends Plugin
  case class JSONPlugin(src: Source) extends Plugin

  //CSV
  case class CSVInit(src: Source) extends LLVMNode
  case class CSVEof() extends LLVMNode
  case class CSVFinish(src: Source) extends LLVMNode
  case class CSVReadAsInt(attrName: String) extends LLVMNode
  case class CSVReadAsFloat(attrName: String) extends LLVMNode
  case class CSVReadAsString(attrName: String) extends LLVMNode
  case class CSVReadAsBool(attrName: String) extends LLVMNode
  case class CSVSkipField() extends LLVMNode
  case class CSVReadPath() extends LLVMNode
  case class CSVReadValue() extends LLVMNode

  //JSON
  case class JSONInit(src: Source) extends LLVMNode
  case class JSONEof() extends LLVMNode
  case class JSONSkipEntry() extends LLVMNode
  /* Should these two be exposed? ReadPath() is needed to unnest..*/
  case class JSONReadPath() extends LLVMNode
  case class JSONReadValue() extends LLVMNode
  case class JSONInitCollection() extends LLVMNode
  /* Relevant to Unnest */
  case class JSONHasNext() extends LLVMNode
  case class JSONGetNext() extends LLVMNode

  //Output-related
  /* TODO Is the concept of a Serializer needed here? */
  case class BeginList() extends LLVMNode
  case class BeginBag() extends LLVMNode
  case class BeginSet() extends LLVMNode
  case class EndList() extends LLVMNode
  case class EndBag() extends LLVMNode
  case class EndSet() extends LLVMNode
  //Place delimiter (if needed)
  case class Delimiter() extends LLVMNode

  /**
   * Various
   */
  case class ExprEval(e: Algebra.Exp) extends LLVMNode
  case class AddToState() extends LLVMNode
  case class CallParent() extends LLVMNode
  case class CallProduce(node: LLVMNode) extends LLVMNode
  case class CallConsume(node: LLVMNode) extends LLVMNode
  //TODO Need Types for accumulator and flusher
  case class Accumulate(m: Monoid) extends LLVMNode
  case class Flush() extends LLVMNode
  //Outer-unnest-related
  case class BitmapInit() extends LLVMNode
  case class BitmapEof()  extends LLVMNode
  case class BitmapAdd()  extends LLVMNode
  //Quite common building blocks - will try to avoid
//  case class IsNull()     extends LLVMNode
//  case class IsNotNull()  extends LLVMNode


  /**
   * Almost 1-1 equivalence with Logical Algebra nodes.
   */
  case class PluginCSVStaged(init: Seq[LLVMNode], toEmit: LLVMNode) extends LLVMOperator
  case class PluginJSONStaged() extends LLVMOperator
  case class SelectStaged(init:Seq[LLVMNode], toEmit: LLVMNode, child: PhysicalNode) extends LLVMOperator
  case class UnnestStaged(init: Seq[LLVMNode], toEmit: LLVMNode, child: PhysicalNode) extends LLVMOperator
  case class OuterUnnestStaged(init: Seq[LLVMNode], toEmit: LLVMNode, child: PhysicalNode) extends LLVMOperator
  case class ReducePrimitiveStaged(m: Monoid, init: Seq[LLVMNode], toEmit: LLVMNode, queryEnd: Seq[LLVMNode], child:
  PhysicalNode) extends LLVMOperator
  case class ReduceCollectionStaged(m: Monoid, init: Seq[LLVMNode], toEmit: LLVMNode, queryEnd: Seq[LLVMNode], child:
  PhysicalNode) extends LLVMOperator
  //Not planning to push in LLVM land for the time being
  case class Join(leftChild: PhysicalNode, rightChild: PhysicalNode) extends SparkOperator
  case class OuterJoin(leftChild: PhysicalNode, rightChild: PhysicalNode) extends SparkOperator
  case class Nest(child: PhysicalNode) extends SparkOperator
  case class ReduceSet(child: PhysicalNode) extends SparkOperator
}

class PhysicalPlanRewriter(tree: Algebra.OperatorNode, world: World) extends Attribution {

  import PhysicalAlgebra._
  import Algebra._

  /* Register 'relevant data sources'. These include
  *  a) scans
  *  b) joins
  *  c) nests
  *  d) unnests (just for the unnested field)
  *  These sources will be used when
  *  i)  registering plugins
  *  ii) identifying which source is relevant for a path */
  var opSources : Map[Algebra.OperatorNode, String] = Map()
  val sourcePlugins : Map[String, Plugin] = Map()
  var opCnt : Int = 1
  def registerSources(n : Algebra.OperatorNode) : Unit = n match {
    case Algebra.Scan(name) => {
      opSources = opSources + (n -> name)
      val src: Source = world.getSource(name)
      src.location match  {
        case LocalFileLocation(path, "text/csv") => sourcePlugins + (name -> CSVPlugin(src))
        case LocalFileLocation(path, "application/json") => sourcePlugins + (name -> JSONPlugin(src))
        case loc @ MemoryLocation(data) => throw PhysicalPlanRewriterError(s"Physical Plan Rewriter does not yet " +
          s"support location $loc")
        case loc => throw PhysicalPlanRewriterError(s"Physical Plan Rewriter does not support " +
          s"location $loc")
      }
    }
    case Algebra.Join(_, left, right)       => {
      opSources = opSources + (n -> ("join"+opCnt.toString))
      opCnt = opCnt + 1
      registerSources(left)
      registerSources(right)
      /* TODO Must add 'join JVM source' to World and also register it here. Schema?? 'Needed fields?'*/
    }
    case Algebra.OuterJoin(_, left, right)  => {
      opSources = opSources + (n -> ("outerJoin"+opCnt.toString))
      opCnt += 1
      registerSources(left)
      registerSources(right)
      /* TODO Must add 'outer join JVM source' to World and also register it here. Schema?? 'Needed fields?'*/
    }
    case Algebra.Select(_, child)           => registerSources(child)
    case Algebra.Unnest(_, _, child)        => {
      opSources = opSources + (n -> ("unnest"+opCnt.toString))
      opCnt = opCnt + 1
      registerSources(child)
    }
    case Algebra.OuterUnnest(_, _, child)   => {
      opSources = opSources + (n -> ("outerUnnest"+opCnt.toString))
      opCnt = opCnt + 1
      registerSources(child)
    }
    case Algebra.Reduce(_, _,  _, child)    => registerSources(child)
    case Algebra.Nest(m, e, _, _, _, child) => {
      opSources = opSources + (n -> ("nest"+opCnt.toString))
      opCnt += 1
      registerSources(child)
      /* TODO Must add 'outer nest JVM source' to World and also register it here. Schema?? 'Needed fields?'*/
    }
    case Algebra.Merge(_, left, right)      => registerSources(left); registerSources(right)
  }

  /* ii) identifying which source is relevant for a path */
  lazy val originalSourceName: OperatorNode => Exp => String = paramAttr {
    n => {
      case Arg(idx)                           => {
        val allQuerySources = sourceNames(n)
        allQuerySources(idx)
      }
      case RecordProj(e, idn)                 => originalSourceName(n)(e)
      case unknown                            => throw PhysicalPlanRewriterError(s"Unexpected expression: $unknown")
    }
  }

  /*
    Note: Pipeline breaking operators are treated as new data sources!
          (So are unnests - at least partially)
     => Have to bridge the different notion of Arg between Logical and Physical lvl
     => Use the same # of Args, but replace appropriate slots with the new sourceNames
     */
  lazy val sourceNames: Algebra.OperatorNode => Seq[String] = attr {
    case Algebra.Scan(name)                         => Seq(name)
    case j @ Algebra.Join(_, left, right)           => {
      val childrenSources = sourceNames(left) ++ sourceNames(right)
      //Replace all childrenNames with the alias of join
      childrenSources.map(_ => opSources(j))
    }
    case oj @ Algebra.OuterJoin(_, left, right)     => {
      val childrenSources = sourceNames(left) ++ sourceNames(right)
      //Replace all childrenNames with the alias of the outer join
      childrenSources.map(_ => opSources(oj))
    }
    case Algebra.Select(_, child)                   => sourceNames(child)
    case u @ Algebra.Unnest(path, _, child)             => {
      val originalSourceNames = sourceNames(child)
      /*def pathArg(path: Exp) : Int = path match {
        case RecordProj(e,idn)  => pathArg(e)
        case Arg(idn)           => idn
        case _                  => throw PhysicalPlanRewriterError(s"Unexpected expression: $path")
      }
      val argProjected : Int = pathArg(path)*/
      originalSourceNames :+ opSources(u)/*originalSourceNames(argProjected)*/
    }
    case ou @ Algebra.OuterUnnest(path, _, child)   => {
      val originalSourceNames = sourceNames(child)
      /*def pathArg(path: Exp) : Int = path match {
        case RecordProj(e,idn)  => pathArg(e)
        case Arg(idn)           => idn
        case _                  => throw PhysicalPlanRewriterError(s"Unexpected expression: $path")
      }
      val argProjected : Int = pathArg(path)*/
      originalSourceNames :+ opSources(ou)/*originalSourceNames(argProjected)*/
    }
    case r @ Algebra.Reduce(_, _,  _, child)        => sourceNames(child)
    case n @ Algebra.Nest(m, e, _, _, _, child)     => {
      val childrenSources = sourceNames(child)
      //Replace all childrenNames with the alias of the nest
      childrenSources.map(_ => opSources(n))
    }
    case Algebra.Merge(_, left, right)              => sourceNames(left) ++ sourceNames(right)
    case unknown                                    => throw PhysicalPlanRewriterError(s"Unexpected operator: $unknown")
  }

  /* Used to identify 'neededFields' per operator.
  * Any operator touching / materializing data needs this information */
  lazy val fieldsInExpr: OperatorNode => Exp => Map[Int, Set[List[String]]] = paramAttr {
    n => {
      case Null                                    => Map()
      case _: BoolConst                            => Map()
      case _: IntConst                             => Map()
      case _: FloatConst                           => Map()
      case _: StringConst                          => Map()
      case Arg(idx)                                => Map(idx -> Set())
      case ProductProj(e, idx)                     => Map(idx -> fieldsInExpr(n)(e)(idx)) //TODO proper understanding?
      case ProductCons(es)                         => {
        /* Have to join the Maps of all Expressions in es */
        val mergeAll : Seq[(Int, Set[List[String]])] = es.foldLeft(Seq[(Int, Set[List[String]])]())
        {
          (m1 , m2) => m1 ++ (fieldsInExpr(n)(m2)).toSeq
        }
        val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = mergeAll.groupBy(_._1)
        val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
        toMap
      }
      case rp @ RecordProj(e, idn)                      => {
        def createProjList(exp: Exp): Map[Int, Set[List[String]]] = {
          def createProjList_(exp: Exp, acc: List[String]): (Int, List[String]) =
            exp match {
              case RecordProj(e_, idn_) => createProjList_(e_, idn_ :: acc)
              case Arg(idx) => (idx, acc)
              case other => throw PhysicalPlanRewriterError(s"Unexpected expression: $other")
            }

          val pair: (Int, List[String]) = createProjList_(exp,List())
          Map(pair._1 -> Set(pair._2))
        }
        createProjList(rp)
      }
      case RecordCons(atts)                        => {
        /* Have to join the Maps of all Expressions in es */
        val mergeAll: Seq[(Int, Set[List[String]])] = atts.map(_.e).foldLeft(Seq[(Int, Set[List[String]])]()) {
          (m1, m2) => m1 ++ (fieldsInExpr(n)(m2)).toSeq
        }
        val grouped: Map[Int, Seq[(Int, Set[List[String]])]] = mergeAll.groupBy(_._1)
        val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
        toMap
      }
      case IfThenElse(e1, e2, e3)                    => {
        val m1 = (fieldsInExpr(n)(e1)).toSeq
        val m2 = (fieldsInExpr(n)(e2)).toSeq
        val m3 = (fieldsInExpr(n)(e3)).toSeq
        val merged = m1 ++ m2 ++ m3
        val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
        val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
        toMap
      }
      case BinaryExp(_, e1, e2)  => {
        val m1 = (fieldsInExpr(n)(e1)).toSeq
        val m2 = (fieldsInExpr(n)(e2)).toSeq
        val merged = m1 ++ m2
        val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
        val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
        toMap
      }
      case UnaryExp(_, e)                          => fieldsInExpr(n)(e)
      case ZeroCollectionMonoid(m)                 => Map()
      case ConsCollectionMonoid(m, e)              => Map()
      case MergeMonoid(_, e1, e2)                  => {
        val m1 = (fieldsInExpr(n)(e1)).toSeq
        val m2 = (fieldsInExpr(n)(e2)).toSeq
        val merged = m1 ++ m2
        val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
        val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
        toMap
      }

    }
  }

  lazy val neededFields: Algebra.OperatorNode => Map[Int, Set[List[String]]] => Map[(String,Int), Set[List[String]]] =
  attr {
    case Algebra.Scan(name)                         => m => m.map { case (k,v) => ((name,0), v) }
    case j @ Algebra.Join(p, left, right)           => m => {
      println("Join and Needed")
      val m2 = fieldsInExpr(j)(p)
      println("Join Pred Fields: "+m2)
      val merged = m.toSeq ++ m2.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(j)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case oj @ Algebra.OuterJoin(p, left, right)     => m => {
      val m2 = fieldsInExpr(oj)(p)
      val merged = m.toSeq ++ m2.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(oj)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case s @ Algebra.Select(p, child)               => m => {
      val m2 = fieldsInExpr(s)(p)
      val merged = m.toSeq ++ m2.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(s)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case u @ Algebra.Unnest(path, p, child)         => m => {
      val m2 = fieldsInExpr(u)(path)
      val m3 = fieldsInExpr(u)(p)
      val merged = m.toSeq ++ m2.toSeq ++ m3.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(u)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case ou @ Algebra.OuterUnnest(path, p, child)   => m => {
      val m2 = fieldsInExpr(ou)(path)
      val m3 = fieldsInExpr(ou)(p)
      val merged = m.toSeq ++ m2.toSeq ++ m3.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(ou)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case r @ Algebra.Reduce(_, e,  p, child)        => m => {
      /* Note: m should be empty here, assuming that a query plan quantum start with a Reduce */
      val m2 = fieldsInExpr(r)(e)
      val m3 = fieldsInExpr(r)(p)
      val merged = m2.toSeq ++ m3.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(r)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      //println("[Reduce: ] "+toMap.map {case (k,v) => (sources(k), v)})
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case n @ Algebra.Nest(_, e, f, p, g, child)     => m => {
      val m2 = fieldsInExpr(n)(e)
      val m3 = fieldsInExpr(n)(f)
      val m4 = fieldsInExpr(n)(p)
      val m5 = fieldsInExpr(n)(g)
      val merged = m.toSeq ++ m2.toSeq ++ m3.toSeq ++ m4.toSeq ++ m5.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(n)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case mer @ Algebra.Merge(_, left, right)        => m => {
      sourceNames(left) ++ sourceNames(right)
      val merged = m.toSeq
      val grouped : Map[Int, Seq[(Int, Set[List[String]])]] = merged.groupBy(_._1)
      val toMap = grouped.mapValues(_.flatMap(_._2).toSet)
      /* Important to retrieve 'original relation names' here */
      val sources : Seq[String] = sourceNames(mer)
      val sourcesWithIndex : Seq[(String,Int)] = sources.view.zipWithIndex
      toMap.map {case (k,v) => (sourcesWithIndex(k), v)}
    }
    case unknown                                    => throw PhysicalPlanRewriterError(s"Unexpected operator:$unknown")
  }

  def apply(): PhysicalAlgebra.PhysicalNode = {

    /*
    * Convert logical plan to physical.
    * replacing nodes with LLVM ones when applicable.
    */
    def rewriteOperators(e: Algebra.OperatorNode): PhysicalAlgebra.PhysicalNode = e match {
      //TODO What about 'neededFields'? This must be already known at this point. For now, I will assume
      // they are all needed...

      case Algebra.Scan(name)                                 => {
        val src : raw.Source = world.catalog(name)
        src.location match {
          case loc @ LocalFileLocation(p,"text/csv") => {
            import raw.executor.PhysicalAlgebra._
            def parse(t: Type, itemName: String): LLVMNode = t match {
              case IntType() => CSVReadAsInt(itemName)
              case FloatType() => CSVReadAsFloat(itemName)
              case BoolType() => CSVReadAsBool(itemName)
              case StringType() => CSVReadAsString(itemName)
              case _ => throw PhysicalPlanRewriterError(s"cannot parse $t in text/csv files")
            }

            val t: Type = src.tipe
            t match {
              case CollectionType(ListMonoid(), RecordType(atts)) => {
                val actions : Seq[LLVMNode] = atts.map( att => if(true) { CSVSkipField() } else parse(att.tipe,att.idn) )
                val actionsAll : Seq[LLVMNode] = actions :+ CallParent()
                PluginCSVStaged(Seq(CSVInit(src)),ForBlock(CSVEof(),actionsAll,Seq(),Seq(),None))
              }
              case _ => throw PhysicalPlanRewriterError(s"cannot make data source from $t")
            }
          }
          case loc @ LocalFileLocation(p,"application/json") => {
            //TODO Add JSON arguments
            PluginJSONStaged()
          }
          case loc @ LocalFileLocation(p,fileType) => throw PhysicalPlanRewriterError(s"Unexpected MIME type: $src.location.")
          case loc => throw PhysicalPlanRewriterError(s"does not support location $loc")
        }
      }
      case Algebra.Select(p, child)                           => {
        val selectBlock = IfBlock(ExprEval(p), Seq(CallParent()), Seq())
        SelectStaged(Seq(),selectBlock,rewriteOperators(child))
      }
      case Algebra.Join(p, l, r)                              => PhysicalAlgebra.Join(rewriteOperators(l), rewriteOperators(l))
      case Algebra.OuterJoin(p, l, r)                         => PhysicalAlgebra.OuterJoin(rewriteOperators(l),
        rewriteOperators(l))
      case Algebra.Unnest(path, pred, child)                  => {
        val sourceName = originalSourceName(e)(path)
        val pgHooks : Map[String,LLVMNode] = sourcePlugins.get(sourceName) match  {
          case Some(pg : Plugin) => pg match {
            case CSVPlugin(src) => throw PhysicalPlanRewriterError(s"$pg does not support collections")
            case JSONPlugin(src) => {
              Map("readPath"        -> JSONReadPath(),
                  "initCollection"  -> JSONInitCollection(),
                  "hasNext"         -> JSONHasNext(),
                  "getNext"         -> JSONGetNext())
            }
            case _ => throw PhysicalPlanRewriterError(s"Unexpected plugin $pg")
          }
          case None => throw PhysicalPlanRewriterError(s"Unknown source: $sourceName.")
        }
        val initActions: Seq[LLVMNode] =
          Seq(pgHooks("readPath"),pgHooks("initCollection"))
        val bodyActions: Seq[LLVMNode] =
          Seq(pgHooks("getNext"),AddToState(),IfBlock(ExprEval(pred),Seq(CallParent()),Seq()))
        val forBlock : LLVMNode =
          ForBlock(pgHooks("hasNext"),bodyActions,Seq(),Seq(),None)
        UnnestStaged(initActions, forBlock, rewriteOperators(child))
      }
      case Algebra.OuterUnnest(path, pred, child)             => {
        val sourceName = originalSourceName(e)(path)
        val pgHooks : Map[String,LLVMNode] = sourcePlugins.get(sourceName) match  {
          case Some(pg : Plugin) => pg match {
            case CSVPlugin(src) => throw PhysicalPlanRewriterError(s"$pg does not support collections")
            case JSONPlugin(src) => {
              Map("readPath"        -> JSONReadPath(),
                  "readValue"       -> JSONReadValue(),
                  "initCollection"  -> JSONInitCollection(),
                  "hasNext"         -> JSONHasNext(),
                  "getNext"         -> JSONGetNext())
            }
            case _ => throw PhysicalPlanRewriterError(s"Unexpected plugin $pg")
          }
          case None => throw PhysicalPlanRewriterError(s"Unknown source: $sourceName.")
        }
        val initActions: Seq[LLVMNode] = Seq()
        val bodyActions: Seq[LLVMNode] =
          Seq(pgHooks("getNext"),IfBlock(ExprEval(pred),Seq(BitmapAdd()),Seq(Accumulate(AndMonoid()))))
        val childrenLoop: LLVMNode = ForBlock(pgHooks("hasNext"),bodyActions,Seq(),Seq(),None)
        val forwardBitmap : LLVMNode = ForBlock(BitmapEof(),Seq(pgHooks("readValue"),CallParent()),Seq(),Seq(),None)
        val notNullActions: Seq[LLVMNode] = Seq(BitmapInit(),pgHooks("initCollection"),childrenLoop,forwardBitmap)
        val parentExpr : Exp = path match  {
          case RecordProj(e,_) => e
          case _ => throw PhysicalPlanRewriterError(s"Unexpected path expr $path")
        }
        val vNotNull : Exp = BinaryExp(Neq(),parentExpr,Null)
        val outerUnnestBody = IfBlock(ExprEval(vNotNull),notNullActions,Seq())
        OuterUnnestStaged(initActions, outerUnnestBody, rewriteOperators(child))
      }
      case Algebra.Reduce(m @ raw.SumMonoid(), e, p, child)       => {
        val ifActions = Seq(ExprEval(e),Accumulate(m))
        val ifBlock: LLVMNode = IfBlock(ExprEval(p),ifActions,Seq())

        ReducePrimitiveStaged(m,Seq(),ifBlock,Seq(Flush()),rewriteOperators(child))
      }
      case Algebra.Reduce(m@raw.MultiplyMonoid(), e, p, child)    => {
        val ifActions = Seq(ExprEval(e),Accumulate(m))
        val ifBlock: LLVMNode = IfBlock(ExprEval(p),ifActions,Seq())

        ReducePrimitiveStaged(m,Seq(),ifBlock,Seq(Flush()),rewriteOperators(child))
      }
      case Algebra.Reduce(m@raw.MaxMonoid(), e, p, child)         => {
        val ifActions = Seq(ExprEval(e),Accumulate(m))
        val ifBlock: LLVMNode = IfBlock(ExprEval(p),ifActions,Seq())

        ReducePrimitiveStaged(m,Seq(),ifBlock,Seq(Flush()),rewriteOperators(child))
      }
      case Algebra.Reduce(m@raw.AndMonoid(), e, p, child)         => {
        val ifActions = Seq(ExprEval(e),Accumulate(m))
        val ifBlock: LLVMNode = IfBlock(ExprEval(p),ifActions,Seq())

        ReducePrimitiveStaged(m,Seq(),ifBlock,Seq(Flush()),rewriteOperators(child))
      }
      case Algebra.Reduce(m@raw.OrMonoid(), e, p, child)          => {
        val ifActions = Seq(ExprEval(e),Accumulate(m))
        val ifBlock: LLVMNode = IfBlock(ExprEval(p),ifActions,Seq())

        ReducePrimitiveStaged(m,Seq(),ifBlock,Seq(Flush()),rewriteOperators(child))
      }
      case Algebra.Reduce(m@raw.ListMonoid(), e, p, child)        => {
        val init = Seq(BeginList())
        val ifActions = Seq(ExprEval(e),Accumulate(m))
        val ifBlock: LLVMNode = IfBlock(ExprEval(p),ifActions,Seq())
        val end = Seq(Flush(),EndList())

        ReduceCollectionStaged(m, init, ifBlock, end, rewriteOperators(child))
      }
      case Algebra.Reduce(m@raw.BagMonoid(), e, p, child)         => {
        val init = Seq(BeginBag())
        val ifActions = Seq(ExprEval(e),Accumulate(m))
        val ifBlock: LLVMNode = IfBlock(ExprEval(p),ifActions,Seq())
        val end = Seq(Flush(),EndBag())

        ReduceCollectionStaged(m, init, ifBlock, end, rewriteOperators(child))
      }
      case Algebra.Reduce(m@raw.SetMonoid(), e, p, child)         => PhysicalAlgebra.ReduceSet(rewriteOperators(child))
      case n                                                      => throw PhysicalPlanRewriterError(s"Unexpected node: $n")
    }

    registerSources(tree)
    rewriteOperators(tree)
  }
}
