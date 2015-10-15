package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution
import raw.World._

import scala.util.parsing.input.Position

case class SemanticAnalyzerException(err: String) extends RawException(err)

/** Analyzes the semantics of an AST.
  * This includes the type checker, type inference and semantic checks (e.g. whether monoids compose correctly).
  *
  * The semantic analyzer reads the user types and the catalog of user-defined class entities from the World object.
  * User types are the type definitions available to the user.
  * Class entities are the data sources available to the user.
  * e.g., in expression `e <- Events` the class entity is `Events`.
  *
  * The original user query is passed optionally for debugging purposes.
  */
class SemanticAnalyzer(val tree: Calculus.Calculus, val world: World, val queryString: Option[String] = None) extends Attribution with MonoidsGraph with LazyLogging {

  // TODO: Add a check to the semantic analyzer that the monoids are no longer monoid variables; they have been sorted out

  import scala.collection.immutable.Seq
  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.util.{Entity, MultipleEntity, UnknownEntity}

  //  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}

  import org.kiama.rewriting.Rewriter._
  import Calculus._
  import SymbolTable._
  import Constraint._
  import World.TypesVarMap

  /** Decorators on the tree.
    */
  private lazy val decorators = new Decorators(tree)

  import decorators.{chain, Chain}

  /** The map of type variables.
    * Updated during unification.
    */
  private val typesVarMap = new TypesVarMap()

  /** Add user types to the map.
    */
  for ((sym, t) <- world.tipes) {
    typesVarMap.union(UserType(sym), t)
  }

  /** Add type variables from user sources.
    */
  for ((_, t) <- world.sources) {
    for (tv <- getVariableTypes(t)) {
      typesVarMap.union(tv, tv)
    }
  }

  /** Stores unification errors.
    * Updated during unification.
    */
  private val tipeErrors =
    scala.collection.mutable.MutableList[Error]()

  /** Type the root of the program.
    * If the constraints could not be solved, the program cannot be typed.
    */
  lazy val tipeEverything = {
    val r = solve(constraints(tree.root))
    //    if (!r) {
    //      throw SemanticAnalyzerException("Requesting the type of an un-typable root")
    //    }
  }

  /** Return the base type of an expression, i.e. without the nullable flag.
    */
  lazy val baseType: Exp => Type = attr {
    e => {
      tipeEverything
      walk(expType(e))
    }
  }

  /** Add the nullable flag to a type.
    */
  // TODO: Should we use u.sym or also copy the symbol to a new one: e.g. Symbol(u.sym.idn) ?
  private def makeNullable(source: Type, models: Seq[Type], nulls: Seq[Type], nullable: Option[Boolean] = None): Type = {
    logger.debug(s"t is **** ${PrettyPrinter(source)} and models ${models.map(PrettyPrinter(_)).mkString(",")}")
    val t = (source, models) match {
      case (col @ CollectionType(m, i), colls: Seq[CollectionType]) =>
        val inners = colls.map(_.innerType)
        CollectionType(m, makeNullable(i, inners, inners, nullable))
      case (f @ FunType(p, e), funs: Seq[FunType])                  =>
        val otherP = funs.map(_.t1)
        val otherE = funs.map(_.t2)
        FunType(makeNullable(p, otherP, otherP, nullable), makeNullable(e, otherE, otherE, nullable))
      case (r @ RecordType(recAtts), recs: Seq[RecordType])         =>
        recAtts match {
          case Attributes(atts) =>
            RecordType(Attributes(
              for ((att, idx) <- atts.zipWithIndex) yield {
                val others = for (rec <- recs) yield {
                  rec.recAtts match {
                    case Attributes(atts1) => atts1(idx)
                  }
                }
                AttrType(att.idn, makeNullable(att.tipe, others.map(_.tipe), others.map(_.tipe), nullable))
              }))

          case AttributesVariable(atts, sym) =>
            RecordType(AttributesVariable(
              for ((att, idx) <- atts.zipWithIndex) yield {
                val others = for (rec <- recs) yield {
                  rec.getType(att.idn).get
                }
                AttrType(att.idn, makeNullable(att.tipe, others, others, nullable))
              }, sym))
          case ConcatAttributes(sym) =>
            // TODO: BEN!!!!
            RecordType(ConcatAttributes(sym))
        }
      //            atts.zip(recs.map {
      //              _.recAtts
      //            }).map { case (a1, as) => AttrType(a1.idn, makeNullable(a1.tipe, as.atts.map(_.tipe).to, as.atts.map(_.tipe).to, nullable)) }
      //          case AttributesVariable(atts, sym) =>
      //            atts.zip(recs.map {
      //              _.recAtts
      //            }).map { case (a1, as) => AttrType(a1.idn, makeNullable(a1.tipe, as.atts.map(_.tipe).to, as.atts.map(_.tipe).to, nullable)) }
      //        }
      //
      //              _.tipe))
      //            for (otherAtts <- recs.map(_.recAtts)) {
      //              atts.zip(otherAtts.atts).map{case (a1, a2) => AttrType(a1.idn, makeNullable(a1.tipe, a2.tipe))
      //            }
      //              val natts = atts.map { case AttrType(idn, i) =>
      //              val others = recs.map { r => r.getType(idn).get }
      //              AttrType(idn, makeNullable(i, others, others, nullable))
      //            }
      //            RecordType(Attributes(natts), n)
      //          case AttributesVariable(atts, sym) =>
      //            val natts = atts.map { case AttrType(idn, i) =>
      //              val others = recs.map { r => r.getType(idn).get }
      //              AttrType(idn, makeNullable(i, others, others, nullable))
      //            }
      //            RecordType(AttributesVariable(natts, sym), n)
      //        }
      case (PatternType(atts), _) =>
        PatternType(atts.map { att => PatternAttrType(makeNullable(att.tipe, Seq(), Seq(), nullable)) })
      case (_: IntType, _)        => IntType()
      case (_: FloatType, _)      => FloatType()
      case (_: BoolType, _)       => BoolType()
      case (_: StringType, _)     => StringType()
      case (u: UserType, _)       => UserType(u.sym)
      case (v: TypeVariable, _)   => TypeVariable(v.sym)
      case (v: NumberType, _)     => NumberType(v.sym)
    }
    t.nullable = nullable.getOrElse(t.nullable || nulls.collect { case t if t.nullable => t }.nonEmpty) // TODO: nulls.exists ?
    t
  }

  /** Return the type after resolving user types.
    */
  def resolvedType(t: Type, occursCheck: Set[Type] = Set()): Type =
    if (occursCheck.contains(t))
      throw new SemanticAnalyzerException("Cycle in user type definition")
    else t match {
      case UserType(sym) => resolvedType(world.tipes(sym), occursCheck + t)
      case _ => t
    }

  /** Extractor that resolves user types.
    */
  object ResolvedType {
    def unapply(t: Type): Option[Type] = Some(resolvedType(t))
  }

  /** Return the type of an expression, including the nullable flag.
    */
  lazy val tipe: Exp => Type = attr {
    e => {

      def innerTipe(t: Type): Type = resolvedType(t) match {
        case CollectionType(_, i) => i
      }

      val te = baseType(e) // regular type (no option except from sources)
      logger.debug(s"*** ${CalculusPrettyPrinter(e)} => ${PrettyPrinter(te)}")

      val nt = e match {
        case RecordProj(e1, idn)                 => tipe(e1) match {
          case rt: RecordType => makeNullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
          case ut: UserType =>
            typesVarMap(ut).root match {
              case rt: RecordType => makeNullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
            }
        }
        case ConsCollectionMonoid(m, e1)         => CollectionType(m, tipe(e1))
        case MultiCons(m, exps)                  => te match {
          case CollectionType(m2, inner) =>
            assert(m == m2)
            val others = exps.map(tipe(_))
            CollectionType(m, makeNullable(inner, others, others))
        }
        case IfThenElse(e1, e2, e3)              => (tipe(e1), tipe(e2), tipe(e3)) match {
          case (t1, t2, t3) => makeNullable(te, Seq(t2, t3), Seq(t1, t2, t3))
        }
        case FunApp(f, v)                        => tipe(f) match {
          case ft @ FunType(t1, t2) =>
            logger.debug(s"my te is ${PrettyPrinter(te)}")
            makeNullable(te, Seq(t2), Seq(ft, t2, tipe(v)))
        }
        case MergeMonoid(_, e1, e2)              => (tipe(e1), tipe(e2)) match {
          case (t1, t2) => makeNullable(te, Seq(t1, t2), Seq(t1, t2))
        }
        case Comp(m: CollectionMonoid, qs, proj) =>
          val inner = tipe(proj)
          makeNullable(te, Seq(CollectionType(m, inner)), qs.collect { case Gen(_, e1) => tipe(e1) })
        case Select(froms, d, g, proj, w, o, h)  =>
          val inner = tipe(proj)
          // we don't care about the monoid here, sine we just walk the types to make them nullable or not, not the monoids
          makeNullable(te, Seq(CollectionType(SetMonoid(), inner)), froms.collect { case Gen(_, e1) => tipe(e1) })
        case Reduce(m: PrimitiveMonoid, g, e1)   => makeNullable(te, Seq(tipe(e1)), Seq(tipe(g.e)))
        case Reduce(m: CollectionMonoid, g, e1)  => makeNullable(te, Seq(CollectionType(m, tipe(e1))), Seq(tipe(g.e)))
        case Filter(g, p)                        => makeNullable(te, Seq(tipe(g.e)), Seq(tipe(g.e)))
        case Join(g1, g2, p)                     => te match {
          case CollectionType(m, inner) => {
            val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
            makeNullable(te, Seq(expectedType), Seq(tipe(g1.e), tipe(g2.e)))
          }
        }
        case OuterJoin(g1, g2, p)                => {
          val x = te match {
            case CollectionType(m, inner) => {
              val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
              makeNullable(te, Seq(expectedType), Seq(tipe(g1.e), tipe(g2.e)))
            }
          }
          x match {
            case CollectionType(_, RecordType(Attributes(atts))) =>
              assert(atts.length == 2)
              atts(1).tipe.nullable = true
          }
          x
        }
        case Unnest(g1, g2, p)                   =>
          te match {
            case CollectionType(m, inner) => {
              val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
              makeNullable(te, Seq(expectedType), Seq(tipe(g1.e), tipe(g2.e)))
            }
          }
        case OuterUnnest(g1, g2, p)              =>
          val x = te match {
            case CollectionType(m, inner) => {
              val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
              makeNullable(te, Seq(expectedType), Seq(tipe(g1.e), tipe(g2.e)))
            }
          }
          x match {
            case CollectionType(_, RecordType(Attributes(atts))) =>
              assert(atts.length == 2)
              atts(1).tipe.nullable = true
          }
          x
        case Nest(m: CollectionMonoid, g, k, p, e1) => {
          te match {
            case CollectionType(m2, _) => makeNullable(te, Seq(CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", tipe(k)), AttrType("_2", CollectionType(m2, tipe(e1)))))))), Seq(tipe(g.e)))
          }
        }
        case Nest(m: PrimitiveMonoid, g, k, p, e1) => {
          te match {
            case CollectionType(m, _) => makeNullable(te, Seq(CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", tipe(k)), AttrType("_2", tipe(e1))))))), Seq(tipe(g.e)))
          }
        }

        //        case MultiNest(g, params) => makeNullable(te, Seq(), Seq(tipe(g.e)))
        case Comp(_, qs, proj) =>
          val output_type = tipe(proj) match {
            case _: IntType   => IntType()
            case _: FloatType => FloatType()
            case _: BoolType  => BoolType()
            case NumberType(v) => NumberType(v)
          }
          output_type.nullable = false
          makeNullable(te, Seq(output_type), qs.collect { case Gen(_, e1) => tipe(e1) })

        case BinaryExp(_, e1, e2) => makeNullable(te, Seq(), Seq(tipe(e1), tipe(e2)))
        case InExp(e1, e2)        => makeNullable(te, Seq(), Seq(tipe(e1), tipe(e2)))
        case UnaryExp(_, e1)      => makeNullable(te, Seq(), Seq(tipe(e1)))
        case ExpBlock(_, e1)      =>
          val t1 = tipe(e1)
          makeNullable(te, Seq(t1), Seq(t1))
        case _: IdnExp            => te
        case _: IntConst          => te
        case _: FloatConst        => te
        case _: BoolConst         => te
        case _: StringConst       => te
        case _: RecordCons        => te
        case _: ZeroCollectionMonoid => te
        case f: FunAbs    =>
          baseType(f) match {
            case FunType(t1, t2) => FunType(makeNullable(t1, Seq(), Seq(), Some(false)), tipe(f.e))
          }
        case _: Partition =>
          // TODO: Ben: HELP!!!
          // Ben: I think it should inherit the nullables the related froms, something like that?
          te
        case _: Star      =>
          // TODO: Ben: HELP!!!
          te
        case Sum(e1)      => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Max(e1)      => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Min(e1)      => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Avg(e1)      => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Count(e1)    => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Exists(e1)   => makeNullable(te, Seq(), Seq(tipe(e1)))
      }
      nt
    }
  }

  /** Type checker errors.
    */
  // TODO: Add check that the *root* type (and only the root type) does not contain ANY type variables, or we can't generate code for it
  // TODO: And certainly no NothingType as well...
  lazy val errors: Seq[Error] = {
    tipeEverything // Must type the entire program before checking for errors
    badEntities ++ tipeErrors
  }

  private lazy val collectBadEntities =
    collect[List, Error] {
      // Identifier declared more than once in the same scope
      case i: IdnDef if entity(i) == MultipleEntity() =>
        MultipleDecl(i)

      //      // Identifier used without being declared
      //      case i: IdnUse if entity(i) == UnknownEntity() =>
      //        UnknownDecl(i)
    }

  private lazy val badEntities = collectBadEntities(tree.root)

  /** Looks up the identifier in the World and returns a new entity instance.
    */
  private def lookupDataSource(idn: String): Entity =
    if (world.sources.contains(idn))
      DataSourceEntity(Symbol(idn))
    else
      UnknownEntity()

  /** The entity of an identifier.
    */
  lazy val entity: IdnNode => Entity = attr {
    case n @ IdnDef(idn) =>
      if (isDefinedInScope(env.in(n), idn))
        MultipleEntity()
      else
        VariableEntity(n, TypeVariable())
    case n @ IdnUse(idn) =>
      lookup(env.in(n), idn, lookupDataSource(idn))
  }

  /** Chain for looking up identifiers.
    */

  private lazy val env: Chain[Environment] =
    chain(envin, envout)

  private def envin(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()

    // Entering new scopes
    case c: Comp => enter(in(c))
    case b: ExpBlock => enter(in(b))

    // TODO: Refactor if Algebra node, open scope
    case r: Reduce    => enter(in(r))
    case f: Filter    => enter(in(f))
    case j: Join      => enter(in(j))
    case o: OuterJoin => enter(in(o))
    case o: OuterUnnest => enter(in(o))
    case n: Nest   => enter(in(n))
    case n: Nest2  => enter(in(n))
    case n: Nest3  => enter(in(n))
    case s: Select => enter(in(s))

    // If we are in a function abstraction, we must open a new scope for the variable argument. But if the parent is a
    // bind, then the `in` environment of the function abstraction must be the same as the `in` environment of the
    // bind.
    case tree.parent.pair(_: FunAbs, b: Bind) => enter(env.in(b))
    case f: FunAbs => enter(in(f))

    // If we are in an expression and the parent is a bind or a generator, then the `in` environment of the expression
    // is the same as that of the parent: the environment does not include the left hand side of the assignment.
    case tree.parent.pair(_: Exp, b: Bind) => env.in(b)
    case tree.parent.pair(_: Exp, g: Gen) => env.in(g)
  }

  private def envout(out: RawNode => Environment): RawNode ==> Environment = {
    // Leaving a scope
    case c: Comp => leave(out(c))
    case b: ExpBlock => leave(out(b))

    // TODO: Refactor if Algebra node, open scope
    case s: Select    => leave(out(s))
    case r: Reduce    => leave(out(r))
    case f: Filter    => leave(out(f))
    case j: Join      => leave(out(j))
    case o: OuterJoin => leave(out(o))
    case o: OuterUnnest => leave(out(o))
    case n: Nest  => leave(out(n))
    case n: Nest2 => leave(out(n))
    case n: Nest3 => leave(out(n))

    // The `out` environment of a function abstraction must remove the scope that was inserted.
    case f: FunAbs => leave(out(f))

    // A new variable was defined in the current scope.
    case n @ IdnDef(i) => define(out(n), i, entity(n))

    // The `out` environment of a bind or generator is the environment after the assignment.
    case Bind(p, _) => env(p)
    case g @ Gen(None, _) => env.in(g)
    case Gen(Some(p), _) => env(p)

    // Expressions cannot define new variables, so their `out` environment is always the same as their `in`
    // environment. The chain does not need to go "inside" the expression to finding any bindings.
    case e: Exp => env.in(e)
  }

  /** Chain for looking up aliases, which are autmaticaly inferred from anonymous generators.
    */

  // TODO Check recordtypes are never empty (record with no fields).

  private lazy val aliasEnv: Chain[Environment] =
    chain(aliasEnvIn, aliasEnvOut)

  private def aliasEnvIn(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()
    case c: Comp             => enter(in(c))
    case b: ExpBlock         => enter(in(b))
    case s: Select           => enter(in(s))
    case f: FunAbs           => enter(in(f))
    case a: LogicalAlgebraNode => enter(in(a))
  }

  private def aliasEnvOut(out: RawNode => Environment): RawNode ==> Environment = {
    case c: Comp     => leave(out(c))
    case b: ExpBlock => leave(out(b))
    case s: Select   => leave(out(s))
    case f: FunAbs   => leave(out(f))
    case a: LogicalAlgebraNode => leave(out(a))
    case g @ Gen(None, e) =>
      def attEntity(env: Environment, att: AttrType, idx: Int) = {
        if (isDefinedInScope(env, att.idn))
          MultipleEntity()
        else
          AttributeEntity(att, g, idx)
      }

      val t = expType(e)
      val nt = find(t)
      logger.debug(s"nt is ${PrettyPrinter(nt)}")
      nt match {
        case CollectionType(_, ResolvedType(RecordType(Attributes(atts)))) =>
          var nenv: Environment = out(g)
          for ((att, idx) <- atts.zipWithIndex) {
            nenv = define(nenv, att.idn, attEntity(nenv, att, idx))
          }
          nenv
        case _ =>
          aliasEnv.in(g)
      }
    case n                => aliasEnv.in(n)
  }

  /** Chain for looking up the partition keyword.
    * The partition scope opens on the projection but only if the SELECT has a GROUP BY.
    */

  private lazy val partitionEnv: Chain[Environment] =
    chain(partitionEnvIn, partitionEnvOut)

  private def partitionEnvIn(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()
    case tree.parent.pair(e: Exp, s: Select) if (e eq s.proj) && s.group.isDefined =>
      val env = enter(in(e))
      define(env, "partition", PartitionEntity(s, TypeVariable()))
  }

  private def partitionEnvOut(out: RawNode => Environment): RawNode ==> Environment = {
    case tree.parent.pair(e: Exp, s: Select) if (e eq s.proj) && s.group.isDefined =>
      leave(out(e))
  }

  /** Chain for looking up the star keyword.
    * The star scope opens on the projection for SELECT or the yield for for-comprehensions.
    */

  private lazy val starEnv: Chain[Environment] =
    chain(starEnvIn, starEnvOut)

  private def starEnvIn(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()
    case tree.parent.pair(e: Exp, s: Select) if e eq s.proj =>
      val env = enter(in(e))
      define(env, "*", StarEntity(s, TypeVariable()))

    case tree.parent.pair(e: Exp, c: Comp) if e eq c.e =>
      // TODO: In the case of a Comp make sure there is at least a Gen or this doesn't make sense!!!! Same on envOut.
      val env = enter(in(e))
      define(env, "*", StarEntity(c, TypeVariable()))
  }

  private def starEnvOut(out: RawNode => Environment): RawNode ==> Environment = {
    case tree.parent.pair(e: Exp, s: Select) if e eq s.proj =>
      leave(out(e))
    case tree.parent.pair(e: Exp, c: Comp) if e eq c.e =>
      leave(out(e))
  }

  /** lookup up attribute entity.
    */
  lazy val lookupAttributeEntity: IdnExp => Entity = attr {
    // TODO: Why aliasEnv.in and not aliasEnv only ????
    idnExp => lookup(aliasEnv.in(idnExp), idnExp.idn.idn, UnknownEntity())
  }

  /** lookup up partition entity.
    */
  lazy val partitionEntity: Partition => Entity = attr {
    e => lookup(partitionEnv.in(e), "partition", UnknownEntity())
  }

  /** lookup up star entity.
    */
  lazy val starEntity: Star => Entity = attr {
    e => {
      logger.debug(s"we get here from ${CalculusPrettyPrinter(e)}");
      lookup(starEnv.in(e), "*", UnknownEntity())
    }
  }

  /////


  //////

  // TODO: Move this to the Types.scala and have it used inside the TypeScheme definition for uniformity!
  case class FreeSymbols(typeSyms: Set[Symbol], monoidSyms: Set[Symbol], attSyms: Set[Symbol])

  /** Type the rhs of a Bind declaration.
    * If successful, returns a list of free type symbols and free monoid symbols (for polymorphism).
    */
  private lazy val tipeBind: Bind => Option[FreeSymbols] = attr {
    case Bind(p, e) =>
      def aux: Option[FreeSymbols] = {
        // TODO: If the unresolved TypeVariables come from UserType/Source, don't include them as free variables.
        //       Instead, leave them unresolved, unless we want a strategy that resolves them based on usage?

        // Add all pattern identifier types to the map before processing the rhs
        // This call is repeated multiple times in case of a PatternProd on the lhs of the Bind. This is harmless.
        //        patternIdnTypes(p).foreach { case pt => typesVarMap.union(pt, pt) }

        // Collect all the roots known in the TypesVarMap.
        // This will be used to detect "new variables" created within, and not yet in the TypesVarMap.
        val prevTypeRoots = typesVarMap.getRoots
        val prevMonoidRoots = monoidRoots() // monoidsVarMap.getRoots
        val prevRecAttRoots = recAttsVarMap.getRoots

        // Type the rhs body of the Bind
        solve(constraints(e))
        val t = expType(e)
        val expected = patternType(p)
        if (!unify(t, expected)) {
          tipeErrors += PatternMismatch(p, walk(t), Some(p.pos))
          return None
        }

        //        // find all parameter types of inner funabs
        //
        //        val lambda_ptypes = collect[Set, Set[Type]] {
        //          case FunAbs(p, _) => patternIdnTypes(p).toSet
        //        }
        //
        //        val to_walk: Set[Type] = lambda_ptypes(e).flatMap(identity)
        //        val typeVars = to_walk.flatMap(getVariableTypes(_))
        //        val monoidVars = to_walk.flatMap(getVariableMonoids(_))
        //        val attVars = to_walk.flatMap(getVariableAtts(_))
        //        logger.debug(s"${CalculusPrettyPrinter(e)} => types are ${typeVars.map(PrettyPrinter(_))}, eType is ${PrettyPrinter(walk(t))}")

        // we should clone all monoids that were recently created and were not in the map before
        // so indeed, i could walk the old map
        // and, extract all its roots.
        // actually, now whenever i create

        // so i could collect ALL - really, all - monoid (roots) (it's basically keys of monoidProperties)
        // i do the same after
        // and, well, regardless of them being in the type of not, i should clone it all (?)
        // i should clone the ones athat are independent of what was in the map before...

        // BTW, when I create a freshVar, ADD IT TO THE MAP.


        // Find all type variables used in the type
        val typeVars = getVariableTypes(t)
        //        logger.debug(s"typeVars = ")
        //        val monoidVars = getVariableMonoids(t)
        val attVars = getVariableAtts(t)

        // go to the old roots
        // extract its type variables <-- this is new
        // walk thm in the new map: those are the new roots
        //

        // i think the find should be cloning all thnigs internally


        //        so we must clone all things that are new and that are somehow related to this type
        //        but if they are new they are somehow related to this body
        //        so, basically, it's all things new, period.


        // For all the "previous roots", get their new roots
        val prevTypeRootsUpdated = prevTypeRoots.flatMap { case v => getVariableTypes(v) }.map { case v => find(v) }
        val prevMonoidRootsUpdated = prevMonoidRoots.map { case v => mFind(v) }
        val prevRecAttRootsUpdated = prevRecAttRoots.map { case v => aFind(v) }

        // TODO QUESTION TO BEN: why typeVars? why not walk ALL THE ROOTs? arent these things somehow needed/related?

        // Collect all symbols from variable types that were not in the maps before we started typing the body of the Bind.
        val freeTypeSyms = typeVars.filter { case vt => !prevTypeRootsUpdated.contains(vt) }.map(_.sym)
        val freeMonoidSyms = monoidRoots().collect { case v: MonoidVariable => v }.filter { case v => !prevMonoidRootsUpdated.contains(v) }.map(_.sym)
        val freeAttSyms = attVars.filter { case v => !prevRecAttRootsUpdated.contains(v) }.map(_.sym)

        Some(FreeSymbols(freeTypeSyms, freeMonoidSyms, freeAttSyms))
      }
      aux
  }

  /** Return the sequence of types in a pattern.
    */
  private lazy val patternIdnTypes: Pattern => Seq[Type] = attr {
    p => {
      def tipes(p: Pattern): Seq[Type] = p match {
        case PatternIdn(idn) => entity(idn) match {
          case VariableEntity(_, t) => Seq(t)
          case _ => Nil
        }
        case PatternProd(ps) => ps.flatMap(tipes)
      }
      tipes(p)
    }
  }

  private def getVariableTypes(t: Type, occursCheck: Set[Type] = Set()): Set[VariableType] = {
    if (occursCheck.contains(t))
      Set()
    else {
      t match {
        case _: NothingType               => Set()
        case _: AnyType                   => Set()
        case _: BoolType                  => Set()
        case _: IntType                   => Set()
        case _: FloatType                 => Set()
        case _: StringType                => Set()
        case _: UserType                  => Set()
        case RecordType(r)     =>
          aFind(r) match {
            case a: Attributes => a.atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) }.toSet
            case a: AttributesVariable => a.atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) }
            case c: ConcatAttributes =>
              val cdef = concatDefinition(c)
              cdef.atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) } ++
                cdef.slotsSet.flatMap { case slots => slots.flatMap { case ConcatSlot(_, t1) => getVariableTypes(t1, occursCheck + t)}}
          }

        //        case RecordType(recAtts)          => recAtts.atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) }.toSet
        case PatternType(atts)            => atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) }.toSet
        case CollectionType(_, innerType) => getVariableTypes(innerType, occursCheck + t)
        case FunType(p, e)                => getVariableTypes(p, occursCheck + t) ++ getVariableTypes(e, occursCheck + t)
        case t1: PrimitiveType            => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) getVariableTypes(typesVarMap(t1).root, occursCheck + t) else Set(t1)
        case t1: NumberType               => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) getVariableTypes(typesVarMap(t1).root, occursCheck + t) else Set(t1)
        case t1: TypeVariable             => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) getVariableTypes(typesVarMap(t1).root, occursCheck + t) else Set(t1)
      }
    }
  }

  private def getVariableAtts(t: Type, occursCheck: Set[Type] = Set()): Set[VariableAttributes] = {
    if (occursCheck.contains(t))
      Set()
    else {
      t match {
        case _: NothingType    => Set()
        case _: AnyType        => Set()
        case _: BoolType       => Set()
        case _: IntType        => Set()
        case _: FloatType      => Set()
        case _: StringType     => Set()
        case _: UserType       => Set()
        case _: PrimitiveType  => Set()
        case _: NumberType     => Set()
        case t1: TypeVariable  =>
          if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1)
            getVariableAtts(typesVarMap(t1).root, occursCheck + t)
          else
            Set()
        case RecordType(r)     =>
          aFind(r) match {
            case a: Attributes => a.atts.flatMap { case att => getVariableAtts(att.tipe, occursCheck + t) }.toSet
            case a: AttributesVariable => Set(a) ++ a.atts.flatMap { case att => getVariableAtts(att.tipe, occursCheck + t) }
            case c: ConcatAttributes =>
              val cdef = concatDefinition(c)
              Set(c) ++
                cdef.atts.flatMap { case att => getVariableAtts(att.tipe, occursCheck + t) } ++
                cdef.slotsSet.flatMap { case slots => slots.flatMap { case ConcatSlot(_, t1) => getVariableAtts(t1, occursCheck + t)}}
          }
        case PatternType(atts) => atts.flatMap { case att => getVariableAtts(att.tipe, occursCheck + t) }.toSet
        case CollectionType(_, innerType) => getVariableAtts(innerType, occursCheck + t)
        case FunType(p, e) => getVariableAtts(p, occursCheck + t) ++ getVariableAtts(e, occursCheck + t)
      }
    }
  }


  /** Return the declaration of an identifier definition.
    */
  protected lazy val decl: IdnDef => Option[Decl] = attr {
    case idn: IdnDef =>
      def getDecl(n: RawNode): Option[Decl] = n match {
        case b: Bind                 => Some(b)
        case g: Gen                  => Some(g)
        case f: FunAbs               => Some(f)
        case tree.parent.pair(_, n1) => getDecl(n1)
        case _                       => None
      }
      getDecl(idn)
  }

  // TODO: Refactor decl vs patDecl: there's repetition

  /** Return the declaration of a pattern.
    */
  protected lazy val patDecl: Pattern => Option[Decl] = attr {
    case p: Pattern =>
      def getDecl(n: RawNode): Option[Decl] = n match {
        case b: Bind                 => Some(b)
        case g: Gen                  => Some(g)
        case f: FunAbs               => Some(f)
        case tree.parent.pair(_, n1) => getDecl(n1)
        case _                       => None
      }
      getDecl(p)
  }

  /** Return the type of an entity.
    * Supports let-polymorphism.
    */
  private lazy val entityType: Entity => Type = attr {
    case VariableEntity(idn, t) =>
      decl(idn) match {
        case Some(b: Bind) => tipeBind(b) match {
          case Some(FreeSymbols(typeSyms, monoidSyms, attSyms)) =>
            val ts = TypeScheme(t, typeSyms, monoidSyms, attSyms)
            //              logger.debug(s"TypeScheme is ${PrettyPrinter(ts)} for ${CalculusPrettyPrinter(b)}")
            ts
          case None => NothingType()
        }
        case _             => t
      }
    case DataSourceEntity(sym)  => world.sources(sym.idn)
    case _: UnknownEntity       => NothingType()
    case _: MultipleEntity      => NothingType()
  }

  /** Instantiate a new type from a type scheme.
    * Used for let-polymorphism.
    * This method is only called if there are type variables, monoid variables or attribute variables.
    * In that case, a new type must be constructed.
    * Note that variable types/monoids whose symbols are not in typeSyms/monoidSyms are not reconstructed and the same
    * object is used to allow unification to proceed unaffected.
    */
  private def instantiateTypeScheme(t: Type, typeSyms: Set[Symbol], monoidSyms: Set[Symbol], attSyms: Set[Symbol]) = {
    logger.debug(s"instantiateTypeScheme called with ${PrettyPrinter(t)} typeSyms $typeSyms monoidSyms $monoidSyms attSyms $attSyms")

    val newSyms = scala.collection.mutable.HashMap[Symbol, Symbol]()

    def getNewSym(sym: Symbol): Symbol = {
      if (!newSyms.contains(sym))
        newSyms += (sym -> SymbolTable.next())
      newSyms(sym)
    }

    val newMonoidSyms = cloneMonoids(monoidSyms)

    def getMonoid(m: Monoid, moccursCheck: Set[Monoid] = Set()): Monoid =
      mFind(m) match {
        case MonoidVariable(sym) if newMonoidSyms.contains(sym) => MonoidVariable(newMonoidSyms(sym))
        case _ => m
      }


    // TODO: DO WE NEED occursCheck HERE?????
    // TODO: And do we even need it in the main method???
    def getAttributes(m: RecordAttributes, occursCheck: Set[Type]): RecordAttributes = {
      val ar = if (recAttsVarMap.contains(m)) recAttsVarMap(m).root else m
      ar match {
        case AttributesVariable(atts, sym) =>
          assert(attSyms.contains(sym))
          AttributesVariable(atts.map { case att => AttrType(att.idn, recurse(att.tipe, occursCheck)) }, getNewSym(sym))
        case c @ ConcatAttributes(sym) =>
          assert(attSyms.contains(sym))
          val cdef = concatDefinition(c)
          val natts = cdef.atts.map { case att => AttrType(att.idn, recurse(att.tipe, occursCheck)) }
          val nslotsSet = cdef.slotsSet.map { case slots => slots.map { case ConcatSlot(prefix, t1) => ConcatSlot(prefix, recurse(t1, occursCheck))}}
          val nc = ConcatAttributes()
          freshConcat(nc, natts, nslotsSet)
          nc
        case Attributes(atts) =>
          Attributes(atts.map { case att => AttrType(att.idn, recurse(att.tipe, occursCheck)) })
      }
    }

    def recurse(t: Type, occursCheck: Set[Type]): Type = {
      if (occursCheck.contains(t))
        t
      else {
        t match {
          case t1 @ TypeVariable(sym) if !typeSyms.contains(sym)  => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) recurse(typesVarMap(t1).root, occursCheck + t) else t1
          case t1 @ NumberType(sym) if !typeSyms.contains(sym)    => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) recurse(typesVarMap(t1).root, occursCheck + t) else t1
          case t1 @ PrimitiveType(sym) if !typeSyms.contains(sym) => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) recurse(typesVarMap(t1).root, occursCheck + t) else t1
          case TypeVariable(sym)             => TypeVariable(getNewSym(sym))
          case NumberType(sym)               => NumberType(getNewSym(sym))
          case PrimitiveType(sym)            => PrimitiveType(getNewSym(sym))
          case _: NothingType                => t
          case _: AnyType                    => t
          case _: IntType                    => t
          case _: BoolType                   => t
          case _: FloatType                  => t
          case _: StringType                 => t
          case _: UserType                   => t
          case RecordType(recAtts)           => RecordType(getAttributes(recAtts, occursCheck + t))
          case PatternType(atts)             => PatternType(atts.map { case att => PatternAttrType(recurse(att.tipe, occursCheck + t)) })
          case c1 @ CollectionType(m, inner) =>
            val nm = getMonoid(m)
            CollectionType(nm.asInstanceOf[CollectionMonoid], recurse(inner, occursCheck + t))
          case FunType(p, e)                 =>
            FunType(recurse(p, occursCheck + t), recurse(e, occursCheck + t))
        }
      }
    }

    recurse(t, Set())
  }

  private def idnType(idn: IdnNode): Type = entityType(entity(idn))

  /** The type corresponding to a given pattern.
    */
  private lazy val patternType: Pattern => Type = attr {
    case PatternIdn(idn) => entity(idn) match {
      case VariableEntity(_, t) => t
      case _                    => NothingType()
    }
    case PatternProd(ps) => PatternType(ps.map { case p => PatternAttrType(patternType(p)) })
  }

  /** The type of an expression.
    * If the type cannot be immediately derived from the expression itself, then type variables are used for unification.
    */
  private lazy val expType: Exp => Type = attr {

    case p: Partition => partitionType(partitionEntity(p))

    case s: Star => starType(starEntity(s))

    // Rule 1
    case _: BoolConst  => BoolType()
    case _: IntConst   => IntType()
    case _: FloatConst => FloatType()
    case _: StringConst => StringType()

    // Rule 5
    case RecordCons(atts) => RecordType(Attributes(atts.map(att => AttrType(att.idn, expType(att.e)))))

    // Rule 9
    case ZeroCollectionMonoid(_: BagMonoid) => CollectionType(BagMonoid(), TypeVariable())
    case ZeroCollectionMonoid(_: ListMonoid) => CollectionType(ListMonoid(), TypeVariable())
    case ZeroCollectionMonoid(_: SetMonoid) => CollectionType(SetMonoid(), TypeVariable())
    case MultiCons(_: BagMonoid, Nil)       => CollectionType(BagMonoid(), TypeVariable())
    case MultiCons(_: ListMonoid, Nil)      => CollectionType(ListMonoid(), TypeVariable())
    case MultiCons(_: SetMonoid, Nil)       => CollectionType(SetMonoid(), TypeVariable())

    // Rule 10
    case ConsCollectionMonoid(_: BagMonoid, e1) => CollectionType(BagMonoid(), expType(e1))
    case ConsCollectionMonoid(_: ListMonoid, e1) => CollectionType(ListMonoid(), expType(e1))
    case ConsCollectionMonoid(_: SetMonoid, e1) => CollectionType(SetMonoid(), expType(e1))
    case MultiCons(_: BagMonoid, e1 :: Nil)     => CollectionType(BagMonoid(), expType(e1))
    case MultiCons(_: ListMonoid, e1 :: Nil)    => CollectionType(ListMonoid(), expType(e1))
    case MultiCons(_: SetMonoid, e1 :: Nil)     => CollectionType(SetMonoid(), expType(e1))

    // Unary expressions
    case UnaryExp(_: Not, _)     => BoolType()
    case UnaryExp(_: ToBool, _)  => BoolType()
    case UnaryExp(_: ToInt, _)   => IntType()
    case UnaryExp(_: ToFloat, _) => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    // Sugar expressions
    case _: Count => IntType()
    case _: Exists => BoolType()

    case n => TypeVariable()
  }

  ////

  case class ConcatSlot(prefix: String, t: Type)

  type ConcatSlots = Seq[ConcatSlot]

  case class ConcatDefinition(atts: Set[AttrType], slotsSet: Set[ConcatSlots])

  private val concatDefinition = scala.collection.mutable.HashMap[ConcatAttributes, ConcatDefinition]()

  private def freshConcat(c: ConcatAttributes, atts: Set[AttrType] = Set(), slotsSet: Set[ConcatSlots] = Set()) = {
    assert(!concatDefinition.contains(c))
    concatDefinition.put(c, ConcatDefinition(atts, slotsSet))
  }

  private case class ConcatProperties(atts: Seq[AttrType], isComplete: Boolean)

  private def getConcatProperties(c: ConcatAttributes): ConcatProperties = {

    // maybe this guy should walk and narrow first
    // because then it may find that, hey, i'm actually complete now

    def resolve(slots: ConcatSlots): ConcatProperties = {

      val usedIdns = scala.collection.mutable.Set[Idn]()

      def uniqueIdn(i: Idn, j: Int = 0): Idn = {
        val ni = if (j == 0 && i.nonEmpty) i else if (i.isEmpty) s"_${j + 1}" else s"${i}_$j"
        if (usedIdns.contains(ni))
          uniqueIdn(i, j + 1)
        else {
          usedIdns += ni
          ni
        }
      }

      val resolvedAtts = scala.collection.mutable.ListBuffer[AttrType]()
      logger.debug(s"slots are $slots")
      for (s <- slots) {
        logger.debug(s"s.t ${PrettyPrinter(s.t)} find(s.t) ${PrettyPrinter(find(s.t))}")
        find(s.t) match {
          case ResolvedType(RecordType(recAtts)) =>
            aFind(recAtts) match {
              case Attributes(atts)      =>
                logger.debug(s".. $atts")
                for (att <- atts) {
                  resolvedAtts += AttrType(uniqueIdn(att.idn), att.tipe)
                }
              case _: AttributesVariable =>
                // Return early since we cannot know the sequence of attributes from the beginning
                return ConcatProperties(resolvedAtts.to, false)
              case c1: ConcatAttributes  =>
                val props1 = getConcatProperties(c1)
                for (att <- props1.atts) {
                  resolvedAtts += AttrType(uniqueIdn(att.idn), att.tipe)
                }
                if (!props1.isComplete) {
                  return ConcatProperties(resolvedAtts.to, false)
                }
            }
          case ResolvedType(_: TypeVariable) =>
            // Return early since we don't know if this type variable will resolve to a record or other type
            return ConcatProperties(resolvedAtts.to, false)
          case t                                               =>
            resolvedAtts += AttrType(uniqueIdn(s.prefix), t)
        }
      }
      ConcatProperties(resolvedAtts.to, true)
    }

    var mostPrecise: ConcatProperties = null
    for (slots <- concatDefinition(c).slotsSet) {
      val props = resolve(slots)
      if (props.isComplete) {
        logger.debug(s"getConcatProperties complete $props")
        return props
      }
      if (mostPrecise == null || props.atts.length > mostPrecise.atts.length) {
        mostPrecise = props
      }
    }
    logger.debug(s"getConcatProperties mostPrecise $mostPrecise")
    mostPrecise
  }

  private val recAttsVarMap = new RecordAttributesVarMap()

  private def unifyAttributes(a: RecordAttributes, b: RecordAttributes, occursCheck: Set[(Type, Type)]): Boolean = {
    logger.debug(s"unifyAttributes a ${PrettyPrinter(a)} b ${PrettyPrinter(b)}")
    (aFind(a), aFind(b)) match {
      case (Attributes(atts1), Attributes(atts2))                             =>
        if (atts1.length == atts2.length && atts1.map(_.idn) == atts2.map(_.idn))
          atts1.zip(atts2).map { case (att1, att2) => unify(att1.tipe, att2.tipe, occursCheck) }.forall(identity)
        else
          false
      case (AttributesVariable(atts1, sym1), AttributesVariable(atts2, sym2)) =>
        val commonIdns = atts1.map(_.idn).intersect(atts2.map(_.idn))
        for (idn <- commonIdns) {
          val att1 = a.getType(idn).head
          val att2 = b.getType(idn).head
          if (!unify(att1, att2, occursCheck)) {
            return false
          }
        }
        if (commonIdns.size == atts1.size && commonIdns.size == atts2.size) {
          recAttsVarMap.union(a, b)
          true
        } else {
          // TODO: We seem to create too many AttributesVariable...
          val commonAttrs = commonIdns.map { case idn => AttrType(idn, a.getType(idn).head) } // Safe to take from the first attribute since they were already unified in the new map
          val na = AttributesVariable(atts1.filter { case att => !commonIdns.contains(att.idn) } ++ atts2.filter { case att => !commonIdns.contains(att.idn) } ++ commonAttrs, SymbolTable.next())
          recAttsVarMap.union(a, b).union(b, na)
          true
        }
      case (AttributesVariable(atts1, _), Attributes(atts2))                  =>
        if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet)) {
          false
        } else {
          for (att1 <- atts1) {
            if (!unify(att1.tipe, b.getType(att1.idn).get, occursCheck)) {
              return false
            }
          }
          recAttsVarMap.union(a, b)
          true
        }
      case (_: Attributes, _: AttributesVariable)                             =>
        unifyAttributes(b, a, occursCheck)
      //      case (_: ConcatAttributes, _: ConcatAttributes) if a == b =>
      //        true
      case (ca: ConcatAttributes, cb: ConcatAttributes)          =>
        // check if their beginnings are incompatible
        logger.debug("a1")
        val propsA = getConcatProperties(ca)
        logger.debug("a2")
        val propsB = getConcatProperties(cb)
        logger.debug("a3")
        logger.debug(s"propsA.atts ${propsA.atts} propsB.atts ${propsB.atts}")
        if (!propsA.atts.zip(propsB.atts).map { case (att1, att2) => unify(att1.tipe, att2.tipe, occursCheck) }.forall(identity)) {
          return false
        }
        // check if their common attribute variables are incompatible
        logger.debug("a4")
        val defCa = concatDefinition(ca)
        logger.debug("a5")
        val defCb = concatDefinition(cb)
        logger.debug("a6")
        val commonIdns = defCa.atts.map(_.idn).intersect(defCb.atts.map(_.idn))
        logger.debug("a7")
        for (idn <- commonIdns) {
          val att1 = a.getType(idn).head
          val att2 = b.getType(idn).head
          if (!unify(att1, att2, occursCheck)) {
            return false
          }
        }
        logger.debug("a1")
        // all checks ok, so can unify
        val c = ConcatAttributes()
        freshConcat(c, defCa.atts ++ defCb.atts, defCa.slotsSet ++ defCb.slotsSet)
        recAttsVarMap.union(a, b).union(b, c)
        true
      case (ca: ConcatAttributes, Attributes(atts1))            =>
        val props = getConcatProperties(ca)
        // if complete, check if they are the same length
        if (props.isComplete && props.atts.length != atts1.length) {
          return false
        }
        // check if the beginning matches
        for ((att1, att2) <- props.atts.zip(atts1)) {
          if (att1.idn != att2.idn || !unify(att1.tipe, att2.tipe, occursCheck)) {
            return false
          }
        }
        // check if the variable attributes match
        val defCa = concatDefinition(ca)
        if (!defCa.atts.map(_.idn).subsetOf(atts1.map(_.idn).toSet)) {
          false
        } else {
          for (att <- defCa.atts) {
            if (!unify(att.tipe, b.getType(att.idn).get, occursCheck)) {
              return false
            }
          }
        }
        // all checks ok, so can unify
        recAttsVarMap.union(ca, b)
        true
      case (_: Attributes, _: ConcatAttributes)                =>
        unifyAttributes(b, a, occursCheck)
      case (ca: ConcatAttributes, AttributesVariable(atts1, _)) =>
        val defCa = concatDefinition(ca)
        // check if the overlapping names match
        val commonIdns = defCa.atts.map(_.idn).intersect(atts1.map(_.idn))
        for (idn <- commonIdns) {
          val att1 = a.getType(idn).head
          val att2 = b.getType(idn).head
          if (!unify(att1, att2, occursCheck)) {
            return false
          }
        }
        // check if the names are already present in the beginning, and if so, their types match
        val propsCa = getConcatProperties(ca)
        for (att <- atts1) {
          // find the variable attribute in the concats list
          val catt = propsCa.atts.collectFirst { case catt if catt.idn == att.idn => catt }
          if (catt.isEmpty && propsCa.isComplete) {
            // the variable attribute is not in the concats, even though the concat is complete
            return false
          } else if (catt.isDefined && !unify(catt.get.tipe, att.tipe)) {
            // the variable attribute is in the concats but the types don't unify
            return false
          }
        }
        // all ok, so can union
        val c = ConcatAttributes()
        freshConcat(c, defCa.atts ++ atts1, defCa.slotsSet)
        recAttsVarMap.union(a, b).union(b, c)
        true
      case (_: AttributesVariable, _: ConcatAttributes)        =>
        unifyAttributes(b, a, occursCheck)
    }
  }

  def logConcatProperties() = {
    logger.debug(s"Concat attributes map:\n${recAttsVarMap.toString}")
    logger.debug(concatDefinition.toString)
    logger.debug(
      s"Concat attributes details:\n" +
        concatDefinition.map {
          case (c, cdef) =>
            val props = getConcatProperties(c)
            if (props.isComplete) {
              s"${PrettyPrinter(c)}(${PrettyPrinter(Attributes(props.atts.map{ case att => AttrType(att.idn, walk(att.tipe))}))})"
            } else {
              val natts = cdef.atts.map { case AttrType(idn1, t1) => AttrType(idn1, walk(t1)) }
              val nslotsSet = cdef.slotsSet.map(_.map { case ConcatSlot(prefix, t1) => ConcatSlot(prefix, walk(t1))})
              s"${PrettyPrinter(c)}(atts=${PrettyPrinter(AttributesVariable(natts))})(slotsSet=$nslotsSet)"
            }
        }.mkString("\n"))
  }

  // ah ha! at every unify, i can / should replace it then, otherwise, only at the end (which anyway is fine, but just seems more work(?))

  // that could be what the auto-propagate could be about
  // after every unify - of any type,
  // i'd try to replace a root concat attributes with smtg more specific
  // all in all, they are equivalent

  //////

  /** Hindley-Milner unification algorithm.
    */
  def unify(t1: Type, t2: Type, occursCheck: Set[(Type, Type)] = Set()): Boolean = {
    //      logger.debug(s"   Unifying t1 ${TypesPrettyPrinter(t1)} and t2 ${TypesPrettyPrinter(t2)}")
    if (occursCheck.contains((t1, t2))) {
      return true
    }
    val nt1 = find(t1)
    val nt2 = find(t2)
    (nt1, nt2) match {
      // TODO: Add NothingType

      case (_: AnyType, t) =>
        true
      case (t, _: AnyType) =>
        true

      case (_: IntType, _: IntType)     =>
        true
      case (_: BoolType, _: BoolType)   =>
        true
      case (_: FloatType, _: FloatType) =>
        true
      case (_: StringType, _: StringType) =>
        true

      case (CollectionType(m1, inner1), CollectionType(m2, inner2)) =>
        logger.debug(s"unifyMonoids of ${PrettyPrinter(m1)} and ${PrettyPrinter(m2)}")
        unifyMonoids(m1, m2) && unify(inner1, inner2, occursCheck + ((t1, t2)))

      case (FunType(p1, e1), FunType(p2, e2)) =>
        unify(p1, p2, occursCheck + ((t1, t2))) && unify(e1, e2, occursCheck + ((t1, t2)))

      case (RecordType(a1), RecordType(a2)) =>
        unifyAttributes(a1, a2, occursCheck + ((t1, t2)))

      case (PatternType(atts1), PatternType(atts2)) =>
        if (atts1.length == atts2.length)
          atts1.zip(atts2).map { case (att1, att2) => unify(att1.tipe, att2.tipe, occursCheck + ((t1, t2))) }.forall(identity)
        else
          false

      case (PatternType(atts1), RecordType(Attributes(atts2))) =>
        if (atts1.length == atts2.length)
          atts1.zip(atts2).map { case (att1, att2) => unify(att1.tipe, att2.tipe, occursCheck + ((t1, t2))) }.forall(identity)
        else
          false

      case (RecordType(Attributes(atts1)), PatternType(atts2)) =>
        unify(t2, t1, occursCheck + ((t1, t2)))

      case (p1: PrimitiveType, p2: PrimitiveType) =>
        typesVarMap.union(p2, p1)
        true
      case (p1: PrimitiveType, _: BoolType)   =>
        typesVarMap.union(p1, nt2)
        true
      case (p1: PrimitiveType, _: IntType)    =>
        typesVarMap.union(p1, nt2)
        true
      case (p1: PrimitiveType, _: FloatType)  =>
        typesVarMap.union(p1, nt2)
        true
      case (p1: PrimitiveType, _: StringType) =>
        typesVarMap.union(p1, nt2)
        true
      case (_: BoolType, _: PrimitiveType)    =>
        unify(nt2, nt1, occursCheck + ((t1, t2)))
      case (_: IntType, _: PrimitiveType)     =>
        unify(nt2, nt1, occursCheck + ((t1, t2)))
      case (_: FloatType, _: PrimitiveType)   =>
        unify(nt2, nt1, occursCheck + ((t1, t2)))
      case (_: StringType, _: PrimitiveType)  =>
        unify(nt2, nt1, occursCheck + ((t1, t2)))

      case (p1: NumberType, p2: NumberType) =>
        typesVarMap.union(p2, p1)
        true
      case (p1: NumberType, _: FloatType) =>
        typesVarMap.union(p1, nt2)
        true
      case (p1: NumberType, _: IntType)   =>
        typesVarMap.union(p1, nt2)
        true
      case (_: FloatType, _: NumberType)  =>
        unify(nt2, nt1, occursCheck + ((t1, t2)))
      case (_: IntType, _: NumberType)    =>
        unify(nt2, nt1, occursCheck + ((t1, t2)))

      case (UserType(sym1), UserType(sym2)) if sym1 == sym2 =>
        true
      case (v1: TypeVariable, v2: TypeVariable) =>
        typesVarMap.union(v2, v1)
        true
      case (v1: TypeVariable, v2: VariableType) =>
        typesVarMap.union(v1, v2)
        true
      case (v1: VariableType, v2: TypeVariable) =>
        unify(v2, v1, occursCheck + ((t1, t2)))
      case (v1: TypeVariable, _)                =>
        typesVarMap.union(v1, nt2)
        true
      case (_, v2: TypeVariable)                =>
        unify(v2, nt1, occursCheck + ((t1, t2)))
      case _                                    =>
        false
    }
  }

  /** This creates a monoid variable... comment it later :) it's used to postpone unification later: see MaxOfMonoid
    * where unifyMonoids is copied with the *NEW VARIABLE* we created here
    */
  private def maxMonoid(ts: Seq[CollectionType]): CollectionMonoid = {
    val nm = MonoidVariable()
    ts.map(_.m).map { case m => addMonoidOrder(m, nm) }
    nm
  }

  /** The type of a partition in a given SELECT.
    * Examples:
    *   SELECT age, PARTITION FROM students GROUP BY age
    *   The type of the SELECT is list(age, list[student])
    *
    *   SELECT age, PARTITION FROM students S GROUP BY age
    *   The type of the SELECT is (same as before): list(age, list[student])
    *
    *   SELECT age, PARTITION FROM students, professors WHERE student_age = professor_age GROUP BY student_age
    *   The type of the SELECT is list(age, list[(_1: student, _2: professor)])
    *
    *   SELECT age, PARTITION FROM students S, professors WHERE s.student_age = professor_age GROUP BY student_age
    *   The type of the SELECT is list(age, list[(s: student, _2: professor)])
    */
  private lazy val selectPartitionType: Select => Type = attr {
    s =>
      def aux: Type = {
        val fromTypes = s.from.map {
          case Gen(_, e) =>
            val t = expType(e)
            find(t) match {
              case t1: CollectionType => t1
              case _ => return NothingType()
            }
        }
        val ninner =
          if (fromTypes.length == 1) {
            fromTypes.head.innerType
          } else {
            val idns = s.from.zipWithIndex.map {
              case (Gen(Some(PatternIdn(IdnDef(idn))), _), _) => idn
              case (Gen(None, _), i)                          => s"_${i + 1}"
            }
            assert(idns.toSet.size == idns.length) // TODO: Making sure that user PatternIdns do not match _1, _2 by accident!
            RecordType(Attributes(idns.zip(fromTypes.map(_.innerType)).map { case (idn, innerType) => AttrType(idn, innerType) }))
          }
        CollectionType(maxMonoid(fromTypes), ninner)
      }
    aux
  }

  /** The type of a * in a given SELECT.
    * Examples:
    *   SELECT * FROM students
    *   The type of the SELECT is list[student]
    *
    *   SELECT age, * FROM students
    *   Reports an error (it's too dangerous to multiply the table by itself)
    *
    *   SELECT age, * FROM students GROUP BY age
    *   The type of the SELECT is list(age, list[student])
    *
    *   SELECT age, * FROM students S GROUP BY age
    *   The type of the SELECT is (same as before): list(age, list[student])
    *
    *   SELECT * FROM students GROUP BY age
    *   The type of the SELECT is list[list[student]]
    *
    *   SELECT * FROM students, professors
    *   The type of the SELECT is list(... student ... professor ...)
    *   If field names are shared in the inner tables, these are renamed by prefixing an auto-generated name.
    *
    *   SELECT * FROM students S, professors P
    *   The type of the SELECT is list(... student ... professor ...)
    *   If field names are shared in the inner tables, these are renamed by prefixing the generator name.
    *
    *   SELECT age, * FROM students, professors WHERE student_age = professor_age GROUP BY student_age
    *   The type of the SELECT is (age, list[... student ... professor ...])
    *   (Same field renaming policy applies as above)
    *
    *   SELECT age, PARTITION FROM students S, professors WHERE s.student_age = professor_age GROUP BY student_age
    *   The type of the SELECT is (age, list[... student ... professor ...])
    *   (Same field renaming policy applies as above)
    */
  private lazy val selectStarType: Select => Type = attr {
    s =>
      def aux: Type = {
        val fromTypes: Seq[CollectionType] = s.from.map {
          case Gen(_, e) =>
            val t = expType(e)
            find(t) match {
              case t1: CollectionType => t1
              case _ => logger.debug("selectStarType found Nothing"); return NothingType()
            }
        }

        s.proj match {
          case _: Star if s.group.isEmpty && fromTypes.length == 1 =>
            // SELECT * FROM students
            return fromTypes.head.innerType
          case _ if s.group.isDefined && fromTypes.length == 1 =>
            // SELECT * FROM students GROUP BY age
            return CollectionType(maxMonoid(fromTypes), fromTypes.head.innerType)
          case _ =>
        }

        if (s.proj != Star() && s.group.isEmpty) {
          // SELECT age, * FROM students
          tipeErrors += IllegalStar(s)
          return NothingType()
        }

        if (s.from.length == 1) {
          //   SELECT age, * FROM students GROUP BY age
          // we know group by is defined otherwise we would have terminated earlier
          // we don't want to make a concat for that (we don't concatenate actually)
          CollectionType(maxMonoid(fromTypes), fromTypes.head.innerType)
        } else {
          // maybe there is no group by
          // compute the inner type and
          // add to the map attributes -> froms

          val c = ConcatAttributes()

          val slots = s.from.zip(fromTypes).map {
            case (Gen(None, _), t) => ConcatSlot("", t.innerType)
            case (Gen(Some(PatternIdn(IdnDef(idn))), _), t) => ConcatSlot(idn, t.innerType)

          }
          freshConcat(c, slotsSet = Set(slots))
          logger.debug(s"selectStarType slots $slots")
          val inner = RecordType(c)
          if (s.group.isDefined) {
            //   SELECT age, * FROM students, professors GROUP BY age
            logger.debug(s"in collection type")
            CollectionType(maxMonoid(fromTypes), inner)
          } else {
            //   SELECT * FROM students, professors
            inner
          }
        }
      }
      aux
  }

  /** Type Checker constraint solver.
    * Solves a sequence of AND constraints.
    */
  private def solve(cs: Seq[Constraint]): Boolean = {

    def solver(c: Constraint): Boolean = c match {
      case SameType(e1, e2, desc)     =>
        val t1 = expType(e1)
        val t2 = expType(e2)
        val r = unify(t1, t2)
        if (!r) {
          tipeErrors += IncompatibleTypes(walk(t1), walk(t2), Some(e1.pos), Some(e2.pos))
        }
        r
      case HasType(e, expected, desc) =>
        val t = expType(e)
        logger.debug(s"HasType(${CalculusPrettyPrinter(e)}, ${PrettyPrinter(expected)})")
        val r = unify(t, expected)
//        logger.debug("monoidsVarMap after unify:\n" + monoidsVarMap.toString)
        logger.debug("typesVarMap after unify:\n" + typesVarMap.toString)
        if (!r) {
          tipeErrors += UnexpectedType(walk(t), walk(expected), desc, Some(e.pos))
        }
        r

      case MaxOfMonoids(n, gs) =>
        logger.debug(s"In MaxOfMonoids")
        logger.debug(s"n ${CalculusPrettyPrinter(n)}")
        logger.debug(s"gs ${gs.map(CalculusPrettyPrinter(_)).mkString("  ")}")

        val fromTypes = gs.map {
          case Gen(_, e) =>
            val te = expType(e)
            find(te) match {
              case tf: CollectionType => tf
              case _                  => return true
            }
        }

        val t = expType(n)
        val m = n match {
          case Comp(pm: PrimitiveMonoid, _, _) => pm
          // TODO: AlgebraNode is too general for reduce
          case (_: AlgebraNode | Comp(_: CollectionMonoid, _, _) | _: Select) =>
            find(t) match {
              case CollectionType(cm, _) => cm
              case _: NothingType => return true
            }
        }
        logger.debug(s"m is ${PrettyPrinter(m)}")

        for (minType <- fromTypes) {
          val minM = minType.m
          logger.debug(s"minM is ${PrettyPrinter(minM)}")
          val nv = MonoidVariable()
          addMonoidOrder(nv, m)
          val r = unifyMonoids(minM, nv)
//          logger.debug("monoidsVarMap after unify:\n" + monoidsVarMap.toString)
          logger.debug("typesVarMap after unify:\n" + typesVarMap.toString)
          if (!r) {
            logger.debug("Hey we failed here")
            // TODO: Fix error message: should have m and nm?
            tipeErrors += IncompatibleMonoids(m, walk(minType), Some(n.pos))
            return false
          }
        }
        return true

      case PartitionHasType(p) =>
        partitionEntity(p) match {
          case PartitionEntity(s, t) =>
            val t1 = selectPartitionType(s)
            val r = unify(t, t1)
            if (!r) {
              tipeErrors += UnexpectedType(t, t1, Some("Unexpected partition type"))
              false
            }
            r
          case _ =>
            tipeErrors += UnknownPartition(p)
            false
        }

      case StarHasType(s) =>
        starEntity(s) match {
          case StarEntity(e, t) =>
            e match {
              case s1: Select =>
                val t1 = find(selectStarType(s1))
                val r = unify(t, t1)
                if (!r) {
                  tipeErrors += UnexpectedType(t, t1, Some("Unexpected star type"))
                  false
                }
                r
              case c: Comp =>
                ???
            }
          case _                =>
            tipeErrors += UnknownStar(s)
            false
        }

      case BoundByType(b @ Bind(p, e)) =>
        tipeBind(b) match {
          case Some(_: FreeSymbols) => true
          case _ => false
        }

      case IdnIsDefined(idnExp @ IdnExp(idn)) =>

        def getType(nt: Type): Boolean = {
          val t1 = nt match {
            case TypeScheme(t, typeSyms, monoidSyms, attSyms) =>
              if (typeSyms.isEmpty && monoidSyms.isEmpty && attSyms.isEmpty)
                t
              else instantiateTypeScheme(t, typeSyms, monoidSyms, attSyms)
            case t => t
          }

          val t = expType(idnExp)
          val r = unify(t, t1)
          if (!r) {
            // The same decl has been used with two different types.
            // TODO: Can we have a more precise error messages? Set the None to a better message!
            tipeErrors += UnexpectedType(walk(t), walk(t1), None, Some(t.pos))
          }
          r
        }

        entity(idn) match {
          case _: UnknownEntity =>
            // Identifier is unknown

            lookupAttributeEntity(idnExp) match {
              case AttributeEntity(att, _, _) =>
                // We found the attribute identifier in a generator
                getType(att.tipe)
              case _: UnknownEntity  =>
                // We didn't found the attribute identifier
                tipeErrors += UnknownDecl(idn)
                false
              case _: MultipleEntity =>
                // We found the attribute identifier more than once
                tipeErrors += AmbiguousIdn(idn)
                false
            }
          case _: MultipleEntity =>
            // Error already reported earlier when processing IdnDef
            false
          case _ =>
            // We found an entity for the identifier.
            // However, we must still check it is not ambiguous so we look up in the anonymous chain as well.
            lookupAttributeEntity(idnExp) match {
              case _: UnknownEntity =>
                // All good
                getType(idnType(idn))
              case (_: AttributeEntity | _: MultipleEntity) =>
                // We found the same identifier used by the user and being anonymous as well!
                tipeErrors += AmbiguousIdn(idn)
                false
            }
        }

      case GenPatternHasType(g) =>
        val t = expType(g.e)
        find(t) match {
          case CollectionType(_, innerType) =>
            val expected = patternType(g.p.get)
            val r = unify(innerType, expected)
            if (!r) {
              tipeErrors += PatternMismatch(g.p.get, walk(innerType), Some(g.p.get.pos))
              return false
            }
            r
        }

      case FunAppType(funApp @ FunApp(f, e)) =>
        val t = expType(f)
        logger.debug(s"== ${PrettyPrinter(walk(expType(f)))}, ${PrettyPrinter(walk(expType(e)))}")
        find(t) match {
          case FunType(expected, output) =>
            val t1 = expType(e)

            def makeUpPattern(t: Type): Type = {
              def recurse(t: Type): Type = find(t) match {
                case RecordType(Attributes(atts)) => PatternType(atts.map { att => PatternAttrType(makeUpPattern(find(att.tipe))) })
                case _                            => t
              }
              recurse(t)
            }

            logger.debug(s"--expected ${PrettyPrinter(walk(expected))}")
            logger.debug(s"--output ${PrettyPrinter(walk(output))}")
            val r = unify(makeUpPattern(t1), expected)
            if (!r) {
              tipeErrors += IncompatibleTypes(walk(t1), walk(expected), Some(e.pos), Some(f.pos))
              return false
            }

            logger.debug(s"++expected ${PrettyPrinter(walk(expected))}")
            logger.debug(s"++output ${PrettyPrinter(walk(output))}")

            val tf = expType(funApp)
            val r1 = unify(output, tf)

            logger.debug(s"makeUpPattern ${PrettyPrinter(walk(makeUpPattern(t1)))}")
            logger.debug("AT funApp with")
            logger.debug(s"e ${PrettyPrinter(walk(t1))} r1 $r1")
            logger.debug(s"tf ${PrettyPrinter(walk(tf))} r1 $r1")
            logger.debug(s"output ${PrettyPrinter(walk(output))}")
            logger.debug(s"expected ${PrettyPrinter(walk(expected))}")

            if (!r1) {
              tipeErrors += IncompatibleTypes(walk(output), walk(tf), Some(e.pos), Some(f.pos))
              return false
            }
            r1
        }

    }

    cs match {
      case c :: rest =>
        //        logger.debug(s"  solving $c")
        if (solver(c))
          solve(rest)
        else
          false
      case _         => true
    }

  }

  /** Given a type, returns a new type that replaces type variables as much as possible, given the map m.
    * This is the type representing the group of types.
    */

  // TODO: Refactor into World.VarMap to avoid duplicated code
  // TODO: Perhaps do the same for all getVariableTypes, etc etc

  private def find(t: Type): Type =
    if (typesVarMap.contains(t)) typesVarMap(t).root else t

  private def aFind(t: RecordAttributes): RecordAttributes =
    if (recAttsVarMap.contains(t)) recAttsVarMap(t).root else t

  /** Reconstruct the type by resolving all inner variable types as much as possible.
    * Also, try to match the type into an existing user type.
    */
  private def walk(t: Type): Type = {

    def pickMostRepresentativeType(g: Group[Type]): Type = {
      val ut = g.elements.collectFirst { case u: UserType => u }
      ut match {
        case Some(picked) =>
          // Prefer user type
          picked
        case None =>
          val ct = g.elements.find { case _: VariableType => false; case _ => true }
          ct match {
            case Some(picked) =>
              // Otherwise, prefer a final - i.e. non-variable - type
              picked
            case None =>
              val vt = g.elements.find { case _: TypeVariable => false; case _ => true }
              vt match {
                case Some(picked) =>
                  // Otherwise, prefer a variable type that is not a type variable (e.g. a number or constrainted record)
                  picked
                case None =>
                  // Finally, prefer the root type variable
                  g.root
              }
          }
      }
    }

    // TODO should we pass occurscheck from the reconstruct type? I guess yes
//
    def reconstructAttributes(atts: RecordAttributes, occursCheck: Set[Type]): RecordAttributes = aFind(atts) match {
      case Attributes(atts1)              =>
        Attributes(atts1.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck)) })
      case AttributesVariable(atts1, sym) =>
        AttributesVariable(atts1.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck)) }, sym)
      case c: ConcatAttributes =>
        val props = getConcatProperties(c)
        if (props.isComplete)
          Attributes(props.atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck)) })
        else
          c
//        val usedIdns = scala.collection.mutable.Set[Idn]()
//
//        def uniqueIdn(i: Idn, j: Int = 0): Idn = {
//          val ni = if (j == 0) i else s"${i}_$j"
//          if (usedIdns.contains(ni))
//            uniqueIdn(i, j+1)
//          else {
//            usedIdns += ni
//            ni
//          }
//        }
//
//        val props = getConcatProperties(c)
//        logger.debug(s"here with props $props")
//        if (props.isComplete)
//          Attributes(props.atts.flatMap {
//            case AttrType(idn1, t1: RecordType) =>
//              reconstructType(t1, occursCheck) match {
//                case t2: RecordType => t2.recAtts.atts.map { case AttrType(idn2, t2) => AttrType(uniqueIdn(s"${idn1}_$idn2"), reconstructType(t2, occursCheck)) }
//              }
//            case AttrType(idn1, t1) => Seq(AttrType(uniqueIdn(idn1), reconstructType(t1, occursCheck)))
//          })
//        else
//          c
    }
//      case c @ ConcatAttributes(_, sym) =>
//        val links = concatGraph(c)
//        for (gs <- links.froms) {
//
//          // Return full sequence of fully-resolved attribute types, if possible
//          def getInners(gs: Seq[Gen]): Option[Seq[AttrType]] = {
//
//            // Collect sequence of (pattern, inner type) for generators with known collection types.
//            // Fail early if some generator type is not yet known.
//            val inners = scala.collection.mutable.MutableList[(Option[Pattern], Type)]()
//            for (g <- gs) {
//              g match {
//                case Gen(p, e) =>
//                  val t = expType(e)
//                  reconstructType(t, occursCheck) match {
//                    case ResolvedType(CollectionType(_, inner1)) => inners += ((p, inner1))
//                    case _ => return None
//                  }
//              }
//            }
//
//            val usedIdns = scala.collection.mutable.Set[Idn]()
//
//            def uniqueIdn(i: Idn, j: Int = 0): Idn = {
//              val ni = if (j == 0) i else s"${i}_$j"
//              if (usedIdns.contains(ni))
//                uniqueIdn(i, j+1)
//              else {
//                usedIdns += ni
//                ni
//              }
//            }
//
//            // Collect sequence of records for fully-typed inner fields
//            val atts = scala.collection.mutable.MutableList[AttrType]()
//            for (inner <- inners) {
//              inner match {
//                case (_, ResolvedType(RecordType(Attributes(atts1)))) =>
//                  for (att <- atts1) {
//                    atts += AttrType(uniqueIdn(att.idn), att.tipe)
//                  }
//                case (_, ResolvedType(_: RecordType)) =>
//                  // Not fully defined record, so cannot narrow
//                  return None
//                case (Some(PatternIdn(IdnDef(idn))), inner1) =>
//                  atts += AttrType(uniqueIdn(idn), inner1)
//                case (None, inner1) =>
//                  // TODO: This _1_2 convention (in case of uniqueIdn) isn't...great
//                  atts += AttrType(uniqueIdn(s"_${atts.length + 1}"), inner1)
//              }
//            }
//
//            logger.debug(s"atts is $atts")
//            assert(atts.map(_.idn).toSet.size == atts.map(_.idn).length) // TODO: Ensure that there are no overlapping idn names
//
//            Some(atts.to)
//          }
//
//          getInners(gs) match {
//            case Some(atts2) =>
//              logger.debug(s"got here@!!!! $atts2")
//              // We found a fully formed attribute, so let's return early
//              return Attributes(atts2)
//            case None =>
//              // Try the next sequence of generators
//          }
//
//        }
//        // Nothing fully-formed was found, so return a ConcatAttributes again
//        // TODO: reconstructType of inner types ? no.. there's no inner types: it's just the froms? hum... to check w/ Ben whether we replicate stuff or not
//        ConcatAttributes(???, sym)
//    }

//
//    def reconstructAttributes(atts: RecordAttributes, occursCheck: Set[Type]): RecordAttributes = atts match {
//      case concat: ConcatAttributes => {
//        val links = concatGraph.get(concat).head
//        val possibleRecordAttributes = links.froms.map {
//          // for each from list, build its type.
//          case f =>
//            val items: Seq[(Option[Idn], Type)] = f.map {
//              // walk the gens and build their type, we keep the gen name if we should use it later
//              //              case Gen(Some(PatternIdn(idn)), e) => (Some(idn.idn), reconstructType(expType(e), occursCheck).asInstanceOf[CollectionType].innerType)
//              //              case Gen(None, e) => (None, find(expType(e)).asInstanceOf[CollectionType].innerType)
//              case Gen(p, e) =>
//                val inner = reconstructType(expType(e), occursCheck) match {
//                  case UserType(m) => world.tipes(m) match {
//                    case CollectionType(_, inner) => inner
//                  }
//                  case CollectionType(_, inner) => inner
//                }
//                p match {
//                  case Some(PatternIdn(idn)) => (Some(idn.idn), inner)
//                  case None => (None, inner)
//                }
//            }
////            // because we wouldn't have a concat attributes with just one from
//            assert(items.length != 1)
////            // length is > 1, we make a concatenation of records
//            val flatItems: Seq[(Option[Idn], Type)] = items.flatMap {
//              case item => item._2 match {
//                case r: RecordType => r.recAtts.atts.map{case a => (Some(a.idn), a.tipe)} // we can have a concat here
//                case _ => Seq((item._1, item._2)) // can be considered as a record of one field since we concatenate
//              }
//            }
////            // this is where we generate names and all, here I just replace None by the index, since it's flattened it is good
//            val newAttrs = flatItems.zipWithIndex.map{case (item, idx) =>
//              AttrType(if (item._1.isEmpty) s"_${idx+1}" else item._1.get, reconstructType(item._2, occursCheck))
//            }
//            ConcatAttributes(newAttrs, concat.sym)
//        }
//        assert(possibleRecordAttributes.size == 1) // TODO to merge all
//        possibleRecordAttributes.head
//      }
//      case Attributes(atts)              =>
//        Attributes(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) })
//      case AttributesVariable(atts, sym) =>
//        AttributesVariable(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, sym)
//    }


    def reconstructType(t: Type, occursCheck: Set[Type]): Type = {
      val r = if (occursCheck.contains(t)) {
        t
      } else {
        t match {
          case _: NothingType                  => t
          case _: AnyType                      => t
          case _: IntType                      => t
          case _: BoolType                     => t
          case _: FloatType                    => t
          case _: StringType                   => t
          case _: UserType                     => t
          case _: PrimitiveType                => if (!typesVarMap.contains(t)) t else reconstructType(pickMostRepresentativeType(typesVarMap(t)), occursCheck + t)
          case _: NumberType                   => if (!typesVarMap.contains(t)) t else reconstructType(pickMostRepresentativeType(typesVarMap(t)), occursCheck + t)
          case RecordType(a)                   => RecordType(reconstructAttributes(a, occursCheck + t))
          case PatternType(atts)          => PatternType(atts.map { case att => PatternAttrType(reconstructType(att.tipe, occursCheck + t)) })
          case CollectionType(m, innerType)    => CollectionType(mFind(m).asInstanceOf[CollectionMonoid], reconstructType(innerType, occursCheck + t))
          case FunType(p, e)                   => FunType(reconstructType(p, occursCheck + t), reconstructType(e, occursCheck + t))
          case t1: TypeVariable => if (!typesVarMap.contains(t1)) t1 else reconstructType(pickMostRepresentativeType(typesVarMap(t1)), occursCheck + t)
        }
      }
      r.nullable = r.nullable || t.nullable
      r
    }

    reconstructType(t, Set())
  }

  def partitionType(p: Entity): Type = p match {
    case PartitionEntity(_, t) => t
    case _ => NothingType()
  }

  def starType(s: Entity): Type = s match {
    case StarEntity(_, t) => t
    case _ => NothingType()
  }
  
  /** Constraints of a node.
    * Each entry represents the constraints (aka. the facts) that a given node adds to the overall type checker.
    */
  // TODO: Decide what goes in expType, what doesn't. Whatever we decide, make it coherent. Could be to put everything but consts in constraint?
  // TODO: Or could be to put things that are constant - ie. depend on primitive types or expTypes only - on the expTYpe directy. The adv of the latter is that if
  // TODO: likely puts much less load in the constraint solver because there's less constraints.
  // TODO: With Ben, we sort of agree that we should re-order constraints so that the stuff that provides more information goes first.
  // TODO: Typically this means HasType(e, IntType()) goes before a SameType(n, e). We believe this may impact the precision of error reporting.
  def constraint(n: RawNode): Seq[Constraint] = {
    import Constraint._

    n match {

      case p: Partition =>
        Seq(
          PartitionHasType(p))

      case s: Star =>
        Seq(
          StarHasType(s))

      case n: IdnExp =>
        Seq(
          IdnIsDefined(n))

      // Select
      case n@Select(froms, d, g, proj, w, o, h) =>
        val m =
          if (o.isDefined)
            ListMonoid()
          else if (d)
            SetMonoid()
          else
            MonoidVariable()

        Seq(
          HasType(n, CollectionType(m, expType(proj))),
          MaxOfMonoids(n, froms))

      // Rule 4
      case n@RecordProj(e, idn) =>
        Seq(
          HasType(e, RecordType(AttributesVariable(Set(AttrType(idn, expType(n)))))))

      // Rule 6
      case n@IfThenElse(e1, e2, e3) =>
        Seq(
          HasType(e1, BoolType(), Some("if condition must be a boolean")),
          SameType(e2, e3, Some("then and else must be of the same type")),
          SameType(n, e2))

      case n @ FunAbs(p, e) =>
        Seq(
          HasType(n, FunType(patternType(p), expType(e))))

      // Rule 8
      case n @ FunApp(f, e) =>
        Seq(
          HasType(f, FunType(TypeVariable(), expType(n))),
          FunAppType(n))
//          HasType(f, FunType(expType(e), expType(n))))

      // Rule 11
      case n@MergeMonoid(_: BoolMonoid, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          HasType(e1, BoolType()),
          HasType(e2, BoolType()))

      case n@MergeMonoid(_: NumberMonoid, e1, e2) =>
        Seq(
          HasType(n, NumberType()),
          SameType(n, e1),
          SameType(e1, e2))

      // Rule 12
      case n@MergeMonoid(m: CollectionMonoid, e1, e2) =>
        Seq(
          HasType(e1, CollectionType(m, TypeVariable())),
          SameType(e1, e2),
          SameType(n, e1))

      // Rule 13
      case n@Comp(_: NumberMonoid, qs, e) =>
        val gs = qs.collect { case g: Gen => g }
          Seq(
            HasType(e, NumberType()),
            SameType(n, e),
            MaxOfMonoids(n, gs))

      case n@Comp(_: BoolMonoid, qs, e) =>
        val gs = qs.collect { case g: Gen => g }
        Seq(
          HasType(e, BoolType()),
          SameType(n, e),
          MaxOfMonoids(n, gs))

      // Rule 14
      case n@Comp(m: CollectionMonoid, qs, e1) =>
        val gs = qs.collect { case g: Gen => g }
        Seq(
          HasType(n, CollectionType(m, expType(e1))),
          MaxOfMonoids(n, gs))

      // Binary Expression
      case n@BinaryExp(_: EqualityOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e1, e2))

      case n@BinaryExp(_: ComparisonOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e2, e1),
          HasType(e1, NumberType()))

      case n@BinaryExp(_: ArithmeticOperator, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, NumberType()))

      // Binary Expression
      case n@InExp(e1, e2) =>
        val inner = TypeVariable()
        Seq(
          HasType(e2, CollectionType(MonoidVariable(), inner)),
          HasType(e1, inner),
          HasType(n, BoolType()))

      // Unary Expression type

      case n@UnaryExp(_: Not, e) =>
        Seq(
          SameType(n, e),
          HasType(e, BoolType()))

      case n@UnaryExp(_: Neg, e) =>
        Seq(
          SameType(n, e),
          HasType(e, NumberType()))

      case n@UnaryExp(_: ToBag, e) =>
        val inner = TypeVariable()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), inner)),
          HasType(n, CollectionType(BagMonoid(), inner)))

      case n@UnaryExp(_: ToList, e) =>
        val inner = TypeVariable()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), inner)),
          HasType(n, CollectionType(ListMonoid(), inner)))

      case n@UnaryExp(_: ToSet, e) =>
        val inner = TypeVariable()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), inner)),
          HasType(n, CollectionType(SetMonoid(), inner)))

      // Expression block type
      case n@ExpBlock(_, e) =>
        Seq(
          SameType(n, e))

      // Declarations

      case Gen(None, e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), TypeVariable())))

      case g @ Gen(Some(p), e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), TypeVariable())),
          GenPatternHasType(g))

      case b: Bind =>
        Seq(
          BoundByType(b))

      // Operators

      case n@Reduce(m: CollectionMonoid, g, e) =>
        Seq(
          HasType(n, CollectionType(m, expType(e))))

      case n@Reduce(m: NumberMonoid, g, e) =>
        Seq(
          HasType(e, NumberType()),
          SameType(n, e))

      case n@Reduce(m: BoolMonoid, g, e) =>
        Seq(
          HasType(e, BoolType()),
          HasType(n, BoolType()))

      case n@Filter(g, p) =>
        Seq(
          SameType(n, g.e),
          HasType(p, BoolType()))

      case n@Nest(rm: CollectionMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        Seq(
          HasType(g.e, CollectionType(m, TypeVariable())),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", expType(k)), AttrType("_2", CollectionType(rm, expType(e)))))))))

      case n@Nest(rm: NumberMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val nt = NumberType()
        Seq(
          HasType(g.e, CollectionType(m, TypeVariable())),
          HasType(e, nt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", expType(k)), AttrType("_2", nt)))))))

      case n@Nest(rm: BoolMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val bt = BoolType()
        Seq(
          HasType(g.e, CollectionType(m, TypeVariable())),
          HasType(e, bt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", expType(k)), AttrType("_2", bt)))))))

      case n@Nest2(rm: CollectionMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val inner = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, inner)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner), AttrType("_2", CollectionType(rm, expType(e)))))))))

      case n@Nest2(rm: NumberMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val nt = NumberType()
        val inner = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, inner)),
          HasType(e, nt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner), AttrType("_2", nt)))))))

      case n@Nest2(rm: BoolMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val bt = BoolType()
        val inner = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, inner)),
          HasType(e, bt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner), AttrType("_2", bt)))))))

      case n@Nest3(rm: CollectionMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val inner1 = TypeVariable()
        val inner2 = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner1), AttrType("_2", inner2)))))),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner1), AttrType("_2", CollectionType(rm, expType(e)))))))))

      case n@Nest3(rm: NumberMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val nt = NumberType()
        val inner1 = TypeVariable()
        val inner2 = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner1), AttrType("_2", inner2)))))),
          HasType(e, nt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner1), AttrType("_2", nt)))))))

      case n@Nest3(rm: BoolMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val bt = BoolType()
        val inner1 = TypeVariable()
        val inner2 = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner1), AttrType("_2", inner2)))))),
          HasType(e, bt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", inner1), AttrType("_2", bt)))))))

//      case n@MultiNest(g, params) =>
//        val m = MonoidVariable()
//        val inner = TypeVariable() // g is over a collection of inner
//
//        def mkConstraints(params: Seq[NestParams]): Seq[Constraint] = {
//          params match {
//            case param :: tail => {
//              param.m match {
//                case rm: CollectionMonoid =>
//                  HasType(param.p, BoolType()) +: mkConstraints(tail)
//                case rm: NumberMonoid =>
//                  val nt = NumberType()
//                  Seq(
//                    HasType(g.e, CollectionType(m, TypeVariable())),
//                    HasType(param.e, nt),
//                    HasType(param.p, BoolType())) ++ mkConstraints(tail)
//                case rm: BoolMonoid =>
//                  val bt = BoolType()
//                  Seq(
//                    HasType(param.e, bt),
//                    HasType(param.p, BoolType())) ++ mkConstraints(tail)
//              }
//            }
//            case Nil => Seq(HasType(g.e, CollectionType(m, inner)))
//          }
//        }
//
//        def mkParamType(param: NestParams): Type = param.m match {
//          case m: CollectionMonoid => CollectionType(m, expType(param.e))
//          case m: NumberMonoid => expType(param.e)
//          case m: BoolMonoid => BoolType()
//        }
//
//        def mkOuterType(params: Seq[NestParams]): Type = params match {
//          case param :: Nil => mkParamType(param)
//          case param :: tail => RecordType(Seq(AttrType("_1", mkParamType(param)), AttrType("_2", mkOuterType(tail))), None)
//        }
//
//        mkConstraints(params) :+ HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", inner), AttrType("_2", mkOuterType(params))), None)))

      case n @ Join(g1, g2, p) =>
        val t1 = TypeVariable()
        val t2 = TypeVariable()
        Seq(
          HasType(g1.e, CollectionType(MonoidVariable(), t1)),
          HasType(g2.e, CollectionType(MonoidVariable(), t2)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(MonoidVariable(), RecordType(Attributes(Seq(AttrType("_1", t1), AttrType("_2", t2)))))),
          MaxOfMonoids(n, Seq(g1, g2)))

      case n @ OuterJoin(g1, g2, p) =>
        val t1 = TypeVariable()
        val t2 = TypeVariable()
        Seq(
          HasType(g1.e, CollectionType(MonoidVariable(), t1)),
          HasType(g2.e, CollectionType(MonoidVariable(), t2)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(MonoidVariable(), RecordType(Attributes(Seq(AttrType("_1", t1), AttrType("_2", t2)))))),
          MaxOfMonoids(n, Seq(g1, g2)))

      case n @ Unnest(g1, g2, p) =>
        val t1 = TypeVariable()
        val t2 = TypeVariable()
        Seq(
          HasType(g1.e, CollectionType(MonoidVariable(), t1)),
          HasType(g2.e, CollectionType(MonoidVariable(), t2)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(MonoidVariable(), RecordType(Attributes(Seq(AttrType("_1", t1), AttrType("_2", t2)))))),
          MaxOfMonoids(n, Seq(g1, g2)))

      case n @ OuterUnnest(g1, g2, p) =>
        val t1 = TypeVariable()
        val t2 = TypeVariable()
        Seq(
          HasType(g1.e, CollectionType(MonoidVariable(), t1)),
          HasType(g2.e, CollectionType(MonoidVariable(), t2)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(MonoidVariable(), RecordType(Attributes(Seq(AttrType("_1", t1), AttrType("_2", t2)))))),
          MaxOfMonoids(n, Seq(g1, g2)))

      // Sugar nodes

      case n @ Sum(e) =>
        val mv = MonoidVariable()
        addMonoidOrder(mv, SumMonoid())
        val tn = NumberType()
        Seq(
          HasType(e, CollectionType(mv, tn)),
          HasType(n, tn))

      case n @ Max(e) =>
        val tn = NumberType()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), tn)),
          HasType(n, tn))

      case n @ Min(e) =>
        val tn = NumberType()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), tn)),
          HasType(n, tn))

      case n @ Avg(e) =>
        val tn = NumberType()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), tn)),
          HasType(n, tn))

      case n @ Count(e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), TypeVariable())))

      case n @ Exists(e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), TypeVariable())))

      case n @ MultiCons(m, head :: tail) if tail.nonEmpty =>
        val thead = expType(head)
        tail.map { case e => HasType(e, thead) } :+ HasType(n, CollectionType(m, thead))
      case _ =>
        Seq()
    }
  }

  /** Collects the constraints of an expression.
    * The constraints are returned in an "ordered sequence", i.e. the child constraints are collected before the current node's constraints.
    */
  // TODO: Move constraint(n) to top-level and apply it always at the end
  // TODO: It seems to Miguel that this is all just a simple top down collect except the case of Bind in a Comp or ExpBlock
  // TODO: So perhaps this can be refactored to make that case evident and all others be just a kiama collect, adding ourselves (node n) last.
  lazy val constraints: Exp => Seq[Constraint] = attr {
    case n @ Comp(_, qs, e) => qs.flatMap{ case e: Exp => constraints(e) case g @ Gen(_, e1) => constraints(e1) ++ constraint(g) case b @ Bind(_, e1) => constraint(b) } ++ constraints(e) ++ constraint(n)
    case n @ Select(from, _, g, proj, w, o, h) =>
      val fc = from.flatMap { case it @ Gen(_, e) => constraints(e) ++ constraint(it) }
      val gc = if (g.isDefined) constraints(g.get) else Nil
      val wc = if (w.isDefined) constraints(w.get) else Nil
      val oc = if (o.isDefined) constraints(o.get) else Nil
      val hc = if (h.isDefined) constraints(h.get) else Nil
      fc ++ gc ++ constraints(proj) ++ wc ++ oc ++ hc ++ constraint(n)
    case n @ FunAbs(_, e) => constraints(e) ++ constraint(n)
    case n @ ExpBlock(binds, e) => binds.toList.flatMap{ case b @ Bind(_, e1) => constraint(b)} ++ constraints(e) ++ constraint(n)
    case n @ MergeMonoid(_, e1, e2) => constraints(e1) ++ constraints(e2) ++ constraint(n)
    case n @ BinaryExp(_, e1, e2) => constraints(e1) ++ constraints(e2) ++ constraint(n)
    case n @ InExp(e1, e2) => constraints(e1) ++ constraints(e2) ++ constraint(n)
    case n @ UnaryExp(_, e) => constraints(e) ++ constraint(n)
    case n: IdnExp => constraint(n)
    case n @ RecordProj(e, _) => constraints(e) ++ constraint(n)
    case n: Const => constraint(n)
    case n @ RecordCons(atts) => atts.flatMap { case att => constraints(att.e) } ++ constraint(n)
    case n @ FunApp(f, e) => constraints(f) ++ constraints(e) ++ constraint(n)
    case n @ ZeroCollectionMonoid(_) => constraint(n)
    case n @ ConsCollectionMonoid(_, e) => constraints(e) ++ constraint(n)
    case n @ MultiCons(_, es) => es.flatMap(constraints) ++ constraint(n)
    case n @ IfThenElse(e1, e2, e3) => constraints(e1) ++ constraints(e2) ++ constraints(e3) ++ constraint(n)
    case n: Partition => constraint(n)
    case n: Star => constraint(n)
    case n @ Reduce(m, g, e) => constraints(g.e) ++ constraint(g) ++ constraints(e) ++ constraint(n)
    case n @ Filter(g, p) => constraints(g.e) ++ constraint(g) ++ constraints(p) ++ constraint(n)
    case n @ Nest(m, g, k, p, e) => constraints(g.e) ++ constraint(g) ++ constraints(k) ++ constraints(e) ++ constraints(p) ++ constraint(n)
    case n @ Nest2(m, g, k, p, e) => constraints(g.e) ++ constraint(g) ++ constraints(k) ++ constraints(e) ++ constraints(p) ++ constraint(n)
    case n @ Nest3(m, g, k, p, e) => constraints(g.e) ++ constraint(g) ++ constraints(k) ++ constraints(e) ++ constraints(p) ++ constraint(n)
//    case n @ MultiNest(g, params) => constraints(g.e) ++ constraint(g) ++ params.flatMap{param => constraints(param.k) ++ constraints(param.e) ++ constraints(param.p)} ++ constraint(n)
    case n @ Join(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ OuterJoin(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ Unnest(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ OuterUnnest(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ Sum(e) => constraints(e) ++ constraint(n)
    case n @ Max(e) => constraints(e) ++ constraint(n)
    case n @ Min(e) => constraints(e) ++ constraint(n)
    case n @ Avg(e) => constraints(e) ++ constraint(n)
    case n @ Count(e) => constraints(e) ++ constraint(n)
    case n @ Exists(e) => constraints(e) ++ constraint(n)
  }

//  /** Create a type variable for the FROMs part of a SELECT.
//    * Used for unification in the PartitionHasType() constraint.
//    */
//  private lazy val selectFromsTypeVar: Select => Type = attr {
//    _ => TypeVariable()
//  }
//
//  /** Walk up tree until we find a Select, if it exists.
//    */
//  private def findSelect(n: RawNode): Option[Select] = n match {
//    case s: Select                   => Some(s)
//    case n1 if tree.isRoot(n1)       => None
//    case tree.parent.pair(_, parent) => findSelect(parent)
//  }
//
//  /** Parent Select of the current Select, if it exists.
//    */
//  private lazy val selectParent: Select => Option[Select] = attr {
//    case n if tree.isRoot(n)         => None
//    case tree.parent.pair(_, parent) => findSelect(parent)
//  }
//
//  /** Finds the Select that this partition refers to.
//   */
//  lazy val partitionSelect: Partition => Option[Select] = attr {
//    case p =>
//
//      // Returns true if `p` used in `e`
//      def inExp(e: Exp) = {
//        var found = false
//        query[Exp] {
//          case n if n eq p => found = true
//        }(e)
//        found
//      }
//
//      // Returns true if `p`p used in the from
//      def inFrom(from: Seq[Gen]): Boolean = {
//        for (f <- from) {
//          f match {
//            case Gen(_, e) => if (inExp(e)) return true
//          }
//        }
//        false
//      }
//
//      findSelect(p) match {
//        case Some(s) =>
//          // The partition node is:
//          // - used in the FROM;
//          // - or in the GROUP BY;
//          // - or there is no GROUP BY (which means it cannot possibly refer to our own Select node)
//          if (inFrom(s.from) || (s.group.isDefined && inExp(s.group.get)) || s.group.isEmpty) {
//            selectParent(s)
//          } else {
//            Some(s)
//          }
//        case None => None
//      }
//  }

  /** For debugging.
    * Prints all the type groups.
    */
  def printTypedTree(): Unit = {

    if (queryString.isEmpty) {
      return
    }
    val q = queryString.head

    def printMap(pos: Set[Position], t: Type) = {
      val posPerLine = pos.groupBy(_.line)
      var output = s"Type: ${FriendlierPrettyPrinter(t)}\n"
      for ((line, lineno) <- q.split("\n").zipWithIndex) {
        output += line + "\n"
        if (posPerLine.contains(lineno + 1)) {
          val cols = posPerLine(lineno + 1).map(_.column).toList.sortWith(_ < _)
          var c = 0
          for (col <- cols) {
            output += " " * (col - c - 1)
            output += "^"
            c = col
          }
          output += "\n"
        }
      }
      output
    }

    val collectMaps = collect[List, (Type, Position)] {
      case e: Exp =>
        val t = expType(e)
        if (!typesVarMap.contains(t)) {
          t -> e.pos
        } else {
          walk(typesVarMap(t).root) -> e.pos
        }
    }

    for ((t, items) <- collectMaps(tree.root).groupBy(_._1)) {
      logger.debug(printMap(items.map(_._2).toSet, t))
    }

  }
}

// TODO: Change constraints to do proper collect in order, so that we don't ever forget anything. Exceptions (if any) can be handled in a pattern match at the beginning
// TODO: Add detailed description of the type checker: the flow, the unification, partial records, how are errors handled, ...
// TODO: Add more tests to the SemanticAnalyzer:
//        - let-polymorphism in combination with patterns;
//        - polymorphism of Gen?
//        - error messages (at least generate a test per error message/location in code where it is generated).
// TODO: Report unrelated errors (by setting failed unifications to NothingType and letting them propagate.)
// TODO: Consider adding syntax like: fun f(x) -> if (x = 0) then 1 else f(x - 1) * x)
//       It should just type to FunType(IntType(), IntType().
//       It is not strictly needed but the notion of a NamedFunction may help code-generation because these are things we don't inline/consider inlining,
// TODO: Do we need to add a closure check, or is that executor-specific?
// TODO: Add check to forbid polymorphic recursion (page 377 or 366 of ML Impl book)
// TODO: Add issue regarding polymorphic code generation:
//       e.g. if Int, Bool on usage generate 2 versions of the method;
//       more interestingly, if ConstraintRecordType, find out the actual records used and generate versions for those.
// TODO: Add notion of declaration. Bind and NamedFunc are now declarations. ExpBlock takes sequence of declarations followed by an expression.
// TODO: Add abstract class to encompass both Select, Comp (and even CanComp)?
// TODO: We don't typecheck CanComp: should we?
// TODO: Add support for typing an expression like max(students.age) where students is a collection. Or even max(students.personal_info.age)
// TODO: If I do yield bag, I think I also constrain on what the input's commutativity and associativity can be!...
//       success("""\x -> for (y <- x) yield bag (y.age * 2, y.name)""", world,
// TODO: I should be able to do for (x <- col) yield f(x) to keep same collection type as in col
//       This should only happen for a single col I guess?. It helps write the map function.
// TODO: Syntactic sugar for functions of 1,2,3,4,5,6,7,8,9,10,...20 arguments. Should support recursion?
//        {
//          fact1 := \(f, n1) -> if (n1 = 0) then 1 else n1 * (f(f, n1 - 1));
//          fact := \n -> fact1(fact1, n);
//          fact
//        }
//      def fact(n) := if (n = 0) then 1 else n * fact(n - 1)
//      -> Func1(fact, n, if (n = 0) then 1 else n * fact(n - 1))
//
// TODO: Support markdown syntax in comments
// TODO: Change code of transformers to rely on resolvedType/ResolvedType instead of world.tipes. Hide world.tipes visiblity from the SemanticAnalyzer (make it private, not public or protected)

// TODO: The monoids stuff is doing impossible checks: e.g. if smtg has been unified before, it can't really narrow further, right?