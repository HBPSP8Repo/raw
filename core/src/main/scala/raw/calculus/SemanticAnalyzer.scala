package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution
import raw.World._

import scala.util.parsing.input.Position

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
class SemanticAnalyzer(val tree: Calculus.Calculus, val world: World, val queryString: Option[String] = None) extends Attribution with LazyLogging {

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

  private val monoidsVarMap = new MonoidsVarMap()

  /** The map of variables.
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

  /** Return the base type of an expression, i.e. without the nullable flag.
    */
  lazy val baseType: Exp => Type = attr {
    e => {
      solve(constraints(e))
      walk(expType(e))
    }
  }

  /** Add the nullable flag to a type.
    */
  private def makeNullable(source: Type, models: Seq[Type], nulls: Seq[Type], nullable: Option[Boolean] = None): Type = {
    val t = (source, models) match {
      case (col @ CollectionType(m, i), colls: Seq[CollectionType])               =>
        val inners = colls.map(_.innerType)
        CollectionType(m, makeNullable(i, inners, inners, nullable))
      case (f @ FunType(p, e), funs: Seq[FunType])                                =>
        val otherP = funs.map(_.t1)
        val otherE = funs.map(_.t2)
        FunType(makeNullable(p, otherP, otherP, nullable), makeNullable(e, otherE, otherE, nullable))
      case (r @ RecordType(attr, n), recs: Seq[RecordType])                       =>
        val attributes = attr.map { case AttrType(idn, i) =>
          val others = recs.map { r => r.getType(idn).get }
          AttrType(idn, makeNullable(i, others, others, nullable))
        }
        RecordType(attributes, n)
      case (_: IntType, _)                                                        => IntType()
      case (_: FloatType, _)                                                      => FloatType()
      case (_: BoolType, _)                                                       => BoolType()
      case (_: StringType, _)                                                     => StringType()
      case (u: UserType, _)                                                       => UserType(u.sym)
      case (v: TypeVariable, _)                                                   => TypeVariable(v.sym)
      case (v: NumberType, _)                                                     => NumberType(v.sym)
      case (r @ ConstraintRecordType(attr, sym), recs: Seq[ConstraintRecordType]) =>
        val attributes = attr.map { case AttrType(idn, i) =>
          val others = recs.map { r => r.getType(idn).get }
          AttrType(idn, makeNullable(i, others, others, nullable))
        }
        ConstraintRecordType(attributes, sym)
    }
    t.nullable = nullable.getOrElse(t.nullable || nulls.collect{case t if t.nullable => t}.nonEmpty) // TODO: nulls.exists ?
    t
  }

  /** Return the type of an expression, including the nullable flag.
    */
  lazy val tipe: Exp => Type = attr {
    e => {
      val te = baseType(e) // regular type (no option except from sources)
      val nt = e match {
        case RecordProj(e1, idn) => tipe(e1) match {
          case rt: RecordType => makeNullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
          case rt: ConstraintRecordType => makeNullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
          case ut: UserType =>
            typesVarMap(ut).root match {
              case rt: RecordType => makeNullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
            }
        }
        case ConsCollectionMonoid(m, e1) => CollectionType(m, tipe(e1))
        case IfThenElse(e1, e2, e3) => (tipe(e1), tipe(e2), tipe(e3)) match {
          case (t1, t2, t3) => makeNullable(te, Seq(t2, t3), Seq(t1, t2, t3))
        }
        case FunApp(f, v) => tipe(f) match {
          case ft @ FunType(t1, t2) => makeNullable(te, Seq(t2), Seq(ft, t2, tipe(v)))
        }
        case MergeMonoid(_, e1, e2) => (tipe(e1), tipe(e2)) match {
          case (t1, t2) => makeNullable(te, Seq(t1, t2), Seq(t1, t2))
        }
        case Comp(m: CollectionMonoid, qs, proj) =>
          val inner = tipe(proj)
          makeNullable(te, Seq(CollectionType(m, inner)), qs.collect { case Gen(_, e1) => tipe(e1)})
        case Select(froms, d, g, proj, w, o, h) =>
          val inner = tipe(proj)
          // we don't care about the monoid here, sine we just walk the types to make them nullable or not, not the monoids
          makeNullable(te, Seq(CollectionType(SetMonoid(), inner)), froms.collect { case Iterator(_, e1) => tipe(e1)})
        case Reduce(m, g, e1) => makeNullable(te, Seq(), Seq(tipe(g.e)))
        case Filter(g, p) => makeNullable(te, Seq(), Seq(tipe(g.e)))
        case Join(g1, g2, p) => makeNullable(te, Seq(), Seq(tipe(g1.e), tipe(g2.e)))
        case OuterJoin(g1, g2, p) =>
          val x = makeNullable(te, Seq(), Seq(tipe(g1.e), tipe(g2.e)))
          x match {
            case CollectionType(_, RecordType(atts, _)) =>
              assert(atts.length == 2)
              atts(1).tipe.nullable = true
          }
          x
        case OuterUnnest(g1, g2, p) =>
          val x = makeNullable(te, Seq(), Seq(tipe(g1.e), tipe(g2.e)))
          x match {
            case CollectionType(_, RecordType(atts, _)) =>
              assert(atts.length == 2)
              atts(1).tipe.nullable = true
          }
          x
        case Nest(m, g, k, p, e1) => makeNullable(te, Seq(), Seq(tipe(g.e)))
        case Nest2(m, g, k, p, e1) => makeNullable(te, Seq(), Seq(tipe(g.e)))
        case Comp(_, qs, proj) =>
          val output_type = tipe(proj) match {
            case _: IntType => IntType()
            case _: FloatType => FloatType()
            case _: BoolType => BoolType()
            case NumberType(v) => NumberType(v)
          }
          output_type.nullable = false
          makeNullable(te, Seq(output_type), qs.collect { case Gen(_, e1) => tipe(e1)})
        case BinaryExp(_, e1, e2) => makeNullable(te, Seq(), Seq(tipe(e1), tipe(e2)))
        case InExp(e1, e2) => makeNullable(te, Seq(), Seq(tipe(e1), tipe(e2)))
        case UnaryExp(_, e1) => makeNullable(te, Seq(), Seq(tipe(e1)))
        case ExpBlock(_, e1) =>
          val t1 = tipe(e1)
          makeNullable(te, Seq(t1), Seq(t1))
        case _: IdnExp => te
        case _: IntConst => te
        case _: FloatConst => te
        case _: BoolConst => te
        case _: StringConst => te
        case _: RecordCons => te
        case _: ZeroCollectionMonoid => te
        case f: FunAbs =>
          val argType = makeNullable(walk(patternType(f.p)), Seq(), Seq(), Some(false))
          FunType(argType, tipe(f.e))
        case _: Partition =>
          // TODO: Ben: HELP!!!
          // Ben: I think it should inherit the nullables the related froms, something like that?
          te
        case Sum(e1) => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Max(e1) => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Min(e1) => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Avg(e1) => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Count(e1) => makeNullable(te, Seq(), Seq(tipe(e1)))
      }
      nt
    }
  }

  /** Type checker errors.
    */
  // TODO: Add check that the *root* type (and only the root type) does not contain ANY type variables, or we can't generate code for it
  // TODO: And certainly no NothingType as well...
  lazy val errors: Seq[Error] = {
    baseType(tree.root) // Must type the entire program before checking for errors
//    logger.debug(s"Final type map\n${typesVarMap.toString}")
//    logger.debug(s"Final monoid map\n${monoidsVarMap.toString}")

    badEntities ++ tipeErrors
  }

  private lazy val collectBadEntities =
    collect[List, Error] {
      // Identifier declared more than once in the same scope
      case i: IdnDef if entity(i) == MultipleEntity() =>
        MultipleDecl(i)

      // Identifier used without being declared
      case i: IdnUse if entity(i) == UnknownEntity() =>
        UnknownDecl(i)
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

  // TODO: Entity lookup based on attribute name
  //       Normally, s.name is a RecordProj of 's', which itself is an entity
  //       Now I'd see 'name'. This is not a user source, so looking up "data source" won't work.
  //       So I'd need to look in the env.in(n) environment for all that is there, and see what matches name (?)
  //       Actually, all that is in
  //         for ( <- students ) yield set name
  //       this could be sugar for
  //         for ( $44 <- students ) yield set $44.name
  //       but i need to find inconsistencies.
  //       in a sense, starting from
  //         for ( $44 <- students ) yield set name
  //       makes some sense but still, the entity of 'name' is what?
  //       i could introduce a new entity...
  //       ...and in the Desugar, replace that entity by the thing it points to.
  //       and in the type checker, get the type of the parent thing and apply the record proj to it...
  //       but that's like being a constrained record thing.
  //       The same mechanism should also support * e.g. for ( <- students, <- professors) yield list *
  //       Which makes me think that perhaps we should have a phase where the records are all anon, but there
  //       is a separate notion of Alias? also to cope with 'from students as s'?
  //       records would then be structs with aliases? not sure... Better forget it for now.

  private lazy val env: Chain[Environment] =
    chain(envin, envout)

  private def envin(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()

    // Entering new scopes
    case c: Comp => enter(in(c))
    case r: Reduce => enter(in(r))
    case f: Filter => enter(in(f))
    case j: Join => enter(in(j))
    case o: OuterJoin => enter(in(o))
    case o: OuterUnnest => enter(in(o))
    case n: Nest => enter(in(n))
    case n: Nest2 => enter(in(n))
    case s: Select => enter(in(s))
    case b: ExpBlock => enter(in(b))

    // If we are in a function abstraction, we must open a new scope for the variable argument. But if the parent is a
    // bind, then the `in` environment of the function abstraction must be the same as the `in` environment of the
    // bind.
    case tree.parent.pair(_: FunAbs, b: Bind) => enter(env.in(b))
    case f: FunAbs => enter(in(f))

    // If we are in an expression and the parent is a bind or a generator, then the `in` environment of the expression
    // is the same as that of the parent: the environment does not include the left hand side of the assignment.
    case tree.parent.pair(_: Exp, b: Bind) => env.in(b)
    case tree.parent.pair(_: Exp, g: Gen) => env.in(g)
    case tree.parent.pair(_: Exp, it: Iterator) => env.in(it)
  }

  private def envout(out: RawNode => Environment): RawNode ==> Environment = {
    // Leaving a scope
    case c: Comp => leave(out(c))
    case s: Select => leave(out(s))
    case r: Reduce => leave(out(r))
    case f: Filter => leave(out(f))
    case j: Join => leave(out(j))
    case o: OuterJoin => leave(out(o))
    case o: OuterUnnest => leave(out(o))
    case n: Nest => leave(out(n))
    case n: Nest2 => leave(out(n))
    case b: ExpBlock => leave(out(b))

    // The `out` environment of a function abstraction must remove the scope that was inserted.
    case f: FunAbs => leave(out(f))

    // A new variable was defined in the current scope.
    case n @ IdnDef(i) => define(out(n), i, entity(n))

    // The `out` environment of a bind or generator is the environment after the assignment.
    case Bind(p, _) => env(p)
    case Gen(p, _) => env(p)
    case Iterator(Some(p), _) => env(p)

    // Expressions cannot define new variables, so their `out` environment is always the same as their `in`
    // environment. The chain does not need to go "inside" the expression to finding any bindings.
    case e: Exp => env.in(e)
  }

  // TODO: Move this to the Types.scala and have it used inside the TypeScheme definition for uniformity!
  case class FreeSymbols(typeSyms: Set[Symbol], monoidSyms: Set[Symbol])

  /** Type the rhs of a Bind declaration.
    * If successful, returns a list of free type symbols and free monoid symbols (for polymorphism).
    */
  private lazy val tipeBind: Bind => Option[FreeSymbols] = attr {
    case Bind(p, e) =>
      // TODO: If the unresolved TypeVariables come from UserType/Source, don't include them as free variables.
      //       Instead, leave them unresolved, unless we want a strategy that resolves them based on usage?

      // Add all pattern identifier types to the map before processing the rhs
      // This call is repeated multiple times in case of a PatternProd on the lhs of the Bind. This is harmless.
      patternIdnTypes(p).foreach { case pt => typesVarMap.union(pt, pt) }

      // Collect all the roots known in the TypesVarMap.
      // This will be used to detect "new variables" created within, and not yet in the TypesVarMap.
      val prevTypeRoots = typesVarMap.getRoots
      val prevMonoidRoots = monoidsVarMap.getRoots

      // Type the rhs body of the Bind
      solve(constraints(e))
      val t = expType(e)
      val expected = patternType(p)
      if (!unify(t, expected)) {
        tipeErrors += UnexpectedType(walk(t), walk(expected), Some("Bind"), Some(e.pos))
        None
      } else {
        // Find all type variables used in the type
        val typeVars = getVariableTypes(t)
        val monoidVars = getVariableMonoids(t)

        // For all the "previous roots", get their new roots
        val prevTypeRootsUpdated = prevTypeRoots.map { case v => typesVarMap(v).root }
        val prevMonoidRootsUpdated = prevMonoidRoots.map { case v => monoidsVarMap(v).root }

        // Collect all symbols from variable types that were not in TypesVarMap before we started typing the body of the Bind.
        val freeTypeSyms = typeVars.collect { case vt: VariableType => vt }.filter { case vt => !prevTypeRootsUpdated.contains(vt) }.map(_.sym)
        val freeMonoidSyms = monoidVars.collect { case v: MonoidVariable => v }.filter { case v => !prevMonoidRootsUpdated.contains(v) }.map(_.sym)

        Some(FreeSymbols(freeTypeSyms, freeMonoidSyms))
      }
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
        case _: NothingType                                    => Set()
        case _: AnyType                                        => Set()
        case _: BoolType                                       => Set()
        case _: IntType                                        => Set()
        case _: FloatType                                      => Set()
        case _: StringType                                     => Set()
        case _: UserType                                       => Set()
        case RecordType(atts, _)                               => atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) }.toSet
        case CollectionType(_, innerType)                      => getVariableTypes(innerType, occursCheck + t)
        case FunType(p, e)                                     => getVariableTypes(p, occursCheck + t) ++ getVariableTypes(e, occursCheck + t)
        case t1: PrimitiveType                                 => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) getVariableTypes(typesVarMap(t1).root, occursCheck + t) else Set(t1)
        case t1: NumberType                                    => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) getVariableTypes(typesVarMap(t1).root, occursCheck + t) else Set(t1)
        case t1: TypeVariable                                  => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) getVariableTypes(typesVarMap(t1).root, occursCheck + t) else Set(t1)
        case t1 @ ConstraintRecordType(atts, _)                => Set(t1) ++ atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) }
      }
    }
  }

  private def getVariableMonoids(t: Type, occursCheck: Set[Type] = Set()): Set[MonoidVariable] = {
    if (occursCheck.contains(t))
      Set()
    else {
      t match {
        case _: NothingType => Set()
        case _: AnyType => Set()
        case _: BoolType => Set()
        case _: IntType => Set()
        case _: FloatType => Set()
        case _: StringType => Set()
        case _: UserType => Set()
        case _: PrimitiveType => Set()
        case _: NumberType => Set()
        case t1: TypeVariable                                  =>
          if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1)
            getVariableMonoids(typesVarMap(t1).root, occursCheck + t)
          else
            Set()
        case RecordType(atts, _) => atts.flatMap { case att => getVariableMonoids(att.tipe, occursCheck + t) }.toSet
        case CollectionType(m: MonoidVariable, innerType) =>
          (mFind(m) match {
            case m: MonoidVariable => Set(m)
            case _                 => Set()
          }) ++ getVariableMonoids(innerType, occursCheck + t)
        case CollectionType(_, innerType) => getVariableMonoids(innerType, occursCheck + t)
        case FunType(p, e) => getVariableMonoids(p, occursCheck + t) ++ getVariableMonoids(e, occursCheck + t)
        case ConstraintRecordType(atts, _) => atts.flatMap { case att => getVariableMonoids(att.tipe, occursCheck + t) }
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
        case i: Iterator             => Some(i)
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
        case i: Iterator             => Some(i)
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
          case Some(FreeSymbols(typeSyms, monoidSyms)) =>
            val ts = TypeScheme(t, typeSyms, monoidSyms)
            //              logger.debug(s"TypeScheme is ${PrettyPrinter(ts)} for ${CalculusPrettyPrinter(b)}")
            ts
          case None => NothingType()
        }
        case _ => t
      }
    case DataSourceEntity(sym) => world.sources(sym.idn)
    case _: UnknownEntity      => NothingType()
    case _: MultipleEntity     => NothingType()
  }

  /** Instantiate a new type from a type scheme.
    * Used for let-polymorphism.
    */
  private def instantiateTypeScheme(t: Type, typeSyms: Set[Symbol], monoidSyms: Set[Symbol]) = {
    val newSyms = scala.collection.mutable.HashMap[Symbol, Symbol]()

    def getNewSym(sym: Symbol): Symbol = {
      if (!newSyms.contains(sym))
        newSyms += (sym -> SymbolTable.next())
      newSyms(sym)
    }

    def getMonoid(m: CollectionMonoid): CollectionMonoid = {
      val mr = if (monoidsVarMap.contains(m)) monoidsVarMap(m).root else m
      mr match {
        case MonoidVariable(commutative, idempotent, sym) if monoidSyms.contains(sym) => MonoidVariable(commutative, idempotent, getNewSym(sym))
        case _ => mr
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
          // TODO: We seem to be missing a strategy to reconstruct constraint record types.
          //       We need to take care to only reconstruct if absolutely needed (if there is a polymorphic symbol somewhere inside?).
          case TypeVariable(sym)                              => TypeVariable(getNewSym(sym))
          case NumberType(sym)                                => NumberType(getNewSym(sym))
          case PrimitiveType(sym)                             => PrimitiveType(getNewSym(sym))
          case ConstraintRecordType(atts, sym)                => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, recurse(t1, occursCheck + t)) }, getNewSym(sym))
          case _: NothingType                                 => t
          case _: AnyType                                     => t
          case _: IntType                                     => t
          case _: BoolType                                    => t
          case _: FloatType                                   => t
          case _: StringType                                  => t
          case _: UserType                                    => t
          // TODO: Is this correct? Aren't we creating new instances of e.g. records or collections when they **might** not have type variables inside?
          //       If that is the case, the unification will be failing later on since those will no longer be the same instance.
          //       Or is it the case that if I got here is it because for sure there are type variables inside?  
          //       Say we have a record with 2 fields which are 2 collections: one of them has type vars the other doesn't. So should we just clone one? Or both?
          case RecordType(atts, name)                         => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, recurse(t1, occursCheck + t)) }, name)
          case c1 @ CollectionType(m, inner)                       =>
//            logger.debug(s"c1 is ${PrettyPrinter(c1)}")
//            logger.debug(s"typeSyms $typeSyms")
//            logger.debug(s"monoidSyms $monoidSyms")
            val nm = getMonoid(m)
//            logger.debug(s"nm $nm")
            CollectionType(nm, recurse(inner, occursCheck + t))
          case FunType(p, e)                                  => FunType(recurse(p, occursCheck + t), recurse(e, occursCheck + t))
        }
      }
    }

    recurse(t, Set())
  }

  private def idnType(idn: IdnNode): Type = entityType(entity(idn))

  /** The type corresponding to a given pattern.
    */
  private def patternType(p: Pattern): Type = p match {
    case PatternIdn(idn) =>
      entity(idn) match {
        case VariableEntity(_, t) => t
        case _                    => NothingType()
      }
    case PatternProd(ps) =>
      RecordType(ps.zipWithIndex.map { case (p1, idx) => AttrType(s"_${idx + 1}", patternType(p1)) }, None)
  }

  /** The type of an expression.
    * If the type cannot be immediately derived from the expression itself, then type variables are used for unification.
    */
  private lazy val expType: Exp => Type = attr {

    case p: Partition =>
      partitionSelect(p) match {
        case Some(s) => selectFromsTypeVar(s)
        case None    =>
          tipeErrors += UnknownPartition(p)
          NothingType()
      }

    // Rule 1
    case _: BoolConst  => BoolType()
    case _: IntConst   => IntType()
    case _: FloatConst => FloatType()
    case _: StringConst => StringType()

    // Rule 3
    case IdnExp(idn) =>
      idnType(idn) match {
        case TypeScheme(t, typeSyms, monoidSyms) =>
          if (typeSyms.isEmpty && monoidSyms.isEmpty)
            t
          else instantiateTypeScheme(t, typeSyms, monoidSyms)
        case t                   => t
      }

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, expType(att.e))), None)

    // Rule 7
    case FunAbs(p, e1) => FunType(patternType(p), expType(e1))

    // Rule 9
    case ZeroCollectionMonoid(_: BagMonoid) => CollectionType(BagMonoid(), TypeVariable())
    case ZeroCollectionMonoid(_: ListMonoid) => CollectionType(ListMonoid(), TypeVariable())
    case ZeroCollectionMonoid(_: SetMonoid) => CollectionType(SetMonoid(), TypeVariable())

    // Rule 10
    case ConsCollectionMonoid(_: BagMonoid, e1) => CollectionType(BagMonoid(), expType(e1))
    case ConsCollectionMonoid(_: ListMonoid, e1) => CollectionType(ListMonoid(), expType(e1))
    case ConsCollectionMonoid(_: SetMonoid, e1) => CollectionType(SetMonoid(), expType(e1))

    // Unary expressions
    case UnaryExp(_: Not, _)     => BoolType()
    case UnaryExp(_: ToBool, _)  => BoolType()
    case UnaryExp(_: ToInt, _)   => IntType()
    case UnaryExp(_: ToFloat, _) => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    // Sugar expressions
    case _: Count => IntType()

    case n => TypeVariable()
  }

  private def monoidsCompatible(m1: Monoid, m2: Monoid): Boolean =
    !((m1.commutative.isDefined && m2.commutative.isDefined && m1.commutative != m2.commutative) ||
      (m1.idempotent.isDefined && m2.idempotent.isDefined && m1.idempotent != m2.idempotent))

  private def unifyMonoids(m1: CollectionMonoid, m2: CollectionMonoid): Boolean = (mFind(m1), mFind(m2)) match {
    case (_: SetMonoid, _: SetMonoid) => true
    case (_: BagMonoid, _: BagMonoid) => true
    case (_: ListMonoid, _: ListMonoid) => true
    case (v1 @ MonoidVariable(c1, i1, _), v2 @ MonoidVariable(c2, i2, _)) if c1 == c2 && i1 == i2 =>
      monoidsVarMap.union(v1, v2)
      true
    case (v1 @ MonoidVariable(c1, i1, _), v2 @ MonoidVariable(c2, i2, _)) if monoidsCompatible(v1, v2) =>
      val nc = if (c1.isDefined) c1 else c2
      val ni = if (i1.isDefined) i1 else i2
      val nv = MonoidVariable(nc, ni)
      monoidsVarMap.union(v1, v2).union(v2, nv)
      true
    case (v1: MonoidVariable, x2) if monoidsCompatible(v1, x2) =>
      monoidsVarMap.union(v1, x2)
      true
    case (_, _: MonoidVariable) =>
      unifyMonoids(m2, m1)
    case _ =>
      false
  }

  /** Hindley-Milner unification algorithm.
    */
  private def unify(t1: Type, t2: Type): Boolean = {

    def recurse(t1: Type, t2: Type, occursCheck: Set[(Type, Type)]): Boolean = {
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
          if (!unifyMonoids(m1, m2)) {
            return false
          }
          recurse(inner1, inner2, occursCheck + ((t1, t2)))

        case (FunType(p1, e1), FunType(p2, e2)) =>
          recurse(p1, p2, occursCheck + ((t1, t2))) && recurse(e1, e2, occursCheck + ((t1, t2)))

        case (RecordType(atts1, name1), RecordType(atts2, name2)) if name1 == name2 && atts1.length == atts2.length && atts1.map(_.idn) == atts2.map(_.idn) =>
          atts1.zip(atts2).map { case (att1, att2) => recurse(att1.tipe, att2.tipe, occursCheck + ((t1, t2))) }.forall(identity)

        case (r1 @ ConstraintRecordType(atts1, _), r2 @ ConstraintRecordType(atts2, _)) =>
          val commonIdns = atts1.map(_.idn).intersect(atts2.map(_.idn))
          for (idn <- commonIdns) {
            val att1 = r1.getType(idn).head
            val att2 = r2.getType(idn).head
            if (!recurse(att1, att2, occursCheck + ((t1, t2)))) {
              return false
            }
          }
          val commonAttrs = commonIdns.map { case idn => AttrType(idn, r1.getType(idn).head) } // Safe to take from the first attribute since they were already unified in the new map
        val nt = ConstraintRecordType(atts1.filter { case att => !commonIdns.contains(att.idn) } ++ atts2.filter { case att => !commonIdns.contains(att.idn) } ++ commonAttrs)
          typesVarMap.union(r1, r2).union(r2, nt)
          true

        case (r1 @ ConstraintRecordType(atts1, _), r2 @ RecordType(atts2, name)) =>
          if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet)) {
            false
          } else {
            for (att1 <- atts1) {
              if (!recurse(att1.tipe, r2.getType(att1.idn).get, occursCheck + ((t1, t2)))) {
                return false
              }
            }
            typesVarMap.union(r1, r2)
            true
          }

        case (r1: RecordType, r2: ConstraintRecordType) =>
          recurse(r2, r1, occursCheck + ((t1, t2)))

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
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: IntType, _: PrimitiveType)     =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: FloatType, _: PrimitiveType)   =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: StringType, _: PrimitiveType)  =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))

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
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: IntType, _: NumberType)    =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))

        case (UserType(sym1), UserType(sym2)) if sym1 == sym2 =>
          true
        case (v1: TypeVariable, v2: TypeVariable) =>
          typesVarMap.union(v2, v1)
          true
        case (v1: TypeVariable, v2: VariableType) =>
          typesVarMap.union(v1, v2)
          true
        case (v1: VariableType, v2: TypeVariable) =>
          recurse(v2, v1, occursCheck + ((t1, t2)))
        case (v1: TypeVariable, _)                =>
          typesVarMap.union(v1, nt2)
          true
        case (_, v2: TypeVariable)                =>
          recurse(v2, nt1, occursCheck + ((t1, t2)))
        case _                                    =>
          false
      }
    }
    recurse(t1, t2, Set())
  }

  private def maxMonoid(ts: Seq[CollectionType]): CollectionMonoid = {
    val ms: Seq[CollectionMonoid] = ts.map(_.m).map(mFind)
    logger.debug(s"ms is $ms")
    ms.collectFirst { case m: SetMonoid => m }.getOrElse(
      ms.collectFirst { case m: BagMonoid => m }.getOrElse(
        ms.collectFirst { case m: ListMonoid => m }.get))
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
        val r = unify(t, expected)
        if (!r) {
          tipeErrors += UnexpectedType(walk(t), walk(expected), desc, Some(e.pos))
        }
        r

      case ExpMonoidSubsetOf(e, m) =>
        val t = expType(e)
        // Subset of monoid
        val rc = if (m.commutative.isDefined && m.commutative.get) None else m.commutative
        val ri = if (m.idempotent.isDefined && m.idempotent.get) None else m.idempotent
        val r = unify(t, CollectionType(MonoidVariable(rc, ri), TypeVariable()))
        if (!r) {
          tipeErrors += IncompatibleMonoids(m, walk(t), Some(e.pos))
        }
        r

      case MaxOfMonoids(s) =>
        val t = expType(s)
        find(t) match {
          case CollectionType(m, inner) =>
            val fromMs = s.from.map {
              case Iterator(p, e) =>
                val t1 = expType(e)
                find(t1) match {
                  case t: CollectionType => t
                  case _: NothingType    => return true
                }
            }
            val nm = maxMonoid(fromMs)
            m match {
              case _: MonoidVariable =>
                val r = unifyMonoids(m, nm)
                if (!r) {
                  // TODO: Fix error message: should have m and nm?
                  tipeErrors += IncompatibleMonoids(nm, walk(t), Some(s.pos))
                }
                r
              case _ =>
                val r = (m.commutative.get || !nm.commutative.get) && (m.idempotent.get || !nm.idempotent.get)
                if (!r) {
                  // TODO: Fix error message: should have m and nm?
                  tipeErrors += IncompatibleMonoids(nm, walk(t), Some(s.pos))
                }
                r
            }
          case _: NothingType => true
        }

      case PartitionHasType(s) =>
        logger.debug(s"PartitionHasType ${CalculusPrettyPrinter(s)}")
        val t = selectFromsTypeVar(s)
        val fromTypes = s.from.map { case f =>
          val e = f.e
          val t = expType(e)
          find(t) match {
            case t: CollectionType => t
            //case CollectionType(m, inner) => CollectionType(m, inner)
            case _ => return true
          }
        }
//        logger.debug(s"fromTypes $fromTypes")
        val t1 =
          if (fromTypes.length == 1)
            fromTypes.head
          else {
            val idns = s.from.map { case Iterator(Some(PatternIdn(IdnDef(idn))), _) => idn }
            CollectionType(maxMonoid(fromTypes), RecordType(idns.zip(fromTypes.map(_.innerType)).map { case (idn, innerType) => AttrType(idn, innerType) }, None))
          }
//        logger.debug(s"t1 is $t1")
//        logger.debug(s"t is $t")
        val r = unify(t, t1)
        if (!r) {
          tipeErrors += UnexpectedType(walk(t), walk(t1), None, Some(s.pos))
        }
        logger.debug(s"r $s tfind ${walk(t)} t1find ${walk(t1)}")
        r

      case InheritType(b @ Bind(p, e)) =>
        tipeBind(b) match {
          case Some(_: FreeSymbols) => true
          case _ => false
        }
    }

    cs match {
      case c :: rest =>
//        logger.debug(s"  solving $c")
        if (solver(c))
          solve(rest)
        else
          false
      case _ => true
    }

  }

  /** Given a type, returns a new type that replaces type variables as much as possible, given the map m.
    * This is the type representing the group of types.
    */
  private def find(t: Type): Type =
    if (typesVarMap.contains(t)) typesVarMap(t).root else t

  private def mFind(t: CollectionMonoid): CollectionMonoid =
    if (monoidsVarMap.contains(t)) monoidsVarMap(t).root else t

  /** Reconstruct the type by resolving all inner variable types as much as possible.
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
          case RecordType(atts, name)          => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, name)
          case CollectionType(m, innerType)    => CollectionType(mFind(m), reconstructType(innerType, occursCheck + t))
          case FunType(p, e)                   => FunType(reconstructType(p, occursCheck + t), reconstructType(e, occursCheck + t))
          case ConstraintRecordType(atts, sym) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, sym)
          case t1: TypeVariable => if (!typesVarMap.contains(t1)) t1 else reconstructType(pickMostRepresentativeType(typesVarMap(t1)), occursCheck + t)
        }
      }
      r.nullable = r.nullable || t.nullable
      r
    }

    reconstructType(t, Set())
  }

  /** Constraints of a node.
    * Each entry represents the constraints (aka. the facts) that a given node adds to the overall type checker.
    */
  // TODO: With Ben, we sort of agree that we should re-order constraints so that the stuff that provides more information goes first.
  // TODO: Typically this means HasType(e, IntType()) goes before a SameType(n, e). We believe this may impact the precision of error reporting.
  def constraint(n: RawNode): Seq[Constraint] = {
    import Constraint._

    n match {

      // Select
      case n @ Select(froms, d, g, proj, w, o, h) =>
        val m =
          if (o.isDefined)
            ListMonoid()
          else if (d)
            SetMonoid()
          else
            MonoidVariable()
        Seq(
          PartitionHasType(n),
          HasType(n, CollectionType(m, expType(proj))),
          MaxOfMonoids(n))

      // Rule 4
      case n @ RecordProj(e, idn) =>
        Seq(
          HasType(e, ConstraintRecordType(Set(AttrType(idn, expType(n))))))

      // Rule 6
      case n @ IfThenElse(e1, e2, e3) =>
        Seq(
          HasType(e1, BoolType(), Some("if condition must be a boolean")),
          SameType(e2, e3, Some("then and else must be of the same type")),
          SameType(n, e2))

      // Rule 8
      case n @ FunApp(f, e) =>
        Seq(
          HasType(f, FunType(expType(e), expType(n))))

      // Rule 11
      case n @ MergeMonoid(_: BoolMonoid, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          HasType(e1, BoolType()),
          HasType(e2, BoolType()))

      case n @ MergeMonoid(_: NumberMonoid, e1, e2) =>
        Seq(
          HasType(n, NumberType()),
          SameType(n, e1),
          SameType(e1, e2))

      // Rule 12
      case n @ MergeMonoid(_: CollectionMonoid, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, CollectionType(MonoidVariable(), TypeVariable())))

      // Rule 13
      case n @ Comp(m: NumberMonoid, qs, e) =>
        qs.collect { case Gen(_, e1) => e1 }.map(e => ExpMonoidSubsetOf(e, m)) ++
        Seq(
          HasType(e, NumberType()),
          SameType(n, e))

      case n @ Comp(m: BoolMonoid, qs, e)   =>
        Seq(
          HasType(e, BoolType()),
          SameType(n, e))

      // Rule 14
      case n @ Comp(m: CollectionMonoid, qs, e1) =>
        qs.collect { case Gen(_, e) => e }.map(e => ExpMonoidSubsetOf(e, m)) ++
          Seq(HasType(n, CollectionType(m, expType(e1))))

      // Binary Expression
      case n @ BinaryExp(_: EqualityOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e1, e2))

      case n @ BinaryExp(_: ComparisonOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e2, e1),
          HasType(e1, NumberType()))

      case n @ BinaryExp(_: ArithmeticOperator, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, NumberType()))

      // Binary Expression
      case n @ InExp(e1, e2) =>
        val inner = TypeVariable()
        Seq(
          HasType(e2, CollectionType(MonoidVariable(), inner)),
          HasType(e1, inner),
          HasType(n, BoolType()))

      // Unary Expression type

      case n @ UnaryExp(_: Not, e) =>
        Seq(
          SameType(n, e),
          HasType(e, BoolType()))

      case n @ UnaryExp(_: Neg, e) =>
        Seq(
          SameType(n, e),
          HasType(e, NumberType()))

      case n @ UnaryExp(_: ToBag, e) =>
        val inner = TypeVariable()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), inner)),
          HasType(n, CollectionType(BagMonoid(), inner)))

      case n @ UnaryExp(_: ToList, e) =>
        val inner = TypeVariable()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), inner)),
          HasType(n, CollectionType(ListMonoid(), inner)))

      case n @ UnaryExp(_: ToSet, e) =>
        val inner = TypeVariable()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), inner)),
          HasType(n, CollectionType(SetMonoid(), inner)))

      // Expression block type
      case n @ ExpBlock(_, e) =>
        Seq(
          SameType(n, e))

      // Declarations

      case Gen(p, e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), patternType(p))))

      case b @ Bind(p, e) =>
        Seq(
          InheritType(b))

      case Iterator(Some(p), e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), patternType(p))))

      // Operators

      case n @ Reduce(m: CollectionMonoid, g, e) =>
        Seq(
          HasType(n, CollectionType(m, expType(e))))

      case n @ Reduce(m: NumberMonoid, g, e) =>
        Seq(
          HasType(e, NumberType()),
          SameType(n, e))

      case n @ Reduce(m: BoolMonoid, g, e) =>
        Seq(
          HasType(e, BoolType()),
          HasType(n, BoolType()))

      case n @ Filter(g, p) =>
        Seq(
          SameType(n, g.e),
          HasType(p, BoolType()))

      case n @ Nest(rm: CollectionMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        Seq(
          HasType(g.e, CollectionType(m, TypeVariable())),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", expType(k)), AttrType("_2", CollectionType(rm, expType(e)))), None))))

      case n @ Nest2(rm: CollectionMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val inner = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, inner)),
          HasType(p, BoolType()),
          // the _1 type of the output record is the same as the child inner type, not the key type
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", inner), AttrType("_2", CollectionType(rm, expType(e)))), None))))

      case n @ Nest(rm: NumberMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val nt = NumberType()
        Seq(
          HasType(g.e, CollectionType(m, TypeVariable())),
          HasType(e, nt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", expType(k)), AttrType("_2", nt)), None))))

      case n @ Nest2(rm: NumberMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val nt = NumberType()
        val inner = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, inner)),
          HasType(e, nt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", inner), AttrType("_2", nt)), None))))

      case n @ Nest(rm: BoolMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val bt = BoolType()
        Seq(
          HasType(g.e, CollectionType(m, TypeVariable())),
          HasType(e, bt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", expType(k)), AttrType("_2", bt)), None))))

      case n @ Nest2(rm: BoolMonoid, g, k, p, e) =>
        val m = MonoidVariable()
        val bt = BoolType()
        val inner = TypeVariable()
        Seq(
          HasType(g.e, CollectionType(m, inner)),
          HasType(e, bt),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", inner), AttrType("_2", bt)), None))))

      case n @ Join(g1, g2, p) =>
        val t1 = TypeVariable()
        val t2 = TypeVariable()
        val m  = MonoidVariable()
        Seq(
          HasType(g1.e, CollectionType(m, t1)),
          HasType(g2.e, CollectionType(m, t2)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", t1), AttrType("_2", t2)), None))))

      case n @ OuterJoin(g1, g2, p) =>
        val t1 = TypeVariable()
        val t2 = TypeVariable()
        val m  = MonoidVariable()
        Seq(
          HasType(g1.e, CollectionType(m, t1)),
          HasType(g2.e, CollectionType(m, t2)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", t1), AttrType("_2", t2)), None))))

      case n @ OuterUnnest(g1, g2, p) =>
        val t1 = TypeVariable()
        val t2 = TypeVariable()
        val m  = MonoidVariable()
        Seq(
          HasType(g1.e, CollectionType(m, t1)),
          HasType(g2.e, CollectionType(MonoidVariable(), t2)),
          HasType(p, BoolType()),
          HasType(n, CollectionType(m, RecordType(Seq(AttrType("_1", t1), AttrType("_2", t2)), None))))

      case n @ Sum(e) =>
        val tn = NumberType()
        Seq(
          HasType(e, CollectionType(MonoidVariable(), tn)),
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
          HasType(e, CollectionType(MonoidVariable(), TypeVariable())),
          HasType(n, NumberType()))

      case _ =>
        Seq()
    }
  }

  /** Collects the constraints of an expression.
    * The constraints are returned in an "ordered sequence", i.e. the child constraints are collected before the current node's constraints.
    */
  // TODO: Move constraint(n) to top-level and apply it always at the end
  lazy val constraints: Exp => Seq[Constraint] = attr {
    case n @ Comp(_, qs, e) => qs.flatMap{ case e: Exp => constraints(e) case g @ Gen(_, e1) => constraints(e1) ++ constraint(g) case b @ Bind(_, e1) => constraint(b) } ++ constraints(e) ++ constraint(n)
    case n @ Select(from, _, g, proj, w, o, h) =>
      val fc = from.flatMap { case it @ Iterator(_, e) => constraints(e) ++ constraint(it) }
      val wc = if (w.isDefined) constraints(w.get) else Nil
      val gc = if (g.isDefined) constraints(g.get) else Nil
      val oc = if (o.isDefined) constraints(o.get) else Nil
      val hc = if (h.isDefined) constraints(h.get) else Nil
      fc ++ wc ++ gc ++ oc ++ hc ++ constraint(n) ++ constraints(proj)
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
    case n @ IfThenElse(e1, e2, e3) => constraints(e1) ++ constraints(e2) ++ constraints(e3) ++ constraint(n)
    case n: Partition => constraint(n)
    case n @ Reduce(m, g, e) => constraints(g.e) ++ constraint(g) ++ constraints(e) ++ constraint(n)
    case n @ Filter(g, p) => constraints(g.e) ++ constraint(g) ++ constraints(p) ++ constraint(n)
    case n @ Nest(m, g, k, p, e) => constraints(g.e) ++ constraint(g) ++ constraints(k) ++ constraints(e) ++ constraints(p) ++ constraint(n)
    case n @ Nest2(m, g, k, p, e) => constraints(g.e) ++ constraint(g) ++ constraints(k) ++ constraints(e) ++ constraints(p) ++ constraint(n)
    case n @ Join(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ OuterJoin(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ Unnest(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ OuterUnnest(g1, g2, p) => constraints(g1.e) ++ constraint(g1) ++ constraints(g2.e) ++ constraint(g2) ++ constraints(p) ++ constraint(n)
    case n @ Sum(e) => constraints(e) ++ constraint(n)
    case n @ Max(e) => constraints(e) ++ constraint(n)
    case n @ Min(e) => constraints(e) ++ constraint(n)
    case n @ Avg(e) => constraints(e) ++ constraint(n)
    case n @ Count(e) => constraints(e) ++ constraint(n)
  }

  /** Create a type variable for the FROMs part of a SELECT.
    * Used for unification in the PartitionHasType() constraint.
    */
  private lazy val selectFromsTypeVar: Select => Type = attr {
    _ => TypeVariable()
  }

  /** Walk up tree until we find a Select, if it exists.
    */
  private def findSelect(n: RawNode): Option[Select] = n match {
    case s: Select                   => Some(s)
    case n1 if tree.isRoot(n1)       => None
    case tree.parent.pair(_, parent) => findSelect(parent)
  }

  /** Parent Select of the current Select, if it exists.
    */
  private lazy val selectParent: Select => Option[Select] = attr {
    case n if tree.isRoot(n)         => None
    case tree.parent.pair(_, parent) => findSelect(parent)
  }

  /** Finds the Select that this partition refers to.
   */
  lazy val partitionSelect: Partition => Option[Select] = attr {
    case p =>

      // Returns true if `p` used in `e`
      def inExp(e: Exp) = {
        var found = false
        query[Exp] {
          case n if n eq p => found = true
        }(e)
        found
      }

      // Returns true if `p`p used in the from
      def inFrom(from: Seq[Iterator]): Boolean = {
        for (f <- from) {
          f match {
            case Iterator(_, e) => if (inExp(e)) return true
          }
        }
        false
      }

      findSelect(p) match {
        case Some(s) =>
          // The partition node is:
          // - used in the FROM;
          // - or in the GROUP BY;
          // - or there is no GROUP BY (which means it cannot possibly refer to our own Select node)
          if (inFrom(s.from) || (s.group.isDefined && inExp(s.group.get)) || s.group.isEmpty) {
            selectParent(s)
          } else {
            Some(s)
          }
        case None => None
      }
  }

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
// TODO: Add support for new Algebra nodes: in constraint and in constraints
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
// TODO: Add support for typing an expression like max(students.age) where students is a collection. Or even max(students.personal_info.age)
// TODO: If I do yield bag, I think I also constrain on what the input's commutativity and associativity can be!...
//       success("""\x -> for (y <- x) yield bag (y.age * 2, y.name)""", world,
// TODO: I should be able to do for (x <- col) yield f(x) to keep same collection type as in col
//       This should only happen for a single col I guess?. It helps write the map function.
// TODO: We also said that we need to drop ConstrainedRecordType. It will instead do a local scope lookup based on the field name and attach itself to that type.
//       This lookup will use a lazy val chain in Kiama and then also be re-used by the OQL Select parser when looking up (Select name) although here name is an Exp so it's not exatly the same.
//       This replaces the RecordProj constraint...
