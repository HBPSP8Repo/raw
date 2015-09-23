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
  private val unifyErrors =
    scala.collection.mutable.MutableList[Error]()

  /** Return the basic type of an expression.
    */
  lazy val data_tipe: Exp => Type = attr {
    e => {
      solve(constraints(e))
      walk(expType(e))
    }
  }

  private def make_nullable(source: Type, models: Seq[Type], nulls: Seq[Type], nullable: Option[Boolean]=None): Type = {
    val t = (source, models) match {
      case (col@CollectionType(m, i), colls: Seq[CollectionType]) => {
        val inners = colls.map(_.innerType)
        CollectionType(m, make_nullable(i, inners, inners, nullable))
      }
      case (f @ FunType(p, e), funs: Seq[FunType]) => {
        val otherP = funs.map(_.t1)
        val otherE = funs.map(_.t2)
        FunType(make_nullable(p, otherP, otherP, nullable), make_nullable(e, otherE, otherE, nullable))
      }
      case (r@RecordType(attr, n), recs: Seq[RecordType]) => {
        val attributes = attr.map { case AttrType(idn, i) => {
          val others = recs.map { r => r.getType(idn).get }
          AttrType(idn, make_nullable(i, others, others, nullable))
        }
        }
        RecordType(attributes, n)
      }
      case (_: IntType, _) => IntType()
      case (_: FloatType, _) => FloatType()
      case (_: BoolType, _) => BoolType()
      case (_: StringType, _) => StringType()
      case (u: UserType, _) => UserType(u.sym)
      case (v: TypeVariable, _) => TypeVariable(v.sym)
      case (v: NumberType, _) => NumberType(v.sym)
      case (r@ConstraintRecordType(attr, sym), recs: Seq[ConstraintRecordType]) => {
        val attributes = attr.map { case AttrType(idn, i) => {
          val others = recs.map { r => r.getType(idn).get }
          AttrType(idn, make_nullable(i, others, others, nullable))
        }
        }
        ConstraintRecordType(attributes, sym)
      }
    }
    t.nullable = nullable.getOrElse(t.nullable || nulls.collect{case t if t.nullable => t}.nonEmpty)
    t
  }

  /** Return the type of an expression (including option flag set).
    */
  lazy val tipe: Exp => Type = attr {
    e => {
      val te = data_tipe(e) // regular type (no option except from sources)
      val nt = e match {
        case RecordProj(e, idn) => tipe(e) match {
          case rt: RecordType => make_nullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
          case rt: ConstraintRecordType => make_nullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
          case ut: UserType => {
            typesVarMap(ut).root match {
              case rt: RecordType => make_nullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
            }
          }
        }
        case ConsCollectionMonoid(m, e) => CollectionType(m, tipe(e))
        case IfThenElse(e1, e2, e3) => (tipe(e1), tipe(e2), tipe(e3)) match {
          case (t1, t2, t3) => make_nullable(te, Seq(t2, t3), Seq(t1, t2, t3))
        }
        case FunApp(f, v) => tipe(f) match {
          case ft @ FunType(t1, t2) => make_nullable(te, Seq(t2), Seq(ft, t2, tipe(v)))
        }
        case MergeMonoid(_, e1, e2) => (tipe(e1), tipe(e2)) match {
          case (t1, t2) => make_nullable(te, Seq(t1, t2), Seq(t1, t2))
        }
        case Comp(m: CollectionMonoid, qs, proj) => {
          val inner = tipe(proj)
          make_nullable(te, Seq(CollectionType(m, inner)), qs.collect { case Gen(_, e) => tipe(e)})
        }
        case Select(froms, d, g, proj, w, o, h) => {
          val inner = tipe(proj)
          // we don't care about the monoid here, sine we just walk the types to make them nullable or not, not the monoids
          make_nullable(te, Seq(CollectionType(SetMonoid(), inner)), froms.collect { case Iterator(_, e) => tipe(e)})
        }
        case Comp(_, qs, proj) => {
          val output_type = tipe(proj) match {
            case _: IntType => IntType()
            case _: FloatType => FloatType()
            case _: BoolType => BoolType()
            case NumberType(v) => NumberType(v)
          }
          output_type.nullable = false
          make_nullable(te, Seq(output_type), qs.collect { case Gen(_, e) => tipe(e)})
        }
        case BinaryExp(_, e1, e2) => make_nullable(te, Seq(), Seq(tipe(e1), tipe(e2)))
        case UnaryExp(_, e1) => make_nullable(te, Seq(), Seq(tipe(e1)))
        case ExpBlock(_, e1) => {
          val t1 = tipe(e1)
          make_nullable(te, Seq(t1), Seq(t1))
        }
        case _: IdnExp => te
        case _: IntConst => te
        case _: FloatConst => te
        case _: BoolConst => te
        case _: StringConst => te
        case _: RecordCons => te
        case _: ZeroCollectionMonoid => te
        case f: FunAbs => {
          val argType = make_nullable(walk(patternType(f.p)), Seq(), Seq(), Some(false))
          FunType(argType, tipe(f.e))
        }
          // TODO: Ben: HELP!!!
        case _: Partition => te
      }
      logger.debug(s"${CalculusPrettyPrinter(e)} => ${TypesPrettyPrinter(nt)}")
      nt
    }
  }

  /** Type checker errors.
    */
  // TODO: Add check that the *root* type (and only the root type) does not contain ANY type variables, or we can't generate code for it
  // TODO: And certainly no NothingType as well...
  lazy val errors: Seq[Error] = {
    data_tipe(tree.root) // Must type the entire program before checking for errors
    logger.debug(s"Final map\n${typesVarMap.toString}")

    badEntities ++ unifyErrors ++ semanticErrors
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

  /** The semantic errors for the tree.
    */
//  private lazy val collectSemanticErrors = collect[List, Error] {

//    // The parent of a generator is a comprehension with incompatble monoid properties
//    case tree.parent.pair(g: Gen, c: Comp) if monoidsIncompatible(c.m, g).isDefined =>
//      monoidsIncompatible(c.m, g).head
//  }

//  private lazy val semanticErrors = collectSemanticErrors(tree.root)

  private lazy val semanticErrors = Seq()

//  /** Check whether monoid is compatible with the generator expression.
//    */
//  private def monoidsIncompatible(m: Monoid, g: Gen): Option[Error] = {
//    def errors(t: Type): Option[Error] = t match {
//      case t1: CollectionType =>
//         TODO: Verify this (e.g. is error on t or t1?) and implement CommutativeIdempotentRequired(t)
//        if (t1.m.commutative.head && !m.commutative.head) {
//          Some(CommutativeRequired(t))
//        } else if (t1.m.idempotent.head && !m.idempotent.head) {
//          Some(IdempotentRequired(t))
//        } else {
//          None
//        }
//      case UserType(sym)   =>
//        errors(world.tipes(sym))
//      case _               =>
//        Some(CollectionRequired(t))
//    }
//     TODO: simplify signature & inner method?
//    errors(tipe(g.e))
//  }

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

  /** Type the rhs of a Bind declaration.
    */
  private lazy val tipeBind: Bind => Type = attr {
    case Bind(p, e) =>
      solve(constraints(e))
      val t = expType(e)
      val expected = patternType(p)
      if (!unify(t, expected)) {
        unifyErrors += UnexpectedType(walk(t), walk(expected), Some("Bind"))
      }
      t
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

  /** Return the type of an entity.
    * Implements let-polymorphism.
    */
  private lazy val entityType: Entity => Type = attr {
    case VariableEntity(idn, t) => {

      // Go up until we find the declaration
      def getDecl(n: RawNode): Option[RawNode] = n match {
        case b: Bind                          => Some(b)
        case g: Gen                           => Some(g)
        case i: Iterator                      => Some(i)
        case f: FunAbs                        => Some(f)
        case tree.parent.pair(_: Pattern, n1) => getDecl(n1)
      }

      // TODO: If the unresolved TypeVariables come from UserType/Source, don't include them as free variables.
      //       Instead, leave them unresolved, unless we want a strategy that resolves them based on usage?

      // TODO: Polymorphic monoid variables

      val p = tree.parent(idn).head.asInstanceOf[Pattern] // Get the pattern
      val dcl = getDecl(p)
      dcl match {
        case Some(b: Bind)      =>
          // Add all pattern identifier types to the map before processing the rhs
          // This call is repeated multiple times in case of a PatternProd on the lhs of the Bind. This is harmless.
          patternIdnTypes(b.p).foreach{ case pt => typesVarMap.union(pt, pt)}

          // Collect all the roots known in the TypesVarMap.
          // This will be used to detect "new variables" created within, and not yet in the TypesVarMap.
          val prevRoots = typesVarMap.getRoots

          // Type the rhs body of the Bind
          val te = tipeBind(b)
          te match {
            case t: NothingType => t
            case _ =>
              // Find all type variables used in the type
              val vars = getVariableTypes(te)
              // For all the "previous roots", get their new roots
              val prevRootsUpdated = prevRoots.map { case v => typesVarMap(v).root }
              // Collect all symbols from variable types that were not in TypesVarMap before we started typing the body of the Bind.
              val freeSyms = vars.collect { case vt: VariableType => vt }.filter { case vt => !prevRootsUpdated.contains(vt) }.map(_.sym)
              TypeScheme(t, freeSyms)
          }
        case Some(g: Gen)       =>
          t
        case Some(it: Iterator) =>
          t
        case Some(f: FunAbs)                  =>
          // Add all pattern identifier types to the map before processing the rhs
          // This call is repeated multiple times in case of a PatternProd on the lhs of the Bind. This is harmless.
          patternIdnTypes(f.p).foreach{ case pt => typesVarMap.union(pt, pt)}
          t
      }
    }
    case DataSourceEntity(sym) => world.sources(sym.idn)
    case _: UnknownEntity      => NothingType()
    case _: MultipleEntity     => NothingType()
  }

  /** Instantiate a new type from a type scheme.
    * Used for let-polymorphism.
    */
  private def instantiateTypeScheme(t: Type, polymorphic: Set[Symbol]) = {
    val newSyms = scala.collection.mutable.HashMap[Symbol, Symbol]()

    def getNewSym(sym: Symbol): Symbol = {
      if (!newSyms.contains(sym))
        newSyms += (sym -> SymbolTable.next())
      newSyms(sym)
    }

    def recurse(t: Type, occursCheck: Set[Type]): Type = {
      if (occursCheck.contains(t))
        t
      else {
        t match {
          case t1 @ TypeVariable(sym) if !polymorphic.contains(sym)  => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) recurse(typesVarMap(t1).root, occursCheck + t) else t1
          case t1 @ NumberType(sym) if !polymorphic.contains(sym)    => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) recurse(typesVarMap(t1).root, occursCheck + t) else t1
          case t1 @ PrimitiveType(sym) if !polymorphic.contains(sym) => if (typesVarMap.contains(t1) && typesVarMap(t1).root != t1) recurse(typesVarMap(t1).root, occursCheck + t) else t1
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
          case CollectionType(m, inner)                       => CollectionType(m, recurse(inner, occursCheck + t))
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
        case _ => NothingType()
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
        case Some(s) => logger.debug("We got here!!!"); selectTypeVar(s)
        case None    => NothingType()
      }

    // Rule 1
    case _: BoolConst  => BoolType()
    case _: IntConst   => IntType()
    case _: FloatConst => FloatType()
    case _: StringConst => StringType()

    // Rule 3
    case IdnExp(idn) =>
      idnType(idn) match {
        case TypeScheme(t, vars) => if (vars.isEmpty) t else instantiateTypeScheme(t, vars)
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

    case n => TypeVariable()
  }

//  /** `m1` is a subset of `m2`
//    */
//  private def monoidSubsetOf(m1: Monoid, m2: Monoid): Boolean = {
//
//
//  }

  private def monoidsCompatible(m1: Monoid, m2: Monoid): Boolean =
    !((m1.commutative.isDefined && m2.commutative.isDefined && m1.commutative != m2.commutative) ||
      (m1.idempotent.isDefined && m2.idempotent.isDefined && m1.idempotent != m2.idempotent))

  private def unifyMonoids(m1: CollectionMonoid, m2: CollectionMonoid): Boolean = (m1, m2) match {
    case (_: SetMonoid, _: SetMonoid) => true
    case (_: BagMonoid, _: BagMonoid) => true
    case (_: ListMonoid, _: ListMonoid) => true
    case (v1 @ MonoidVariable(c1, i1, _), v2 @ MonoidVariable(c2, i2, _)) if c1 == c2 && i1 == i2 =>
      monoidsVarMap.union(v2, v1)
      true
    case (v1 @ MonoidVariable(c1, i1, _), v2 @ MonoidVariable(c2, i2, _)) if monoidsCompatible(m1, m2) =>
      val nc = if (c1.isDefined) c1 else c2
      val ni = if (i1.isDefined) i1 else i2
      val nv = MonoidVariable(nc, ni)
      monoidsVarMap.union(v1, v2).union(v2, nv)
      true
    case (v1: MonoidVariable, _) if monoidsCompatible(m1, m2) =>
      monoidsVarMap.union(v1, m2)
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
//      logger.debug(s"Unifying t1 ${TypesPrettyPrinter(t1)} and t2 ${TypesPrettyPrinter(t2)}")
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
          unifyErrors += IncompatibleTypes(walk(t1), walk(t2))
        }
        r
      case HasType(e, expected, desc) =>
        val t = expType(e)
        val r = unify(t, expected)
        if (!r) {
          unifyErrors += UnexpectedType(walk(t), walk(expected), desc)
        }
        r

      case ExpMonoidSubsetOf(e, m) =>
        val t = expType(e)
        find(t) match {
          case CollectionType(m1, _) =>
            // Subset of monoid
            val rc = if (m.commutative.isDefined && m.commutative.get) None else m.commutative
            val ri = if (m.idempotent.isDefined && m.idempotent.get) None else m.idempotent
            val r = unify(t, CollectionType(MonoidVariable(rc, ri), TypeVariable()))
            if (!r) {
              unifyErrors += IncompatibleMonoids(m, walk(t))
            }
            r
        }

      case PartitionHasType(s) =>
        val t = selectTypeVar(s)

        // TODO: This sounds like it's redundant code to a future type checker of the Join operator...

        def fromType(from: Seq[Iterator]): Type =
          if (from.length == 1) {
            val t = expType(from.head.e)
            find(t) match {
              case CollectionType(_, innerType) =>
                CollectionType(BagMonoid(), innerType)
              case _ =>
                unifyErrors += CollectionRequired(walk(t))
                NothingType()
            }
          }
          else if (from.length > 1) {
            val innerTypes = from.map { case f =>
              val t = expType(f.e)
              find(t) match {
                case CollectionType(_, innerType) =>
                  innerType
                case _ =>
                  unifyErrors += CollectionRequired(walk(t))
                  NothingType()
              }
            }
            CollectionType(BagMonoid(), RecordType(from.zip(innerTypes).map { case (Iterator(Some(PatternIdn(IdnDef(idn))), _), innerType) => AttrType(idn, innerType) }, None))
          } else
            ??? // Add error: we're using a partition over an empty generator(?)

        val t1 = fromType(s.from)
        logger.debug(s"t1 is $t1")
        logger.debug(s"t is $t")
        val r = unify(t, t1)
        if (!r) {
          unifyErrors += IncompatibleTypes(walk(t), walk(t1))
        }
        r
    }

    cs match {
      case c :: rest => if (solver(c)) solve(rest) else false
      case _ => true
    }

  }

  /** Given a type, returns a new type that replaces type variables as much as possible, given the map m.
    * This is the type representing the group of types.
    */
  private def find(t: Type): Type =
    if (typesVarMap.contains(t)) typesVarMap(t).root else t

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
          case CollectionType(m, innerType)    => CollectionType(if (monoidsVarMap.contains(m)) monoidsVarMap(m).root else m, reconstructType(innerType, occursCheck + t))
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
  def constraint(n: RawNode): Seq[Constraint] = {
    import Constraint._

    n match {

      case n @ Select(froms, d, g, proj, w, o, h) =>
        val m = if (o.isDefined) ListMonoid() else if (d) SetMonoid() else BagMonoid()
        Seq(
          PartitionHasType(n),
          HasType(n, CollectionType(m, expType(proj))))

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
//        ==> this one is broken because it puts n and e2 in the same gorup.
//    But if e2 contains an option, we blindly go inside it.
//    So we never put e2 in the gorup; we put the inner type of e2s option. So we are safe.
//
//        if e1 or e2 or e3 are options, then n is an option; but the type of n is the "inner type" - case of option of e2 and e3

        // if e1 is an option, then the result is an optionn
        // if either e2 or e3 are options, then the result is an option
        // and of course if both above hold, then the result is a single option (not an option of option :-))



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

//        btw, change order of things above; do e1 and e2 first.
//      if e1 or e2 are options, then n must be an option

      case n @ MergeMonoid(_: NumberMonoid, e1, e2) =>
        Seq(
          HasType(n, NumberType()),
          SameType(n, e1),
          SameType(e1, e2)
        )
//
//        SameType(e1, e2) is ok
//        but then, we need something like:
//        if e1 or e2 are options, then n is an option, but the type of n is the inner type of e1/e2

      // Rule 12
      case n @ MergeMonoid(_: CollectionMonoid, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, CollectionType(MonoidVariable(), TypeVariable()))
        )

      // Rule 13
      case n @ Comp(m: NumberMonoid, qs, e) =>
        qs.collect { case Gen(_, e) => e }.map(e => ExpMonoidSubsetOf(e, m)) ++
        Seq(
          HasType(e, NumberType()),
          SameType(n, e)
        )
      case n @ Comp(m: BoolMonoid, qs, e)   =>
        Seq(
          HasType(e, BoolType()),
          SameType(n, e)
        )

      // Rule 14
      case n @ Comp(m: CollectionMonoid, qs, e1) =>
        qs.collect { case Gen(_, e) => e }.map(e => ExpMonoidSubsetOf(e, m)) ++
          Seq(HasType(n, CollectionType(m, expType(e1))))

      // Binary Expression type
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

      // Unary Expression type
      case n @ UnaryExp(_: Neg, e) =>
        Seq(
          SameType(n, e),
          HasType(e, NumberType()))

      // Expression block type
      case n @ ExpBlock(_, e) =>
        Seq(
          SameType(n, e))

      case Gen(p, e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), patternType(p)))
        )

      case Iterator(Some(p), e) =>
        Seq(
          HasType(e, CollectionType(MonoidVariable(), patternType(p)))
        )

      case _ =>
        Seq()
    }
  }

  /** Collects the constraints of an expression.
    * The constraints are returned in an "ordered sequence", i.e. the child constraints are collected before the current node's constraints.
    */
  // TODO: Move constraint(n) to top-level and apply it always at the end
  lazy val constraints: Exp => Seq[Constraint] = attr {
    case n @ Comp(_, qs, e) => qs.flatMap{ case e: Exp => constraints(e) case g @ Gen(_, e1) => constraints(e1) ++ constraint(g) case _ => Nil } ++ constraints(e) ++ constraint(n)
    case n @ Select(from, _, g, proj, w, o, h) =>
      val fc = from.flatMap { case it @ Iterator(_, e) => constraints(e) ++ constraint(it) }
      val wc = if (w.isDefined) constraints(w.get) else Nil
      val gc = if (g.isDefined) constraints(g.get) else Nil
      val oc = if (o.isDefined) constraints(o.get) else Nil
      val hc = if (h.isDefined) constraints(h.get) else Nil
      fc ++ wc ++ gc ++ oc ++ hc ++ constraints(proj) ++ constraint(n)
    case n @ FunAbs(_, e) => constraints(e) ++ constraint(n)
    case n @ ExpBlock(_, e) => constraints(e) ++ constraint(n)
    case n @ MergeMonoid(_, e1, e2) => constraints(e1) ++ constraints(e2) ++ constraint(n)
    case n @ BinaryExp(_, e1, e2) => constraints(e1) ++ constraints(e2) ++ constraint(n)
    case n @ UnaryExp(_, e) => constraints(e) ++ constraint(n)
    case n: IdnExp => constraint(n)
    case n @ RecordProj(e, _) => constraints(e) ++ constraint(n)
    case n: Const => constraint(n)
    case n @ RecordCons(atts) => atts.flatMap { case att => constraints(att.e) } ++ constraint(n)
    case n @ FunApp(f, e) => constraints(f) ++ constraints(e) ++ constraint(n)
    case n @ ConsCollectionMonoid(_, e) => constraints(e) ++ constraint(n)
    case n @ IfThenElse(e1, e2, e3) => constraints(e1) ++ constraints(e2) ++ constraints(e3) ++ constraint(n)
    case n: Partition => constraint(n)
  }

  private lazy val selectTypeVar: Select => Type = attr {
    case Select(from, _, _, _, _, _, _) => TypeVariable()
  }

  private lazy val partitionSelect: Partition => Option[Select] = attr {
    case p =>
      def findSelect(n: RawNode): Option[Select] = n match {
        case s: Select                   => Some(s)
        case n if tree.isRoot(n)         => None
        case tree.parent.pair(_, parent) => findSelect(parent)
      }
      findSelect(p)
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
      var output = s"Type: ${TypesPrettyPrinter(t)}\n"
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
      case e: Exp => {
        val t = expType(e)
        if (!typesVarMap.contains(t)) {
          t -> e.pos
        } else {
          walk(typesVarMap(t).root) -> e.pos
        }
      }
    }

    for ((t, items) <- collectMaps(tree.root).groupBy(_._1)) {
      logger.debug(printMap(items.map(_._2).toSet, t))
    }

  }
}

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
// TODO: I think monoidsVarMap has to be handled during instantiateTypeScheme, just like monoidsVarMAp? Write test case that breaks 1st!!!
// TODO: We also said that we need to drop ConstrainedRecordType. It will instead do a local scope lookup based on the field name and attach itself to that type.
//       This lookup will use a lazy val chain in Kiama and then also be re-used by the OQL Select parser when looking up (Select name) although here name is an Exp so it's not exatly the same.
//       This replaces the RecordProj constraint...
