package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution
import org.kiama.rewriting.Strategy
import raw.World._

import scala.collection.GenTraversableOnce
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
class SemanticAnalyzer(val tree: Calculus.Calculus, world: World, val queryString: Option[String] = None) extends Attribution with LazyLogging {

  import scala.collection.immutable.Seq
  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.util.{Entity, MultipleEntity, UnknownEntity}
  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}
  import org.kiama.rewriting.Rewriter._
  import Calculus._
  import SymbolTable._
  import Constraint._
  import World.VarMap

  /** Decorators on the tree.
    */
  private lazy val decorators = new Decorators(tree)

  import decorators.{chain, Chain}

  /** The map of variables.
    * Updated during unification.
    */
  private val mappings = new VarMap(query = queryString)

  /** Add user types to the map.
    */
  for ((sym, t) <- world.tipes) {
    mappings.union(UserType(sym), t)
  }

  /** Add type variables from user sources.
    */
  for ((_, t) <- world.sources) {
    for (tv <- getVariableTypes(t)) {
      mappings.union(tv, tv)
    }
  }

  /** Stores unification errors.
    * Updated during unification.
    */
  private val unifyErrors =
    scala.collection.mutable.MutableList[Error]()

  /** Return the type of an expression.
    */
  lazy val tipe: Exp => Type = attr {
    e => {
      solve(constraints(e))
      walk(expType(e))
    }
  }

  /** Type checker errors.
    */
  // TODO: Add check that the *root* type (and only the root type) does not contain ANY type variables, or we can't generate code for it
  // TODO: And certainly no NothingType as well...
  lazy val errors: Seq[Error] = {
    tipe(tree.root) // Must type the entire program before checking for errors
    logger.debug(s"Final map\n${mappings.toString}")

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
  private lazy val collectSemanticErrors = collect[List, Error] {

    // The parent of a generator is a comprehension with incompatble monoid properties
    case tree.parent.pair(g: Gen, c: Comp) if monoidsIncompatible(c.m, g).isDefined =>
      monoidsIncompatible(c.m, g).head
  }

  private lazy val semanticErrors = collectSemanticErrors(tree.root)

  /** Check whether monoid is compatible with the generator expression.
    */
  private def monoidsIncompatible(m: Monoid, g: Gen): Option[Error] = {
    def errors(t: Type): Option[Error] = t match {
      case _: SetType  =>
        if (!m.commutative && !m.idempotent)
          Some(CommutativeIdempotentRequired(t))
        else if (!m.commutative)
          Some(CommutativeRequired(t))
        else if (!m.idempotent)
          Some(IdempotentRequired(t))
        else
          None
      case _: BagType  =>
        if (!m.commutative)
          Some(CommutativeRequired(t))
        else
          None
      case _: ListType =>
        None
      case _: ConstraintCollectionType =>
        None
      case UserType(sym)   =>
        errors(world.tipes(sym))
      case _: TypeVariable =>
        None
      case _               =>
        Some(CollectionRequired(t))
    }
    // TODO: simplify signature & inner method?
    errors(tipe(g.e))
  }

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

  private lazy val env: Chain[Environment] =
    chain(envin, envout)

  private def envin(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()

    // Entering new scopes
    case c: Comp => enter(in(c))
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
  }

  private def envout(out: RawNode => Environment): RawNode ==> Environment = {
    // Leaving a scope
    case c: Comp => leave(out(c))
    case b: ExpBlock => leave(out(b))

    // The `out` environment of a function abstraction must remove the scope that was inserted.
    case f: FunAbs => leave(out(f))

    // A new variable was defined in the current scope.
    case n @ IdnDef(i) => define(out(n), i, entity(n))

    // The `out` environment of a bind or generator is the environment after the assignment.
    case Bind(p, _) => env(p)
    case Gen(p, _) => env(p)

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

  /** Type the rhs of a Gen declaration.
    */
  private lazy val tipeGen: Gen => Type = attr {
    case Gen(p, e) =>
      solve(constraints(e))
      val t = expType(e)
      val expected = ConstraintCollectionType(patternType(p), None, None)
      if (!unify(t, expected)) {
        unifyErrors += UnexpectedType(walk(t), walk(expected), Some("Generator"))
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
        case ListType(innerType)                               => getVariableTypes(innerType, occursCheck + t)
        case SetType(innerType)                                => getVariableTypes(innerType, occursCheck + t)
        case BagType(innerType)                                => getVariableTypes(innerType, occursCheck + t)
        case FunType(p, e)                                     => getVariableTypes(p, occursCheck + t) ++ getVariableTypes(e, occursCheck + t)
        case t1: PrimitiveType                                 => if (mappings.contains(t1) && mappings(t1).root != t1) getVariableTypes(mappings(t1).root, occursCheck + t) else Set(t1)
        case t1: NumberType                                    => if (mappings.contains(t1) && mappings(t1).root != t1) getVariableTypes(mappings(t1).root, occursCheck + t) else Set(t1)
        case t1: TypeVariable                                  => if (mappings.contains(t1) && mappings(t1).root != t1) getVariableTypes(mappings(t1).root, occursCheck + t) else Set(t1)
        case t1 @ ConstraintRecordType(atts, _)                => Set(t1) ++ atts.flatMap { case att => getVariableTypes(att.tipe, occursCheck + t) }
        case t1 @ ConstraintCollectionType(innerType, _, _, _) => Set(t1) ++ getVariableTypes(innerType, occursCheck + t)
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
        case f: FunAbs                        => Some(f)
        case tree.parent.pair(_: Pattern, n1) => getDecl(n1)
      }

      // TODO: If the unresolved TypeVariables come from UserType/Source, don't include them as free variables.
      //       Instead, leave them unresolved, unless we want a strategy that resolves them based on usage?

      val p = tree.parent(idn).head.asInstanceOf[Pattern] // Get the pattern
      val dcl = getDecl(p)
      dcl match {
        case Some(b: Bind)      =>
          // Add all pattern identifier types to the map before processing the rhs
          // This call is repeated multiple times in case of a PatternProd on the lhs of the Bind. This is harmless.
          patternIdnTypes(b.p).foreach{ case pt => mappings.union(pt, pt)}

          // Collect all the roots known in the VarMap.
          // This will be used to detect "new variables" created within, and not yet in the VarMap.
          val prevRoots = mappings.getRoots

          // Type the rhs body of the Bind
          val te = tipeBind(b)
          te match {
            case t: NothingType => t
            case _ =>
              // Find all type variables used in the type
              val vars = getVariableTypes(te)
              // For all the "previous roots", get their new roots
              val prevRootsUpdated = prevRoots.map { case v => mappings(v).root }
              // Collect all symbols from variable types that were not in VarMap before we started typing the body of the Bind.
              val freeSyms = vars.collect { case vt: VariableType => vt }.filter { case vt => !prevRootsUpdated.contains(vt) }.map(_.sym)
              TypeScheme(t, freeSyms)
          }
        case Some(g: Gen)       =>
          // Add all pattern identifier types to the map before processing the rhs
          // This call is repeated multiple times in case of a PatternProd on the lhs of the Bind. This is harmless.
          patternIdnTypes(g.p).foreach{ case pt => mappings.union(pt, pt)}

          // Type the rhs body of the Gen
          val te = tipeGen(g)
          // TODO: Implement TypeScheme support? Add test case that needs it first...
          t
        case Some(f: FunAbs)                  =>
          // Add all pattern identifier types to the map before processing the rhs
          // This call is repeated multiple times in case of a PatternProd on the lhs of the Bind. This is harmless.
          patternIdnTypes(f.p).foreach{ case pt => mappings.union(pt, pt)}
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
          case t1 @ TypeVariable(sym) if !polymorphic.contains(sym)  => if (mappings.contains(t1) && mappings(t1).root != t1) recurse(mappings(t1).root, occursCheck + t) else t1
          case t1 @ NumberType(sym) if !polymorphic.contains(sym)    => if (mappings.contains(t1) && mappings(t1).root != t1) recurse(mappings(t1).root, occursCheck + t) else t1
          case t1 @ PrimitiveType(sym) if !polymorphic.contains(sym) => if (mappings.contains(t1) && mappings(t1).root != t1) recurse(mappings(t1).root, occursCheck + t) else t1
          // TODO: We seem to be missing a strategy to reconstruct constraint record types.
          //       We need to take care to only reconstruct if absolutely needed (if there is a polymorphic symbol somewhere inside?).
          case TypeVariable(sym)                              => TypeVariable(getNewSym(sym))
          case NumberType(sym)                                => NumberType(getNewSym(sym))
          case PrimitiveType(sym)                             => PrimitiveType(getNewSym(sym))
          case ConstraintRecordType(atts, sym)                => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, recurse(t1, occursCheck + t)) }, getNewSym(sym))
          case ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(recurse(innerType, occursCheck + t), c, i, getNewSym(sym))
          case _: NothingType                                 => t
          case _: AnyType                                     => t
          case _: IntType                                     => t
          case _: BoolType                                    => t
          case _: FloatType                                   => t
          case _: StringType                                  => t
          case _: UserType                                    => t
          case RecordType(atts, name)                         => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, recurse(t1, occursCheck + t)) }, name)
          case BagType(inner)                                 => BagType(recurse(inner, occursCheck + t))
          case SetType(inner)                                 => SetType(recurse(inner, occursCheck + t))
          case ListType(inner)                                => ListType(recurse(inner, occursCheck + t))
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
    case ZeroCollectionMonoid(_: BagMonoid) => BagType(TypeVariable())
    case ZeroCollectionMonoid(_: ListMonoid) => ListType(TypeVariable())
    case ZeroCollectionMonoid(_: SetMonoid) => SetType(TypeVariable())

    // Rule 10
    case ConsCollectionMonoid(_: BagMonoid, e1) => BagType(expType(e1))
    case ConsCollectionMonoid(_: ListMonoid, e1) => ListType(expType(e1))
    case ConsCollectionMonoid(_: SetMonoid, e1) => SetType(expType(e1))

    // Rule 14
    case Comp(_: BagMonoid, _, e1) => BagType(expType(e1))
    case Comp(_: ListMonoid, _, e1) => ListType(expType(e1))
    case Comp(_: SetMonoid, _, e1) => SetType(expType(e1))

    // Unary expressions
    case UnaryExp(_: Not, _)     => BoolType()
    case UnaryExp(_: ToBool, _)  => BoolType()
    case UnaryExp(_: ToInt, _)   => IntType()
    case UnaryExp(_: ToFloat, _) => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case n => TypeVariable()
  }

  /** Hindley-Milner unification algorithm.
    */
  private def unify(t1: Type, t2: Type): Boolean = {

    def recurse(t1: Type, t2: Type, occursCheck: Set[(Type, Type)]): Boolean = {
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

        case (SetType(inner1), SetType(inner2)) =>
          recurse(inner1, inner2, occursCheck + ((t1, t2)))
        case (BagType(inner1), BagType(inner2)) =>
          recurse(inner1, inner2, occursCheck + ((t1, t2)))
        case (ListType(inner1), ListType(inner2)) =>
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
          mappings.union(r1, r2).union(r2, nt)
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
            mappings.union(r1, r2)
            true
          }

        case (r1: RecordType, r2: ConstraintRecordType) =>
          recurse(r2, r1, occursCheck + ((t1, t2)))

        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2 @ ConstraintCollectionType(inner2, c2, i2, _)) =>
          if (c1.isDefined && c2.isDefined && (c1.get != c2.get)) {
            false
          } else if (i1.isDefined && i2.isDefined && (i1.get != i2.get)) {
            false
          } else {
            if (!recurse(inner1, inner2, occursCheck + ((t1, t2)))) {
              false
            } else {
              val nc = if (c1.isDefined) c1 else c2
              val ni = if (i1.isDefined) i1 else i2
              val nt = ConstraintCollectionType(inner1, nc, ni)
              mappings.union(col1, col2).union(col2, nt)
              true
            }
          }

        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: SetType)  =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && i1.get) || i1.isEmpty)) {
            if (!recurse(inner1, col2.innerType, occursCheck + ((t1, t2)))) {
              false
            } else {
              mappings.union(col1, col2)
              true
            }
          } else {
            false
          }
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: BagType)  =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty)) {
            if (!recurse(inner1, col2.innerType, occursCheck + ((t1, t2)))) {
              false
            } else {
              mappings.union(col1, col2)
              true
            }
          } else {
            false
          }
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: ListType) =>
          if (((c1.isDefined && !c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty)) {
            if (!recurse(inner1, col2.innerType, occursCheck + ((t1, t2)))) {
              false
            } else {
              mappings.union(col1, col2)
              true
            }
          } else {
            false
          }

        case (col1: CollectionType, col2: ConstraintCollectionType) =>
          recurse(col2, col1, occursCheck + ((t1, t2)))

        case (p1: PrimitiveType, p2: PrimitiveType) =>
          mappings.union(p2, p1)
          true
        case (p1: PrimitiveType, _: BoolType)   =>
          mappings.union(p1, nt2)
          true
        case (p1: PrimitiveType, _: IntType)    =>
          mappings.union(p1, nt2)
          true
        case (p1: PrimitiveType, _: FloatType)  =>
          mappings.union(p1, nt2)
          true
        case (p1: PrimitiveType, _: StringType) =>
          mappings.union(p1, nt2)
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
          mappings.union(p2, p1)
          true
        case (p1: NumberType, _: FloatType) =>
          mappings.union(p1, nt2)
          true
        case (p1: NumberType, _: IntType)   =>
          mappings.union(p1, nt2)
          true
        case (_: FloatType, _: NumberType)  =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: IntType, _: NumberType)    =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))

        case (UserType(sym1), UserType(sym2)) if sym1 == sym2 =>
          true
        case (v1: TypeVariable, v2: TypeVariable) =>
          mappings.union(v2, v1)
          true
        case (v1: TypeVariable, v2: VariableType) =>
          mappings.union(v1, v2)
          true
        case (v1: VariableType, v2: TypeVariable) =>
          recurse(v2, v1, occursCheck + ((t1, t2)))
        case (v1: TypeVariable, _)                =>
          mappings.union(v1, nt2)
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
    if (mappings.contains(t)) mappings(t).root else t

  /** Reconstruct the type by resolving all inner variable types as much as possible.
    */
  private def walk(t: Type): Type = {

    def pickMostRepresentativeType(g: Group): Type = {
      val ut = g.tipes.collectFirst { case u: UserType => u }
      ut match {
        case Some(picked) =>
          // Prefer user type
          picked
        case None =>
          val ct = g.tipes.find { case _: VariableType => false; case _ => true }
          ct match {
            case Some(picked) =>
              // Otherwise, prefer a final - i.e. non-variable - type
              picked
            case None =>
              val vt = g.tipes.find { case _: TypeVariable => false; case _ => true }
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
      if (occursCheck.contains(t)) {
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
          case _: PrimitiveType                => if (!mappings.contains(t)) t else reconstructType(pickMostRepresentativeType(mappings(t)), occursCheck + t)
          case _: NumberType                   => if (!mappings.contains(t)) t else reconstructType(pickMostRepresentativeType(mappings(t)), occursCheck + t)
          case RecordType(atts, name)          => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, name)
          case ListType(innerType)             => ListType(reconstructType(innerType, occursCheck + t))
          case SetType(innerType)              => SetType(reconstructType(innerType, occursCheck + t))
          case BagType(innerType)              => BagType(reconstructType(innerType, occursCheck + t))
          case FunType(p, e)                   => FunType(reconstructType(p, occursCheck + t), reconstructType(e, occursCheck + t))
          case ConstraintRecordType(atts, sym) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, sym)
          case ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(reconstructType(innerType, occursCheck + t), c, i, sym)
          case t1: TypeVariable => if (!mappings.contains(t1)) t1 else reconstructType(pickMostRepresentativeType(mappings(t1)), occursCheck + t)
        }
      }
    }

    reconstructType(t, Set())
  }

  /** Constraints of a node.
    * Each entry represents the constraints (aka. the facts) that a given node adds to the overall type checker.
    */
  def constraint(n: Exp): Seq[Constraint] = {
    import Constraint._

    n match {
      // Rule 4
      case RecordProj(e, idn) =>
        Seq(
          HasType(e, ConstraintRecordType(Set(AttrType(idn, expType(n))))))

      // Rule 6
      case IfThenElse(e1, e2, e3) =>
        Seq(
          HasType(e1, BoolType(), Some("if condition must be a boolean")),
          SameType(e2, e3, Some("then and else must be of the same type")),
          SameType(n, e2))

      // Rule 8
      case FunApp(f, e) =>
        Seq(
          HasType(f, FunType(expType(e), expType(n))))

      // Rule 11
      case MergeMonoid(_: BoolMonoid, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          HasType(e1, BoolType()),
          HasType(e2, BoolType()))

      case MergeMonoid(_: NumberMonoid, e1, e2) =>
        Seq(
          HasType(n, NumberType()),
          SameType(n, e1),
          SameType(e1, e2)
        )

      // Rule 12
      case MergeMonoid(_: CollectionMonoid, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, ConstraintCollectionType(TypeVariable(), None, None))
        )

      // Rule 13
      case Comp(_: NumberMonoid, _, e) =>
        Seq(
          HasType(e, NumberType()),
          SameType(n, e)
        )
      case Comp(_: BoolMonoid, _, e)   =>
        Seq(
          HasType(e, BoolType()),
          SameType(n, e)
        )

      // Binary Expression type
      case BinaryExp(_: EqualityOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e1, e2))

      case BinaryExp(_: ComparisonOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e2, e1),
          HasType(e1, NumberType()))

      case BinaryExp(_: ArithmeticOperator, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, NumberType()))

      // Unary Expression type
      case UnaryExp(_: Neg, e) =>
        Seq(
          SameType(n, e),
          HasType(e, NumberType()))

      // Expression block type
      case ExpBlock(_, e) =>
        Seq(
          SameType(n, e))

      case _ =>
        Seq()
    }
  }

  /** Collects the constraints of an expression.
    * The constraints are returned in an "ordered sequence", i.e. the child constraints are collected before the current node's constraints.
    */
  lazy val constraints: Exp => Seq[Constraint] = attr {
    case n @ Comp(_, qs, e) => qs.flatMap{ case e: Exp => constraints(e) case _ => Nil } ++ constraints(e) ++ constraint(n)
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
        if (!mappings.contains(t)) {
          t -> e.pos
        } else {
          walk(mappings(t).root) -> e.pos
        }
      }
    }

    for ((t, items) <- collectMaps(tree.root).groupBy(_._1)) {
      logger.debug(printMap(items.map(_._2).toSet, t))
    }

  }

}

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
// TODO: Re-do Unnester to use the same tree. Refactor code into new package raw.core
// TODO: Add support for typing an expression like max(students.age) where students is a collection. Or even max(students.personal_info.age)
// TODO: If I do yield bag, I think I also constrain on what the input's commutativity and associativity can be!...
//       success("""\x -> for (y <- x) yield bag (y.age * 2, y.name)""", world,
// TODO: I should be able to do for (x <- col) yield f(x) to keep same collection type as in col
//       This should only happen for a single col I guess?. It helps write the map function.
