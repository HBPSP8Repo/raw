package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution

case class SemanticAnalyzerError(err: String) extends RawException(err)

/** Analyzes the semantics of an AST.
  * This includes the type checker/inference as well as monoid compatibility.
  *
  * The semantic analyzer reads the user types and the catalog of user-defined class entities from the World object.
  * User types are the type definitions available to the user.
  * Class entities are the data sources available to the user.
  * e.g., in expression `e <- Events` the class entity is `Events`.
  *
  * The original user query is passed optionally for debugging purposes.
  */
class SemanticAnalyzer(val tree: Calculus.Calculus, world: World, val query: Option[String] = None) extends Attribution with LazyLogging {

  import scala.collection.immutable.{Map, Seq}
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
      case _: SetType =>
        if (!m.commutative && !m.idempotent)
          Some(CommutativeIdempotentRequired(t))
        else if (!m.commutative)
          Some(CommutativeRequired(t))
        else if (!m.idempotent)
          Some(IdempotentRequired(t))
        else
          None
      case _: BagType =>
        if (!m.commutative)
          Some(CommutativeRequired(t))
        else
          None
      case _: ListType =>
        None
      case _: ConstraintCollectionType =>
        None
      case UserType(sym) =>
        errors(world.tipes(sym))
      case _: TypeVariable =>
        None
      case _ =>
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

  private lazy val entityType: Entity => Type = attr {
    case VariableEntity(idn, t) => t
    case DataSourceEntity(sym) => world.sources(sym.idn)
    case _: UnknownEntity => NothingType()
  }

  private def idnType(idn: IdnNode): Type = entityType(entity(idn))

  /** The type corresponding to a given pattern.
    */
  private def patternType(p: Pattern): Type = p match {
    case PatternIdn(idn) => entity(idn) match {
      case VariableEntity(_, t) => t
      case _ => NothingType()
    }
    case PatternProd(ps) => RecordType(ps.zipWithIndex.map { case (p1, idx) => AttrType(s"_${idx + 1}", patternType(p1)) }, None)
  }

  private def typeWithPos(t: Type, e: Exp): Type = {
    t.pos = e.pos
    t
  }
//
//  def cloneType(t: Type): Type = {
//    val m = scala.collection.mutable.Map[Symbol, TypeVariable]()
//
//    def recurse(t: Type): Type = t match {
//      case _: NothingType => t
//      case _: AnyType => t
//      case _: PrimitiveType => t
//      case RecordType(atts, name) => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, cloneType(t1)) }, name)
//      case ListType(innerType) => ListType(cloneType(innerType))
//      case SetType(innerType) => SetType(cloneType(innerType))
//      case BagType(innerType) => BagType(cloneType(innerType))
//      case FunType(t1, t2) => FunType(cloneType(t1), cloneType(t2))
//      case ConstraintRecordType(atts, sym) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, cloneType(t1)) }, sym)
//      case ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(cloneType(innerType), c, i, sym)
//      case t1: TypeVariable =>
//        if (!m.contains(t1.sym)) {    // Clone the type variable
//          m += (t1.sym -> TypeVariable())
//        }
//        m(t1.sym)
//    }
//
//    recurse(t)
//  }

  /** The type of an expression.
    * If the type cannot be immediately derived from the expression itself, then type variables are used for unification.
    */
  private lazy val expType: Exp => Type = attr {
    case e => typeWithPos(expType1(e), e)
  }

  private def expType1(e: Exp): Type = e match {

    // Rule 1
    case _: BoolConst => BoolType()
    case _: IntConst => IntType()
    case _: FloatConst => FloatType()
    case _: StringConst => StringType()

    // Rule 3
    case IdnExp(idn) => idnType(idn)

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, expType(att.e))), None)

    // Rule 7
    case FunAbs(p, e1) => FunType(patternType(p), expType(e1), constraints(e1))

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
    case UnaryExp(_: Not, _) => BoolType()
    case UnaryExp(_: ToBool, _) => BoolType()
    case UnaryExp(_: ToInt, _) => IntType()
    case UnaryExp(_: ToFloat, _) => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case n => TypeVariable()
  }

  /** Hindley-Milner unification algorithm.
    */
  private def unify(t1: Type, t2: Type, m: VarMap): Either[VarMap, VarMap] = {

    def recurse(t1: Type, t2: Type, m: VarMap, occursCheck: Set[(Type, Type)]): Either[VarMap, VarMap] = {
      logger.debug(s"recurse t1 ${TypesPrettyPrinter(t1)} t2 ${TypesPrettyPrinter(t2)} occursCheck ${occursCheck}")
      if (occursCheck.contains((t1, t2)))
        return Right(m)
      val nt1 = find(t1, m)
      val nt2 = find(t2, m)
      (nt1, nt2) match {
          // TODO: Add NothingType

        case (_: AnyType, t) =>
          Right(m)
        case (t, _: AnyType) =>
          Right(m)

        case (_: IntType, _: IntType) =>
          Right(m)
        case (_: BoolType, _: BoolType) =>
          Right(m)
        case (_: FloatType, _: FloatType) =>
          Right(m)
        case (_: StringType, _: StringType) =>
          Right(m)

        case (SetType(inner1), SetType(inner2)) =>
          Right(recurse(inner1, inner2, m, occursCheck + ((t1, t2))) match { case Right(nm) => nm case Left(nm) => return Left(nm) })
        case (BagType(inner1), BagType(inner2)) =>
          Right(recurse(inner1, inner2, m, occursCheck + ((t1, t2))) match { case Right(nm) => nm case Left(nm) => return Left(nm) })
        case (ListType(inner1), ListType(inner2)) =>
          Right(recurse(inner1, inner2, m, occursCheck + ((t1, t2))) match { case Right(nm) => nm case Left(nm) => return Left(nm) })

//        case (FunType(p1, e1, cs1), FunType(p2, e2, cs2)) =>
//          recurse(p1, p2, m, occursCheck + ((t1, t2))) match {
//            case Right(nm) => recurse(e1, e2, nm, occursCheck + ((t1, t2))) match {
//              case Right(nm1) => Right(nm1)
//              case Left(nm1) => Left(nm1)
//            }
//            case Left(nm) => Left(nm)
//          }

        case (RecordType(atts1, name1), RecordType(atts2, name2)) if name1 == name2 && atts1.length == atts2.length && atts1.map(_.idn) == atts2.map(_.idn) =>
          var curm = m
          for ((att1, att2) <- atts1.zip(atts2)) {
            recurse(att1.tipe, att2.tipe, curm, occursCheck + ((t1, t2))) match {
              case Right(nm) => curm = nm
              case Left(nm) => return Left(nm)
            }
          }
          Right(curm)
        case (r1 @ ConstraintRecordType(atts1, _), r2 @ ConstraintRecordType(atts2, _)) =>
          val commonIdns = atts1.map(_.idn).intersect(atts2.map(_.idn))
          var curm = m
          for (idn <- commonIdns) {
            val att1 = r1.getType(idn).head
            val att2 = r2.getType(idn).head
            recurse(att1, att2, curm, occursCheck + ((t1, t2))) match {
              case Right(nm) => curm = nm
              case Left(nm) => return Left(nm)
            }
          }
          val commonAttrs = commonIdns.map { case idn => AttrType(idn, r1.getType(idn).head) } // Safe to take from the first attribute since they were already unified in the new map
          val nt = ConstraintRecordType(atts1.filter { case att => !commonIdns.contains(att.idn) } ++ atts2.filter { case att => !commonIdns.contains(att.idn) } ++ commonAttrs)
          Right(curm.union(r1, r2).union(r2, nt))
        case (r1 @ ConstraintRecordType(atts1, _), r2 @ RecordType(atts2, name)) =>
          if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet)) {
            Left(m)
          } else {
            var curm = m
            for (att1 <- atts1) {
              recurse(att1.tipe, r2.getType(att1.idn).get, curm, occursCheck + ((t1, t2))) match {
                case Right(nm) => curm = nm
                case Left(nm) => return Left(nm)
              }
            }
            Right(curm.union(r1, r2))
          }
        case (r1: RecordType, r2: ConstraintRecordType) =>
          recurse(r2, r1, m, occursCheck + ((t1, t2)))
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2 @ ConstraintCollectionType(inner2, c2, i2, _)) =>
          if (c1.isDefined && c2.isDefined && (c1.get != c2.get)) {
            Left(m)
          } else if (i1.isDefined && i2.isDefined && (i1.get != i2.get)) {
            Left(m)
          } else {
            recurse(inner1, inner2, m, occursCheck + ((t1, t2))) match {
              case Right(nm) =>
                val nc = if (c1.isDefined) c1 else c2
                val ni = if (i1.isDefined) i1 else i2
                val nt = ConstraintCollectionType(inner1, nc, ni)
                Right(nm.union(col1, col2).union(col2, nt))
              case Left(nm) => Left(nm)
            }
          }
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: SetType) =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && i1.get) || i1.isEmpty))
            recurse(inner1, col2.innerType, m, occursCheck + ((t1, t2))) match {
              case Right(nm) => Right(nm.union(col1, col2))
              case Left(nm) => Left(nm)
            }
          else
            Left(m)
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: BagType) =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty))
            recurse(inner1, col2.innerType, m, occursCheck + ((t1, t2))) match {
              case Right(nm) => Right(nm.union(col1, col2))
              case Left(nm) => Left(nm)
            }
          else
            Left(m)
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: ListType) =>
          if (((c1.isDefined && !c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty))
            recurse(inner1, col2.innerType, m, occursCheck + ((t1, t2))) match {
              case Right(nm) => Right(nm.union(col1, col2))
              case Left(nm) => Left(nm)
            }
          else
            Left(m)
        case (col1: CollectionType, col2: ConstraintCollectionType) =>
          recurse(col2, col1, m, occursCheck + ((t1, t2)))


        case (c1 @ ConstraintFunType(apps1, _), c2 @ ConstraintFunType(apps2, _)) =>
          val nc = ConstraintFunType(apps1 ++ apps2)
          Right(m.union(c1, c2).union(c2, nc))
        case (c1 @ ConstraintFunType(apps1, _), FunType(p2, e2, cs)) =>
//
//          def patternCompatible(t: Type, p: Pattern): Boolean = ???
//
//          for ((t1, e) <- apps1) {
//            if (patternCompatible(t1, p2)) {
//
//              duplicate cs (replace p2 types by corresponding t1)
//
//
//            } else
//              return Left(m)
//          }
//
//          call solve on

          ???

        case (FunType(p1, e1, cs1), FunType(p2, e2, cs2)) =>
          ???
        case (f: FunType, c2: ConstraintFunType) =>
          recurse(c2, f, m, occursCheck + ((t1, t2)))

//        case (ConstraintSchemeType(p1, c1s), ConstraintSchemeType(p2, c2s)) => {
//          (p1, p2) match {
//            case (_: PatternIdn, _: PatternProd) => return Left(m)
//            case (_: PatternProd, _: PatternIdn) => return Left(m)
//            case (p1: PatternProd, p2: PatternProd) =>
//              rewrite c2s with replace p1.seq with p2.seq
//              remap both to ConstraintSchemeType(p1, both constraints)
//        }


        case (p1: PrimitiveType, p2: PrimitiveType) =>
          Right(m.union(p2, p1))
        case (p1: PrimitiveType, _: BoolType) =>
          Right(m.union(p1, t2))
        case (p1: PrimitiveType, _: IntType) =>
          Right(m.union(p1, t2))
        case (p1: PrimitiveType, _: FloatType) =>
          Right(m.union(p1, t2))
        case (p1: PrimitiveType, _: StringType) =>
          Right(m.union(p1, t2))
        case (_: BoolType, _: PrimitiveType) =>
          recurse(t2, t1, m, occursCheck + ((t1, t2)))
        case (_: IntType, _: PrimitiveType) =>
          recurse(t2, t1, m, occursCheck + ((t1, t2)))
        case (_: FloatType, _: PrimitiveType) =>
          recurse(t2, t1, m, occursCheck + ((t1, t2)))
        case (_: StringType, _: PrimitiveType) =>
          recurse(t2, t1, m, occursCheck + ((t1, t2)))

        case (p1: NumberType, p2: NumberType) =>
          Right(m.union(p2, p1))
        case (p1: NumberType, _: FloatType) =>
          Right(m.union(p1, t2))
        case (p1: NumberType, _: IntType) =>
          Right(m.union(p1, t2))
        case (_: FloatType, _: NumberType) =>
          recurse(t2, t1, m, occursCheck + ((t1, t2)))
        case (_: IntType, _: NumberType) =>
          recurse(t2, t1, m, occursCheck + ((t1, t2)))

        case (UserType(sym1), UserType(sym2)) if sym1 == sym2 =>
          Right(m)
        case (v1: TypeVariable, v2: TypeVariable) =>
          Right(m.union(v2, v1))
        case (v1: TypeVariable, v2: VariableType) =>
          Right(m.union(v1, v2))
        case (v1: VariableType, v2: TypeVariable) =>
          recurse(v2, v1, m, occursCheck + ((t1, t2)))
        case (v1: TypeVariable, _) =>
          Right(m.union(v1, nt2))
        case (_, v2: TypeVariable) =>
          recurse(v2, nt1, m, occursCheck + ((t1, t2)))
        case _ =>
          Left(m)
      }
    }
    logger.debug(s"ENTER unify(${PrettyPrinter(t1)}, ${PrettyPrinter(t2)})")
    val r = recurse(t1, t2, m, Set())
    logger.debug(s"EXIT unify ${PrettyPrinter(t1)}, ${PrettyPrinter(t2)}")
    r match {
      case Right(m) => logger.debug("RESULT is Right"); printVarMap(m)
      case Left(m) => logger.debug("RESULT is Left"); printVarMap(m)
    }

    r
  }

  /** Return a set with all type variables within a type.
    */
  private def getTypeVariables(t: Type): Set[TypeVariable] = t match {
    case _: NothingType => Set()
    case _: AnyType => Set()
    case _: BoolType => Set()
    case _: IntType => Set()
    case _: FloatType => Set()
    case _: StringType => Set()
    case _: PrimitiveType => Set()
    case _: NumberType => Set()
    case _: UserType => Set()   // TODO: Is this correct? Does it matter? Does getTypeVariables/getConstraintTypeVariables even matter anyway???
    case RecordType(atts, _) => atts.flatMap { case att => getTypeVariables(att.tipe) }.toSet
    case ListType(innerType) => getTypeVariables(innerType)
    case SetType(innerType) => getTypeVariables(innerType)
    case BagType(innerType) => getTypeVariables(innerType)
    case FunType(p, e, _) => getTypeVariables(p) ++ getTypeVariables(e)
    case ConstraintRecordType(atts, _) => atts.flatMap { case att => getTypeVariables(att.tipe) }
    case ConstraintCollectionType(innerType, _, _, _) => getTypeVariables(innerType)
    case t1: TypeVariable => Set(t1)
  }

  /** Return a set with all type variables in a constraint.
    */
  private def getConstraintTypeVariables(c: Constraint): Set[TypeVariable] = c match {
    case And(cs @ _*) =>
      cs.flatMap(c => getConstraintTypeVariables(c)).toSet
    case SameType(t1, t2, _) =>
      getTypeVariables(t1) ++ getTypeVariables(t2)
    case HasType(t, expected, _) =>
      getTypeVariables(t) ++ getTypeVariables(expected)
    case NoConstraint => Set()
  }

  /** Type Checker constraint solver.
    * Takes a constraint and a given map of variable types (e.g. type variables, constraint types), and returns either:
    * - a new map, if a solution to the constraint was found;
    * - or a map of the solution found so far plus a list of error messages, if the constraint could not be resolved.
    */
  private def solve(c: Constraint, m: VarMap): Either[(VarMap, Seq[Error]), VarMap] = {

    logger.debug(s"Processing constraint: $c")
    logger.debug("Input map:")
    //logger.debug(m.map { case (v: Symbol, t: Type) => s"${v.idn} => ${PrettyPrinter(t)}" }.mkString("{\n", ",\n", "}"))

    c match {
      case And(cs @ _*) if cs.nonEmpty =>
        // TODO: Handle 1st constraint of basic types (e.g. 1 = 1.1)
        // Group constraints into two groups: those that have type variables and those that do not.
        val g = cs.groupBy(c => getConstraintTypeVariables(c).intersect(m.typeVariables).nonEmpty)
        // The next constraint to solve is always a constraint with type variables, if it exists.
        // TODO: Prefer constraints with more type variables?
        val next = if (g.contains(true)) g(true).head else g(false).head
        // Solve that constraint and call ourselves recursively with the rest of the And constraints
        solve(next, m) match {
          case Left(nm) =>
            Left(nm)
          case Right(nm) =>
            val rest = And(cs.filterNot(c => c == next):_*)
            solve(rest, nm)
        }
      case _: And => // Empty And
        Right(m)
      case SameType(t1, t2, desc) =>
        unify(t1, t2, m) match {
          case Right(nm) =>
            Right(nm)
          case Left(nm) =>
            Left(nm, List(IncompatibleTypes(walk(t1, nm), walk(t2, nm))))
        }
      case HasType(t, expected, desc) =>
        unify(t, expected, m) match {
          case Right(nm) =>
            Right(nm)
          case Left(nm) =>
            Left(nm, List(UnexpectedType(walk(t, nm), walk(expected, nm), desc)))
        }
      case NoConstraint => Right(m)
    }
  }

//
  // maybe fix the code before to be in a state where things run: comment out new funtype-related code but don't delete it as we'll need it shortly.
//we were talking about adding a PrimitiveType and a NumberType.
//  Both are VariableTypes.
//    In unify, they constrained to be more narrow in the obvious, left as an exercise to the implementor.
//    We then drop the Or and change the constraints to use Primitive/Number type.
//  Then we change the return type of solve to always return a single Map. Or goes away, And is trivial.
//    Then run the tests and should work as today.
//    Then, we implement the ConstraintFun unifying with Fun. It simply calls the solver itself,
//  (after carefulyl replacing the types in the constraints - see notes on the unify code), and returns
//  the map of the solve.
//  Add a TODO for the future: if we ever want to sum a List[Int] with an Int,
//  we could do multiple passes to the root. In that case, in the expType of the sum,
//  we'd check the lhs and rhs; if they hve been resolved to final types (.e.g List[Int] and Int)
//  then the expType would itself hardcode Int. The rest of the solver works as before; we just might need
//  to run it in multiple passes until it converges, i.e. until types do not narrow any more.
//



 //  and return a single map everywhere
//

//  private lazy val applyConstraints =
//    collect[List, Constraint] {
//      case b: Bind => constraint(b)
//      case g: Gen => constraint(g)
//      case e: Exp => constraint(e)
//    }

  /** The root constraint is an And of all constraints imposed by each node in the expression.
    */
  private lazy val rootConstraint =
    flattenAnds(
      Constraint.And(
        constraints(tree.root)
          .filter {
            case NoConstraint => false
            case _ => true }:_*))

  /** Flatten all occurrences on nested And constraints.
    */
  private def flattenAnds(c: Constraint): Constraint = c match {
    case And(cs @ _*) => And(cs.map(flattenAnds).flatMap{
      case And(cs1 @ _*) => cs1
      case _ => Seq(c)
    }:_*)
    case c_ => c
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

  private lazy val solutions: Either[(VarMap, Seq[Error]), VarMap] = {
    var m: VarMap = new VarMap(query = query)
    for ((sym, t) <- world.tipes) {
      m = m.union(UserType(sym), t)
    }
    solve(rootConstraint, m)
  }

  lazy val errors: Seq[Error] =
    if (badEntities.nonEmpty)
      badEntities
    else {
      logger.debug("Root constraint:")
      logger.debug(rootConstraint.toString)

      // TODO: Add check that the *root* type (and only the root type) does not contain ANY type variables, or we can't generate code for it
      // TODO: And certainly no NothingType if we still need it.

      solutions match {
        // All is OK so check for semantic errors (i.e. run the error checking phase that requires final types)
        case Right(m) => semanticErrors

        // The type checking errors
        case Left((m, errs)) => errs
      }
    }

  /** Given a type, returns a new type that replaces type variables as much as possible, given the map m.
    * This is the type representing the group of types.
    */
  private def find(t: Type, m: VarMap): Type =
    if (m.contains(t)) m(t).root else t

  /** Reconstruct the type into a user-friendly type: in practice this means resolving any "inner" type variables
    * and giving preference to UserType when they exist.
    */
  private def walk(t: Type, m: VarMap): Type = {

    def pickMostRepresentativeType(t: Type): Type =
      if (m.contains(t)) {
        // Collect a UserType definition if it exists.
        // We assume the UserType is the most friendly one to return to the user.
        val g = m(t)
        val userType = g.tipes.collectFirst { case u: UserType => u }
        userType match {
          case Some(ut) =>
            // We found a UserType, so let's return it
            ut
          case None =>
            // If we could not find a UserType, try to return anything that is not a VariableType if it exists.
            // Otherwise, we are forced to return the VariableType (e.g. TypeVariable, Constraint, ...).
            val ts = g.tipes.filter { case _: VariableType => false case _ => true }
            val nt = if (ts.nonEmpty) ts.head else g.root
            nt
        }
      } else
        t

    def reconstructType(ot: Type, occursCheck: Set[Type]): Type = {
      logger.debug(s"ot is $ot");
      if (occursCheck.contains(ot)) {
        ot
      } else {
        val t = pickMostRepresentativeType(ot)
        t match {
          case _: NothingType   => t
          case _: AnyType       => t
          case _: IntType       => t
          case _: BoolType      => t
          case _: FloatType     => t
          case _: StringType    => t
          case _: PrimitiveType => if (!m.contains(t)) t else reconstructType(m(t).root, occursCheck + ot)
          case _: NumberType    => if (!m.contains(t)) t else reconstructType(m(t).root, occursCheck + ot)
          case _: UserType      => t
          case RecordType(atts, name) => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + ot)) }, name)
          case ListType(innerType) => ListType(reconstructType(innerType, occursCheck + ot))
          case SetType(innerType)  => SetType(reconstructType(innerType, occursCheck + ot))
          case BagType(innerType)  => BagType(reconstructType(innerType, occursCheck + ot))
          case FunType(p, e, cs)   => FunType(reconstructType(p, occursCheck + ot), reconstructType(e, occursCheck + ot), cs)
          // TODO: Shouldn't I walk ALL Variable types?
          case ConstraintRecordType(atts, sym) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + ot)) }, sym)
          case ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(reconstructType(innerType, occursCheck + ot), c, i, sym)
          case t1: TypeVariable => if (!m.contains(t1)) t1 else reconstructType(m(t1).root, occursCheck + ot)
        }
      }
    }

    reconstructType(t, Set())
  }

  def printVarMap(m: VarMap) = logger.debug(m.toString())

  /** Return the type of an expression.
    */
  def tipe(e: Exp): Type = solutions match {
    case Right(m) =>
      printVarMap(m)
      walk(expType(e), m)
    case Left(m) => throw SemanticAnalyzerError("No solutions found")
  }

  /** Constraints of a node.
    * Each entry represents the constraints (aka. the facts) that a given node adds to the overall type checker.
    */
  lazy val constraint: RawNode => Constraint = {
    case n => val c = constraint1(n); c.pos = n.pos; c
  }

  lazy val constraint1: RawNode => Constraint = {
    import Constraint._
    attr {

      // Rule 4
      case n @ RecordProj(e, idn) =>
        HasType(expType(e), ConstraintRecordType(Set(AttrType(idn, expType(n)))))

      // Rule 6
      case n @ IfThenElse(e1, e2, e3) =>
        And(
          HasType(expType(e1), BoolType(), Some("if condition must be a boolean")),
          SameType(expType(e2), expType(e3), Some("then and else must be of the same type")),
          SameType(expType(n), expType(e2)))

      // Rule 8
      case n @ FunApp(f, e) =>
        HasType(expType(f), ConstraintFunType(Seq((expType(e), expType(n)))))

      // Rule 11
      case n @ MergeMonoid(_: BoolMonoid, e1, e2) =>
        And(
          HasType(expType(n), BoolType()),
          HasType(expType(e1), BoolType()),
          HasType(expType(e2), BoolType()))

      case n @ MergeMonoid(_: NumberMonoid, e1, e2) =>
        And(
          HasType(expType(n), NumberType()),
          SameType(expType(n), expType(e1)),
          SameType(expType(e1), expType(e2))
        )

      // Rule 12
      case n @ MergeMonoid(_: CollectionMonoid, e1, e2) =>
        And(
          SameType(expType(n), expType(e1)),
          SameType(expType(e1), expType(e2)),
          HasType(expType(e2), ConstraintCollectionType(TypeVariable(), None, None))
        )

      // Rule 13
      case n @ Comp(m: PrimitiveMonoid, _, e) =>
        // TODO: Add constraint saying it is primitive
        SameType(expType(n), expType(e))

      // Binary Expression type
      case n @ BinaryExp(_: EqualityOperator, e1, e2) =>
        And(
          HasType(expType(n), BoolType()),
          SameType(expType(e1), expType(e2)))

      case n @ BinaryExp(_: ComparisonOperator, e1, e2) =>
        And(
          HasType(expType(n), BoolType()),
          SameType(expType(e2), expType(e1)),
          HasType(expType(e1), NumberType()))

      case n @ BinaryExp(_: ArithmeticOperator, e1, e2) =>
        And(
          SameType(expType(n), expType(e1)),
          SameType(expType(e1), expType(e2)),
          HasType(expType(e2), NumberType()))

      // Unary Expression type
      case n @ UnaryExp(_: Neg, e) =>
        And(
          SameType(expType(n), expType(e)),
          HasType(expType(e), NumberType()))

      // Expression block type
      case n @ ExpBlock(_, e) =>
        SameType(expType(n), expType(e))

      // Generator
      case n @ Gen(p, e) =>
        HasType(expType(e), ConstraintCollectionType(patternType(p), None, None))

      // Bind
      case n @ Bind(p, e) =>
        HasType(expType(e), patternType(p))

      case n =>
        NoConstraint
    }
  }

  lazy val constraints: RawNode => Seq[Constraint] = attr {
//    case f: FunApp =>
//      // Let-polymorphism
//      val orig = constraints1(i)
//
    case n => constraints1(n)
  }

  def constraints1(n: RawNode): Seq[Constraint] = {
    val cs = collect[Seq, Constraint] {
      case n: RawNode => constraint(n)
    }
    cs(n)
  }

}

// TODO: Inferring FunAbs in the context of a FunApp. Add simple FunApp tests first!
// TODO: Make one SemanticAnalyzer test per function.
// TODO: Sort out the SemanticAnalyzer tests: there are useful tests there that are commented out since we didn't match properly (e.g. on errors)
// TODO: Add more tests to the SemanticAnalyzer with the intuit of testing the error reporting: the error messages may not yet be the most clear.
// TODO: Report unrelated errors by returning multiple Lefts - likely setting things to NothingType and letting them propagate.
//       Related with ths one, is the notion of whether Left(...) on solve should bother to return the bad map, since all we
//       actually need are the error messages.
// TODO: Consider adding syntax like: fun f(x) -> if (x = 0) then 1 else f(x - 1) * x)
//       It should just type to FunType(IntType(), IntType().
//       It is not strictly needed but the notion of a NamedFunction may help code-generation because these are things we don't inline/consider inlining,
// TODO: Do we need to add a closure check, or is that executor-specific?
// TODO: Add check to forbit polymorphic recursion (page 377 or 366 of ML Impl book)
// TODO: Add issue regarding polymorphic code generation:
//       e.g. if Int, Bool on usage generate 2 versions of the method;
//       more interestingly, if ConstraintRecordType, find out the actual records used and generate versions for those.
// TODO: Add notion of declaration. Bind and NamedFunc are now declarations. ExpBlock takes sequence of declarations followed by an expression.
// TODO: Re-do Unnester to use the same tree. Refactor code into new package raw.core
// TODO: Add support for typing an expression like max(students.age) where students is a collection. Or even max(students.personal_info.age)

