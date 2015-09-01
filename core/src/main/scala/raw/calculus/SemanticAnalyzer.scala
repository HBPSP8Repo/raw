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
  */
class SemanticAnalyzer(val tree: Calculus.Calculus, world: World) extends Attribution with LazyLogging {

  import scala.collection.immutable.{Map, Seq}
  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.util.{Entity, MultipleEntity, UnknownEntity}
  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}
  import org.kiama.rewriting.Rewriter._
  import Calculus._
  import SymbolTable._
  import Constraint._

  /** Decorators on the tree.
    */
  private lazy val decorators = new Decorators(tree)

  import decorators.{chain, Chain}

  /** The semantic errors for the tree.
    */
  private lazy val collectSemanticErrors = collect[List, Error] {

    // TODO: Add check that final types are all inferred.
    // TODO: No type variables?
    // TODO: Certainly no NothingType

    // Semantic error in monoid composition
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
      case _: TypeVariable =>
        None
      case _ =>
        Some(CollectionRequired(t))
    }

    // TODO: figure out if we should go the walk here or not. Here yes, for sure.
    tipe(g.e) match {
      case UserType(t) => errors(world.userTypes(t))
      case t => errors(t)
    }
  }

  /** Looks up the identifier in the World and returns a new entity instance.
    */
  private def lookupDataSource(idn: String): Entity =
    if (world.sources.contains(idn))
      DataSourceEntity(idn)
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
    case DataSourceEntity(name) => world.sources(name)
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

  /** The type of an expression.
    * If the type cannot be immediately derived from the expression itself, then type variables are used for unification.
    */
  private lazy val expType: Exp => Type = attr {
    case e =>
      val t = expType1(e)
      t.pos = e.pos
      t
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
    case UnaryExp(_: Not, _) => BoolType()
    case UnaryExp(_: ToBool, _) => BoolType()
    case UnaryExp(_: ToInt, _) => IntType()
    case UnaryExp(_: ToFloat, _) => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case n => TypeVariable()
  }

  type VarMap = Map[String, Type]

  /** Hindley-Milner unification algorithm.
    */
  private def unify(t1: Type, t2: Type): Either[VarMap, VarMap] = {

    def recurse(t1: Type, t2: Type, m: VarMap): Either[VarMap, VarMap] = {
      (t1, t2) match {
        case (_: AnyType, t) =>
          Right(m)
        case (t, _: AnyType) =>
          Right(m)
        case (u: UserType, _) =>
          recurse(world.userTypes(u.idn), t2, m)
        case (_, u: UserType) =>
          recurse(t1, world.userTypes(u.idn), m)
        case (t1: PrimitiveType, t2: PrimitiveType) if t1 == t2 =>
          Right(m)
        case (SetType(inner1), SetType(inner2)) =>
          Right(recurse(inner1, inner2, m) match { case Right(nm) => nm case Left(nm) => return Left(nm) })
        case (BagType(inner1), BagType(inner2)) =>
          Right(recurse(inner1, inner2, m) match { case Right(nm) => nm case Left(nm) => return Left(nm) })
        case (ListType(inner1), ListType(inner2)) =>
          Right(recurse(inner1, inner2, m) match { case Right(nm) => nm case Left(nm) => return Left(nm) })
        case (FunType(a1, a2), FunType(b1, b2)) =>
          recurse(a1, b1, m) match {
            case Right(nm) => recurse(a2, b2, nm) match {
              case Right(nm1) => Right(nm1)
              case Left(nm1) => Left(nm1)
            }
            case Left(nm) => Left(nm)
          }
        case (t1 @ RecordType(atts1, name1), t2 @ RecordType(atts2, name2)) if name1 == name2 && atts1.length == atts2.length && atts1.map(_.idn) == atts2.map(_.idn) =>
          var curm = m
          for ((att1, att2) <- atts1.zip(atts2)) {
            recurse(att1.tipe, att2.tipe, curm) match {
              case Right(nm) => curm = nm
              case Left(nm) => return Left(nm)
            }
          }
          Right(curm)
        case (t1 @ ConstraintRecordType(atts1, _), t2 @ ConstraintRecordType(atts2, _)) =>
          val commonIdns = atts1.map(_.idn).intersect(atts2.map(_.idn))
          var curm = m
          for (idn <- commonIdns) {
            val att1 = t1.getType(idn).head
            val att2 = t2.getType(idn).head
            recurse(att1, att2, curm) match {
              case Right(nm) => curm = nm
              case Left(nm) => return Left(nm)
            }
          }
          val commonAttrs = commonIdns.map { case idn => AttrType(idn, t1.getType(idn).head) } // Safe to take from the first attribute since they were already unified in the new map
          val nt = ConstraintRecordType(atts1.filter { case att => !commonIdns.contains(att.idn) } ++ atts2.filter { case att => !commonIdns.contains(att.idn) } ++ commonAttrs)
          Right(curm +(t1.sym -> t2, t2.sym -> nt))
        case (t1 @ ConstraintRecordType(atts1, _), t2 @ RecordType(atts2, name)) =>
          if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet)) {
            Left(m)
          } else {
            var curm = m
            for (att1 <- atts1) {
              recurse(att1.tipe, t2.getType(att1.idn).get, curm) match {
                case Right(nm) => curm = nm
                case Left(nm) => return Left(nm)
              }
            }
            Right(curm + (t1.sym -> t2))
          }
        case (t1: RecordType, t2: ConstraintRecordType) =>
          recurse(t2, t1, m)
        case (t1 @ ConstraintCollectionType(inner1, c1, i1, _), t2 @ ConstraintCollectionType(inner2, c2, i2, _)) =>
          if (c1.isDefined && c2.isDefined && (c1.get != c2.get)) {
            Left(m)
          } else if (i1.isDefined && i2.isDefined && (i1.get != i2.get)) {
            Left(m)
          } else {
            recurse(inner1, inner2, m) match {
              case Right(nm) =>
                val nc = if (c1.isDefined) c1 else c2
                val ni = if (i1.isDefined) i1 else i2
                val nt = ConstraintCollectionType(inner1, nc, ni)
                Right(nm +(t1.sym -> t2, t2.sym -> nt))
              case Left(nm) => Left(nm)
            }
          }
        case (t1 @ ConstraintCollectionType(inner1, c1, i1, _), t2: SetType) =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && i1.get) || i1.isEmpty))
            recurse(inner1, t2.innerType, m) match {
              case Right(nm) => Right(nm + (t1.sym -> t2))
              case Left(nm) => Left(nm)
            }
          else
            Left(m)
        case (t1 @ ConstraintCollectionType(inner1, c1, i1, _), t2: BagType) =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty))
            recurse(inner1, t2.innerType, m) match {
              case Right(nm) => Right(nm + (t1.sym -> t2))
              case Left(nm) => Left(nm)
            }
          else
            Left(m)
        case (t1 @ ConstraintCollectionType(inner1, c1, i1, _), t2: ListType) =>
          if (((c1.isDefined && !c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty))
            recurse(inner1, t2.innerType, m) match {
              case Right(nm) => Right(nm + (t1.sym -> t2))
              case Left(nm) => Left(nm)
            }
          else
            Left(m)
        case (t1: CollectionType, t2: ConstraintCollectionType) =>
          recurse(t2, t1, m)
        case (t1: TypeVariable, t2: TypeVariable) =>
          Right(m + (t2.sym -> t1))
        case (t1: TypeVariable, t2: VariableType) =>
          Right(m + (t1.sym -> t2))
        case (t1: VariableType, t2: TypeVariable) =>
          Right(m + (t2.sym -> t1))
        case (t1: TypeVariable, _) =>
          Right(m + (t1.sym -> t2))
        case (_, t2: TypeVariable) =>
          Right(m + (t2.sym -> t1))
        case _ =>
          Left(m)
      }
    }
    logger.debug(s"ENTER unify(${PrettyPrinter(t1)}, ${PrettyPrinter(t2)})")
    val r = recurse(t1, t2, Map())
    logger.debug(s"EXIT unify ${PrettyPrinter(t1)}, ${PrettyPrinter(t2)}\n=> $r")
    r
  }

  /** Return a set with all type variables within a type.
    */
  private def getTypeVariables(t: Type): Set[TypeVariable] = t match {
    case _: NothingType => Set()
    case _: AnyType => Set()
    case _: PrimitiveType => Set()
    case UserType(idn) => getTypeVariables(world.userTypes(idn))
    case RecordType(atts, _) => atts.flatMap { case att => getTypeVariables(att.tipe) }.toSet
    case ListType(innerType) => getTypeVariables(innerType)
    case SetType(innerType) => getTypeVariables(innerType)
    case BagType(innerType) => getTypeVariables(innerType)
    case FunType(t1, t2) => getTypeVariables(t1) ++ getTypeVariables(t2)
    case ConstraintRecordType(atts, _) => atts.flatMap { case att => getTypeVariables(att.tipe) }
    case ConstraintCollectionType(innerType, _, _, _) => getTypeVariables(innerType)
    case t1: TypeVariable => Set(t1)
  }

  /** Return a set with all type variables in a constraint.
    */
  private def getConstraintTypeVariables(c: Constraint): Set[TypeVariable] = c match {
    case Or(cs @ _*) =>
      cs.flatMap(c => getConstraintTypeVariables(c)).toSet
    case And(cs @ _*) =>
      cs.flatMap(c => getConstraintTypeVariables(c)).toSet
    case SameType(e1, e2, _) =>
      getTypeVariables(expType(e1)) ++ getTypeVariables(expType(e2))
    case HasType(e, expected, _) =>
      getTypeVariables(expType(e)) ++ getTypeVariables(expected)
    case NoConstraint => Set()
  }

  /** Type Checker constraint solver.
    * Takes a constraint and a given map of variable types (e.g. type variables, constraint types), and returns either:
    * - a new map, if a solution to the constraint was found;
    * - or a map of the solution found so far plus a list of error messages, if the constraint could not be resolved.
    */
  private def solve(c: Constraint, m: VarMap = Map()): Either[(VarMap, Seq[Error]), Seq[VarMap]] = {

    logger.debug(s"Processing constraint: $c")
    logger.debug("Input map:")
    logger.debug(m.map { case (v: String, t: Type) => s"$v => ${PrettyPrinter(t)}" }.mkString("{\n", ",\n", "}"))

    c match {
      case Or(cs @ _*) if cs.nonEmpty =>
        val head = cs.head
        val rest = cs.tail
        (solve(head, m), solve(Or(rest:_*), m)) match {
          case (Right(m1), Right(m2)) => Right(m1 ++ m2)
          case (Right(m1), _) => Right(m1)
          case (_, Right(m2)) => Right(m2)
          case (Left((m1, errs)), _) =>
            // In case of failure, we take the 1st map that failed
            Left((m1, errs))
        }
      case _: Or => // Empty Or
        Left((m, Nil))
      case And(cs @ _*) if cs.nonEmpty =>
        // ....
        // TODO: Handle 1st the constants that are really known? e.g 2 = "blah" : we should set type of = first?

        // TODO: Describe heuristic in DETAIL here

        // ....
        val g = cs.groupBy(c => getConstraintTypeVariables(c).map(_.sym).intersect(m.keySet).nonEmpty)
        val next =
          if (g.contains(true)) {
          g(true).head
        } else {
          g(false).head
        }
        solve(next, m) match {
          case Left(nm) => Left(nm)
          case Right(nms) =>
            val rest = And(cs.filterNot(c => c == next):_*)
            val all_ms = for (m1 <- nms) yield solve(rest, m1)
            val ms: Seq[VarMap] = all_ms.filter(_.isRight).flatMap(_.right.get)
            if (ms.isEmpty) all_ms.filter(_.isLeft).head else Right(ms)
        }
      case _: And => // Empty And
        Right(Seq(m))
      case SameType(e1, e2, desc) =>
        val nt1 = walk(expType(e1), m)
        val nt2 = walk(expType(e2), m)
        unify(nt1, nt2) match {
          case Right(nm) =>
            Right(Seq(m ++ nm))
          case Left(nm) =>
            nt1.pos = e1.pos
            nt2.pos = e2.pos
            Left(m ++ nm, List(IncompatibleTypes(nt1, nt2)))
        }
      case HasType(e, expected, desc) =>
        val nt = walk(expType(e), m)
        val nexpected = walk(expected, m)
        unify(nt, nexpected) match {
          case Right(nm) => Right(Seq(m ++ nm))
          case Left(nm) =>
            nt.pos = e.pos
            Left(m ++ nm, List(UnexpectedType(nt, nexpected, desc)))
        }
      case NoConstraint => Right(Seq(m))
    }
  }

  private lazy val applyConstraints =
    collect[List, Constraint] {
      case b: Bind => constraint(b)
      case g: Gen => constraint(g)
      case e: Exp => constraint(e)
    }

  /** The root constraint is an And of all constraints imposed by each node in the expression.
    */
  private lazy val rootConstraint =
    flattenAnds(
      Constraint.And(
        applyConstraints(tree.root)
          .filter {
            case NoConstraint => false
            case _ => true }:_*))

  /** Flatten all occurrences on nested And constraints.
    */
  private def flattenAnds(c: Constraint): Constraint = c match {
    case Or(cs @ _*) => Or(cs.map(flattenAnds):_*)
    case And(cs @ _*) => And(cs.map(flattenAnds).flatMap{ case c => c match {
      case And(cs1 @ _*) => cs1
      case _ => Seq(c)
    }}:_*)
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

  private lazy val solutions: Either[(VarMap, Seq[Error]), Seq[VarMap]] = solve(rootConstraint)

  lazy val errors: Seq[Error] =
    if (badEntities.nonEmpty)
      badEntities
    else {
      logger.debug("Root constraint:")
      logger.debug(rootConstraint.toString)

      solutions match {
        // Found too many alternative solutions, which all type check
        case Right(ms) if ms.length > 1 => Seq(TooManySolutions)

        // All is OK so check for semantic errors (i.e. run the error checking phase that requires final types)
        case Right(m) => semanticErrors

        // The type checking errors
        case Left((m, errs)) => errs
      }
    }

  /** Given a type, returns a new type that replaces type variables as much as possible, given the map m.
    */
  private def walk(t: Type, m: VarMap): Type =
    t match {
      case _: NothingType => t
      case _: AnyType => t
      case _: PrimitiveType => t
      case _: UserType => t
      case RecordType(atts, name) => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, walk(t1, m)) }, name)
      case ListType(innerType) => ListType(walk(innerType, m))
      case SetType(innerType) => SetType(walk(innerType, m))
      case BagType(innerType) => BagType(walk(innerType, m))
      case FunType(t1, t2) => FunType(walk(t1, m), walk(t2, m))
      case t1 @ ConstraintRecordType(atts, sym) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, walk(t1, m)) }, sym)
      case t1 @ ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(walk(innerType, m), c, i, sym)
      case t1: TypeVariable => if (m.contains(t1.sym) && m(t1.sym) != t) walk(m(t1.sym), m) else t
    }

  /** Return the type of an expression.
    */
  def tipe(e: Exp): Type = solutions match {
    case Right(m) if m.length > 1 => throw SemanticAnalyzerError("Too many solutions found")
    case Right(m) => walk(expType(e), m.head)
    case Left(m) => throw SemanticAnalyzerError("No solutions found")
  }

  /** Constraints of a node.
    * Each entry represents the constraints (aka. the facts) that a given node adds to the overall type checker.
    */
  lazy val constraint: RawNode => Constraint.Constraint = {
    import Constraint._
    attr {
      // Rule 4
      case n @ RecordProj(e, idn) =>
        HasType(e, ConstraintRecordType(Set(AttrType(idn, expType(n)))))

      // Rule 6
      case n @ IfThenElse(e1, e2, e3) =>
        And(
          HasType(e1, BoolType(), Some("if condition must be a boolean")),
          SameType(e2, e3, Some("then and else must be of the same type")),
          SameType(n, e2))

      // Rule 8
      case n @ FunApp(f, e) =>
        HasType(f, FunType(expType(e), expType(n)))

      // Rule 11
      case n @ MergeMonoid(_: BoolMonoid, e1, e2) =>
        And(
          HasType(n, BoolType()),
          HasType(e1, BoolType()),
          HasType(e2, BoolType()))

      case n @ MergeMonoid(_: NumberMonoid, e1, e2) =>
        And(
          Or(
            HasType(n, IntType()),
            HasType(n, FloatType())
          ),
          SameType(n, e1),
          SameType(e1, e2)
        )

      // Rule 12
      case n @ MergeMonoid(_: CollectionMonoid, e1, e2) =>
        And(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, ConstraintCollectionType(TypeVariable(), None, None))
        )

      // Rule 13
      case n @ Comp(m: PrimitiveMonoid, _, e) =>
        // TODO: Add constraint saying it is primitive
        SameType(n, e)

      // Binary Expression type
      case n @ BinaryExp(_: EqualityOperator, e1, e2) =>
        And(
          HasType(n, BoolType()),
          SameType(e1, e2))

      case n @ BinaryExp(_: ComparisonOperator, e1, e2) =>
        And(
          HasType(n, BoolType()),
          SameType(e2, e1),
          Or(
            HasType(e1, IntType()),
            HasType(e1, FloatType())))

      case n @ BinaryExp(_: ArithmeticOperator, e1, e2) =>
        And(
          SameType(n, e1),
          SameType(e1, e2),
          Or(
            HasType(e2, IntType()),
            HasType(e2, FloatType())))

      // Unary Expression type
      case n @ UnaryExp(_: Neg, e) =>
        And(
          SameType(n, e),
          Or(
            HasType(e, IntType()),
            HasType(e, FloatType())))

      // Expression block type
      case n @ ExpBlock(_, e) =>
        SameType(n, e)

      // Generator
      case n @ Gen(p, e) =>
        HasType(e, ConstraintCollectionType(patternType(p), None, None))

      // Bind
      case n @ Bind(p, e) =>
        HasType(e, patternType(p))

      case n =>
        NoConstraint
    }
  }

  }
