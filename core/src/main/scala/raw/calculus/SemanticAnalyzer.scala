package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution
import org.kiama.rewriting.Rewriter._

import scala.Predef

case class SemanticAnalyzerError(err: String) extends RawException(err)

/** Analyzes the semantics of an AST.
  * This includes the type checker/inference as well as monoid compatibility.
  *
  * The semantic analyzer reads the user types and the catalog of user-defined class entities from the World object.
  * User types are the type definitions available to the user.
  * Class entities are the data sources available to the user.
  * e.g., in expression `e <- Events` the class entity is `Events`.
  */
class SemanticAnalyzer(tree: Calculus.Calculus, world: World) extends Attribution with LazyLogging {

  import scala.collection.immutable.{Map, Seq}
  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.util.{Entity, MultipleEntity, UnknownEntity}
  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}
  import Calculus._
  import SymbolTable._
  import Constraint._

  /** Decorators on the tree.
    */
  private lazy val decorators = new Decorators(tree)

  import decorators.{chain, Chain}

  /** The semantic errors for the tree.
    */
  private lazy val collectTreeErrors = collect[List, Message] {

    // Identifier declared more than once in the same scope
    case d@IdnDef(i) if entity(d) == MultipleEntity() =>
      ErrorMessage(d, s"$i is declared more than once")

    // Identifier used without being declared
    case u@IdnUse(i) if entity(u) == UnknownEntity() =>
      ErrorMessage(u, s"$i is not declared")

    // Identifier declared has no inferred type
    case d@IdnDef(i) if entityType(entity(d)) == NothingType() =>
      ErrorMessage(d, s"$i has no type")

    // Identifier used has no inferred type
    case u@IdnUse(i) if entityType(entity(u)) == NothingType() =>
      ErrorMessage(u, s"$i has no type")

    // Semantic error in monoid composition
    case c@Comp(m, qs, _) => {
      def getMessage: Message = {
        for (q <- qs) {
          q match {
            case Gen(_, g) =>
              monoidsCompatible(m, g) match {
                case Some(m) => return m
                case None =>
              }
            case _ =>
          }
        }
        NoMessage
      }
      getMessage
    }
  }

  private lazy val treeErrors = collectTreeErrors(tree.root).collect { case e: ErrorMessage => e }

  /** Check whether monoid is compatible with the generator expression.
    */
  private def monoidsCompatible(m: Monoid, g: Exp): Option[Message] = {
    def errors(t: Type) = t match {
      case _: SetType =>
        if (!m.commutative && !m.idempotent)
          Some(ErrorMessage(m, "expected a commutative and idempotent monoid"))
        else if (!m.commutative)
          Some(ErrorMessage(m, "expected a commutative monoid"))
        else if (!m.idempotent)
          Some(ErrorMessage(m, "expected an idempotent monoid"))
        else
          None
      case _: BagType =>
        if (!m.commutative)
          Some(ErrorMessage(m, "expected a commutative monoid"))
        else
          None
      case _: ListType =>
        None
      case _: ConstraintCollectionType =>
        None
      case _: TypeVariable =>
        None
      case t =>
        Some(ErrorMessage(g, s"expected collection but got ${PrettyPrinter(t)}"))
    }

    // TODO: figure out if we should go the walk here or not. Here yes, for sure.
    tipe(g) match {
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
    case n@IdnDef(idn) =>
      if (isDefinedInScope(env.in(n), idn))
        MultipleEntity()
      else
        VariableEntity(n, TypeVariable(SymbolTable.next()))
    case n@IdnUse(idn) =>
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
    case n@IdnDef(i) => define(out(n), i, entity(n))

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

  /** Annotate the type with the (parser) position of the expression.
    */
  def tipeWithPos(t: Type, e: Exp): Type = {
    t.pos = e.pos
    t
  }

  /** The type of an expression.
    * If the type cannot be immediately derived from the expression itself, then TypeVariables are used.
    */
  lazy val expType: Exp => Type = attr {

    // Rule 1
    case c: BoolConst => tipeWithPos(BoolType(), c)
    case c: IntConst => tipeWithPos(IntType(), c)
    case c: FloatConst => tipeWithPos(FloatType(), c)
    case c: StringConst => tipeWithPos(StringType(), c)

    // Rule 2: is type variable

    // Rule 3
    case i@IdnExp(idn) => tipeWithPos(idnType(idn), i)

    // Rule 4: is type variable

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, expType(att.e))), None)

    // Rile 6: is type variable

    // Rule 7
    case FunAbs(p, e) => FunType(patternType(p), expType(e))

    // Rule 8: is type variable

    // Rule 9
    case z@ZeroCollectionMonoid(_: BagMonoid) => tipeWithPos(BagType(TypeVariable(SymbolTable.next())), z)
    case z@ZeroCollectionMonoid(_: ListMonoid) => tipeWithPos(ListType(TypeVariable(SymbolTable.next())), z)
    case z@ZeroCollectionMonoid(_: SetMonoid) => tipeWithPos(SetType(TypeVariable(SymbolTable.next())), z)

    // Rule 10
    case n@ConsCollectionMonoid(_: BagMonoid, e) => tipeWithPos(BagType(expType(e)), n)
    case n@ConsCollectionMonoid(_: ListMonoid, e) => tipeWithPos(ListType(expType(e)), n)
    case n@ConsCollectionMonoid(_: SetMonoid, e) => tipeWithPos(SetType(expType(e)), n)

    // Rule 11: is type variable

    // Rule 14
    case n@Comp(_: BagMonoid, _, e) => tipeWithPos(BagType(expType(e)), n)
    case n@Comp(_: ListMonoid, _, e) => tipeWithPos(ListType(expType(e)), n)
    case n@Comp(_: SetMonoid, _, e) => tipeWithPos(SetType(expType(e)), n)

    case UnaryExp(_: Not, _) => BoolType()
    case UnaryExp(_: ToBool, _) => BoolType()
    case UnaryExp(_: ToInt, _) => IntType()
    case UnaryExp(_: ToFloat, _) => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case n => tipeWithPos(TypeVariable(SymbolTable.next()), n)
  }

  type VarMap = Map[String, Type]

  def recordsDifferentStructure(r1: RecordType, r2: RecordType): Option[String] = (r1, r2) match {
    case (RecordType(atts1, name1), RecordType(atts2, name2)) => {
      if (name1 != name2)
        return Some("records with different names")
      if (atts1.length != atts2.length)
        return Some("records with different sizes")
      if (atts1.map(_.idn) != atts2.map(_.idn))
        return Some("records have different field names")
      None
    }
  }

  /** Hindley-Milner unification algorithm.
    */
  def unify(t1: Type, t2: Type): Either[VarMap, VarMap] = {

    def recurse(t1: Type, t2: Type, m: VarMap): Either[VarMap, VarMap] = {
      (t1, t2) match {
        case (_: AnyType, t) => Right(m)
        case (t, _: AnyType) => Right(m)
        case (u: UserType, _) => recurse(world.userTypes(u.idn), t2, m)
        case (_, u: UserType) => recurse(t1, world.userTypes(u.idn), m)
        case (t1: PrimitiveType, t2: PrimitiveType) if t1 == t2 => Right(m)
        case (SetType(t1), SetType(t2)) =>
          Right(recurse(t1, t2, m) match { case Right(m) => m case Left(m) => return Left(m) })
        case (BagType(t1), BagType(t2)) =>
          Right(recurse(t1, t2, m) match { case Right(m) => m case Left(m) => return Left(m) })
        case (ListType(t1), ListType(t2)) =>
          Right(recurse(t1, t2, m) match { case Right(m) => m case Left(m) => return Left(m) })
        case (FunType(a1, a2), FunType(b1, b2)) =>
          recurse(a1, b1, m) match {
            case Right(m) => recurse(a2, b2, m) match {
              case Right(m) => Right(m)
              case Left(err) => return Left(err)
            }
          }
        case (t1 @ RecordType(atts1, name1), t2 @ RecordType(atts2, name2)) =>
          recordsDifferentStructure(t1, t2) match {
            case Some(_) => Left(m)
            case None => {
              var curm = m
              for ((att1, att2) <- atts1.zip(atts2)) {
                recurse(att1.tipe, att2.tipe, curm) match {
                  case Right(m) => curm = m
                  case Left(m) => return Left(m)
                }
              }
              Right(curm)
            }
          }

        case (t1@ConstraintRecordType(idn1, atts1), t2@ConstraintRecordType(idn2, atts2)) => {
          val commonIdns = atts1.map(_.idn).intersect(atts2.map(_.idn))
          var curm = m
          for (idn <- commonIdns) {
            val att1 = t1.getType(idn).head
            val att2 = t2.getType(idn).head
            recurse(att1, att2, curm) match {
              case Right(m) => curm = m
              case Left(m) => return Left(m)
            }
          }
          val commonAttrs = commonIdns.map { case idn => AttrType(idn, t1.getType(idn).head) } // Safe to take from the first attribute since they were already unified in the new map
          val nt = ConstraintRecordType(SymbolTable.next(), atts1.filter { case att => !commonIdns.contains(att.idn) } ++ atts2.filter { case att => !commonIdns.contains(att.idn) } ++ commonAttrs)
          Right(curm +(t1.idn -> t2, t2.idn -> nt))
        }

        case (t1 @ ConstraintRecordType(idn1, atts1), t2 @ RecordType(atts2, name)) => {
          if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet)) {
            Left(m)
          } else {
            var curm = m
            for (att1 <- atts1) {
              recurse(att1.tipe, t2.getType(att1.idn).get, curm) match {
                case Right(m) => curm = m
                case Left(m) => return Left(m)
              }
            }
            Right(curm + (t1.idn -> t2))
          }
        }

        case (t1: RecordType, t2: ConstraintRecordType) =>
          recurse(t2, t1, m)

        case (t1 @ ConstraintCollectionType(idn1, inner1, c1, i1), t2 @ ConstraintCollectionType(idn2, inner2, c2, i2)) => {
          if (c1.isDefined && c2.isDefined && (c1.get != c2.get)) {
            Left(m)
          } else if (i1.isDefined && i2.isDefined && (i1.get != i2.get)) {
            Left(m)
          } else {
            recurse(inner1, inner2, m) match {
              case Right(m) =>
                val nc = if (c1.isDefined) c1 else c2
                val ni = if (i1.isDefined) i1 else i2
                val nt = ConstraintCollectionType(SymbolTable.next(), inner1, nc, ni)
                Right(m +(t1.idn -> t2, t2.idn -> nt))
              case Left(err) => Left(err)
            }
          }
        }

        case (t1@ConstraintCollectionType(idn1, inner1, c1, i1), t2: SetType) =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && i1.get) || i1.isEmpty))
            recurse(inner1, t2.innerType, m) match {
              case Right(m) => Right(m + (t1.idn -> t2))
              case Left(err) => Left(err)
            }
          else
            Left(m)
        case (t1@ConstraintCollectionType(idn1, inner1, c1, i1), t2: BagType) =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty))
            recurse(inner1, t2.innerType, m) match {
              case Right(m) => Right(m + (t1.idn -> t2))
              case Left(err) => Left(err)
            }
          else
            Left(m)
        case (t1@ConstraintCollectionType(idn1, inner1, c1, i1), t2: ListType) =>
          if (((c1.isDefined && !c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty))
            recurse(inner1, t2.innerType, m) match {
              case Right(m) => Right(m + (t1.idn -> t2))
              case Left(err) => Left(err)
            }
          else
            Left(m)
        case (t1: CollectionType, t2: ConstraintCollectionType) =>
          recurse(t2, t1, m)

        case (t1: TypeVariable, t2: TypeVariable) => Right(m + (t2.idn -> t1))
        case (t1: TypeVariable, t2: VariableType) => Right(m + (t1.idn -> t2))
        case (t1: VariableType, t2: TypeVariable) => Right(m + (t2.idn -> t1))
        case (t1: TypeVariable, t2) =>
          Right(m + (t1.idn -> t2))
        case (t1, t2: TypeVariable) =>
          Right(m + (t2.idn -> t1))

        case _ =>
          Left(m)
      }
    }
    logger.debug(s"ENTER unify(${PrettyPrinter(t1)}, ${PrettyPrinter(t2)})")
    val r = recurse(t1, t2, Map())
    logger.debug(s"EXIT unify ${PrettyPrinter(t1)}, ${PrettyPrinter(t2)}\n=> $r")
    r
  }

  def getTypeVariables(t: Type, m: VarMap): Set[TypeVariable] =
  t match {
    case _: AnyType => Set()
    case _: PrimitiveType => Set()
    case UserType(idn) => getTypeVariables(world.userTypes(idn), m)
    case RecordType(atts, _) =>
      atts.flatMap { case att => getTypeVariables(att.tipe, m) }.toSet
    case ListType(innerType) => getTypeVariables(innerType, m)
    case SetType(innerType) => getTypeVariables(innerType, m)
    case BagType(innerType) => getTypeVariables(innerType, m)
    case FunType(t1, t2) => getTypeVariables(t1, m) ++ getTypeVariables(t2, m)
    case ConstraintRecordType(_, atts) =>
      atts.flatMap { case att => getTypeVariables(att.tipe, m) }.toSet
    case ConstraintCollectionType(_, innerType, _, _) =>
      getTypeVariables(innerType, m)
    case t: TypeVariable => Set(t)
  }

  def getConstraintTypeVariables(c: Constraint, m: VarMap): Set[TypeVariable] = c match {
    case Or(cs @ _*) =>
      cs.flatMap(c => getConstraintTypeVariables(c, m)).toSet
    case And(cs @ _*) =>
      cs.flatMap(c => getConstraintTypeVariables(c, m)).toSet
    case SameType(e1, e2, _) =>
      getTypeVariables(expType(e1), m) ++ getTypeVariables(expType(e2), m)
    case HasType(e, texpected, _) =>
      getTypeVariables(expType(e), m) ++ getTypeVariables(texpected, m)
    case NoConstraint => Set()
  }

    def solve(c: Constraint, m: VarMap = Map()): Either[(VarMap, Seq[Message]), Seq[VarMap]] = {

      logger.debug(s"Processing constraint\n$c\nMap is\n")
      logger.debug("VarMap\n" + m.map { case (v: String, t: Type) => s"$v => ${PrettyPrinter(t)}" }.mkString("{\n", ",\n", "}"))

      c match {
        case Or(cs @ _*) if cs.nonEmpty =>
          val head = cs.head
          val rest = cs.tail
          (solve(head, m), solve(Or(rest:_*), m)) match {
            case (Right(m1), Right(m2)) => Right(m1 ++ m2)
            case (Right(m1), _) => Right(m1)
            case (_, Right(m2)) => Right(m2)
            case (Left((m, errs)), _) =>
              // In case of failure, we take the 1st map that failed
              Left((m, errs))
          }
        case _: Or => // Empty Or
          Left((m, Nil))
        case And(cs @ _*) if cs.nonEmpty =>
          // ....
          // TODO: Handle 1st the constants that are really known? e.g 2 = "blah" : we should set type of = first?

          // ....
          val g = cs.groupBy(c => getConstraintTypeVariables(c, m).map(_.idn).intersect(m.keySet).nonEmpty)
          val next =
            if (g.contains(true)) {
            g(true).head
          } else {
            g(false).head
          }
          solve(next, m) match {
            case Left(m) => Left(m)
            case Right(ms1) =>
              val rest = And(cs.filterNot(c => c == next):_*)
              val all_ms = for (m1 <- ms1) yield solve(rest, m1)
              val ms: Seq[VarMap] = all_ms.filter(_.isRight).flatMap(_.right.get)
              if (ms.isEmpty) all_ms.filter(_.isLeft).head else Right(ms)
          }
        case _: And => // Empty And
          Right(Seq(m))
        case SameType(e1, e2, desc) => {
          val nt1 = walk(expType(e1), m)
          val nt2 = walk(expType(e2), m)
          unify(nt1, nt2) match {
            case Right(nm) =>
              Right(Seq(m ++ nm))
            case Left(nm) =>
              val em = m ++ nm
              val err = desc match {
                case Some(desc) => ???
                  // TODO: Add IncompatibleTypes message taking 2 positions
                case None => ErrorMessage(e1, s"incompatible types: ${expectedTypePrettyPrinter(nt1)} and ${expectedTypePrettyPrinter(nt2)}")
              }
              Left(em, List(err))
          }
        }
        case HasType(e, texpected, desc) =>
          val nt = walk(expType(e), m)
          val ntexpected = walk(texpected, m)
          unify(nt, ntexpected) match {
            case Right(nm) => Right(Seq(m ++ nm))
            case Left(nm) =>
              val em = m ++ nm
              val err = desc match {
                case Some(desc) => ErrorMessage(e, s"$desc but got ${expectedTypePrettyPrinter(nt)}")
                case None => ErrorMessage(e, s"expected ${expectedTypePrettyPrinter(ntexpected)} but got ${expectedTypePrettyPrinter(nt)}")
              }
              Left(em, List(err))
          }
        case NoConstraint => Right(Seq(m))
      }
    }

  private def expectedTypePrettyPrinter(t: Type): String = {
    def p(v: Option[Boolean], s: String) =
      v match {
        case Some(true) => Some(s)
        case Some(false) => Some(s"non-$s")
        case _ => None
      }

    t match {
      case ConstraintCollectionType(_, inner, c, i) =>
        val prefix = List(p(c, "commutative"), p(i, "idempotent")).filter(_.isDefined).mkString(" and ")
        if (prefix.nonEmpty)
          prefix + " collection of " + expectedTypePrettyPrinter(inner)
        else
          "collection of " + expectedTypePrettyPrinter(inner)
      case ConstraintRecordType(idn, atts) =>
        val satts = atts.map { case att => s"attribute ${att.idn} of type ${expectedTypePrettyPrinter(att.tipe)}" }.mkString(" and ")
        if (satts.nonEmpty)
          s"record with $satts"
        else
          s"record"
      case FunType(t1, t2) =>
        s"function taking ${expectedTypePrettyPrinter(t1)} and returning ${expectedTypePrettyPrinter(t2)}"
      case _: TypeVariable => "unknown" // TODO: or should be any?
      case t => PrettyPrinter(t)
    }
  }


    private lazy val applyConstraints =
      collect[List, Constraint] {
        case b: Bind => constraint(b)
        case g: Gen => constraint(g)
        case e: Exp => constraint(e)
      }

  private lazy val rootConstraint =
    flattenAnds(
      Constraint.And(
        applyConstraints(tree.root)
          .filter {
            case NoConstraint => false
            case _ => true }:_*))

  def flattenAnds(c: Constraint): Constraint = c match {
    case Or(cs @ _*) => Or(cs.map(flattenAnds):_*)
    case And(cs @ _*) => And(cs.map(flattenAnds).flatMap{ case c => c match {
      case And(cs1 @ _*) => cs1
      case c => Seq(c)
    }}:_*)
    case c => c
  }

  logger.debug("Constraints are " + rootConstraint)

    private lazy val solutions: Either[(VarMap, Seq[Message]), Seq[VarMap]] = solve(rootConstraint)

    lazy val errors: Seq[Message] = solutions match {
      case Right(ms) if ms.length > 1 =>
        logger.debug("TOO MANY SOLUTIONS");
        for (m <- ms)
          logger.debug("VarMap\n" + m.map{case (v: String, t: Type) => s"$v => ${PrettyPrinter(t)}"}.mkString("{\n",",\n", "}"))

        List(ErrorMessage(tree.root, "too many solutions"))
      case Right(m) => treeErrors
      case Left((m, errs)) => errs
    }

    def walk(t: Type, m: VarMap): Type =
      t match {
        case _: AnyType => t
        case _: PrimitiveType => t
        case _: UserType => t
        case RecordType(atts, name) => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, walk(t1, m)) }, name)
        case ListType(innerType) => ListType(walk(innerType, m))
        case SetType(innerType) => SetType(walk(innerType, m))
        case BagType(innerType) => BagType(walk(innerType, m))
        case FunType(t1, t2) => FunType(walk(t1, m), walk(t2, m))
        case ConstraintRecordType(idn, atts) => ConstraintRecordType(idn, atts.map { case AttrType(idn1, t1) => AttrType(idn1, walk(t1, m)) })
        case ConstraintCollectionType(idn, innerType, c, i) => ConstraintCollectionType(idn, walk(innerType, m), c, i)
        case TypeVariable(idn) => if (m.contains(idn) && m(idn) != t) walk(m(idn), m) else t
      }



    def tipe(e: Exp): Type =
        solutions match {
          case Right(m) if m.length > 1 => throw SemanticAnalyzerError("Several solutions found")
          case Right(m) => walk(expType(e), m.head)
          case Left(m) => throw SemanticAnalyzerError("No solutions found")
        }

    lazy val constraint: RawNode => Constraint.Constraint = {
      import Constraint._
      attr {
        // Rule 4
        case n @ RecordProj(e, idn) =>
          //val c = HasAttr(expType(e), AttrType(idn, expType(n)))
          //val c = expType(e) === ConstraintRecordType(SymbolTable.next(), Set(AttrType(idn, expType(n))))
          HasType(e, ConstraintRecordType(SymbolTable.next(), Set(AttrType(idn, expType(n)))))

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

        case n@MergeMonoid(_: PrimitiveMonoid, e1, e2) =>
          And(
            // TODO: Refactor to add a constraint saying it is primitive
            Or(
              HasType(n, BoolType()),
              HasType(n, IntType()),
              HasType(n, FloatType())
            ),
            SameType(n, e1),
            SameType(e1, e2)
          )
//          logger.debug(s"\n\n\nN pos is ${n.pos}\n\n\n")
//          logger.debug(s"\n\n\nexpType(n) pos is ${expType(n).pos}\n\n\n")
//          logger.debug(s"\n\n\nexpType(e1) pos is ${expType(e1).pos}\n\n\n")
//          logger.debug(s"\n\n\nexpType(e2) pos is ${expType(e2).pos}\n\n\n")

        // Rule 12
        case n@MergeMonoid(_: CollectionMonoid, e1, e2) =>
          And(
            SameType(n, e1),
            SameType(e1, e2),
          // TODO: This should rather be AreSameCollections(e1, e2) that checks if they are both collections and whether they have the same inner type - w/o caring what that is
          // TODO: and the same collection properties (idempotent, commutative).
          // TODO: That also replaces expType(e1) === expType(e2)
          // TODO: Or, we just say IsCollection(...) and that only checks that exptType(e2) is a collection & nothing else - no checks on inner type or collection properties.
          //  IsCollection(expType(e2), TypeVariable(SymbolTable.next()))
            HasType(e2, ConstraintCollectionType(SymbolTable.next(), TypeVariable(SymbolTable.next()), None, None))
          )

        // Rule 13
        case n@Comp(m: PrimitiveMonoid, _, e) =>
          SameType(n, e)
          // TODO: Add constraint saying it is primitive :)

        // Binary Expression type
        case n @ BinaryExp(_: EqualityOperator, e1, e2) =>
          And(
            HasType(n, BoolType()),
            SameType(e1, e2))

        case n@BinaryExp(_: ComparisonOperator, e1, e2) =>
          And(
            HasType(n, BoolType()),
            SameType(e2, e1),
            Or(
              HasType(e1, IntType()),
              HasType(e1, FloatType())))

        case n@BinaryExp(_: ArithmeticOperator, e1, e2) =>
          And(
            SameType(n, e1),
            SameType(e1, e2),
            Or(
              HasType(e2, IntType()),    // TODO: SameType(Not, constraint) but a domain of a expression
              HasType(e2, FloatType())))

        // Unary Expression type
        case n@UnaryExp(_: Neg, e) =>
          And(
            SameType(n, e),
            Or(
              HasType(e, IntType()),
              HasType(e, FloatType())))

        // Expression block type
        case n@ExpBlock(_, e) =>
          SameType(n, e)

        // Generator
        case n@Gen(p, e) =>
//          logger.debug(s"Gen ${CalculusPrettyPrinter(p)} ${CalculusPrettyPrinter(e)}")
          // TODO: Maybe replace by a new constraint IsInner instead, saying that p is inner of e?
          //val c = IsCollection(expType(e), patternType(p))
          //val c = ConstraintCollectionType(SymbolTable.next(), patternType(p), None, None) === expType(e)
          HasType(e, ConstraintCollectionType(SymbolTable.next(), patternType(p), None, None))

        // Bind
        case n @ Bind(p, e) =>
          // sounds too weak
//          val c = patternType(p) === expType(e)
          // TODO: Check with Ben
          HasType(e, patternType(p))

        case n =>
//          logger.debug(s"NOT HANDLED IS $n")
          NoConstraint
      }
    }

  }
