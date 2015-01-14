package raw.calculus

trait SemanticAnalyzer {
  
  import Calculus._
  import org.kiama.==>
  import org.kiama.attribution.Attribution._
  import org.kiama.attribution.Decorators.{ chain, Chain }
  import org.kiama.rewriting.Rewriter.collectall
  import org.kiama.util.{ Entity, MultipleEntity, UnknownEntity }
  import org.kiama.util.Messaging.{ check, checkuse, message, noMessages }
  import org.kiama.util.Patterns.HasParent
  import org.kiama.util.TreeNode
  import SymbolTable._

  /** Map of user-defined types.
   */
  val userTypes: Map[String, Type]
  
  /** Catalog of user-defined class entities.
   *  Class entities are the only entry points in the system.
   *  A class entity is the right-hand side of a Generator,
   *  e.g. the `Events` in expression `e <- Events`.
   */
  val catalog: Set[ClassEntity]
  
  /** Collect semantic error messages for a given tree.
   */
  // TODO: Improve error messages.
  val errors = attr(collectall {

    // Variable declared more than once in the same comprehension
    case d @ IdnDef(i) if d -> entity == MultipleEntity() =>
      message(d, s"$i is declared more than once")

    // Identifier used without being declared
    case u @ IdnUse(i) if u -> entity == UnknownEntity() =>
      message(u, s"$i is not declared")

    case e: Exp =>
      // Mismatch between type expected and actual type
      message(e, s"type error: expected ${e -> expectedType} got ${e -> tipe}",
        !((e -> expectedType).exists(isCompatible(_, e -> tipe)))) ++
        check(e) {

          // Record type required
          case r @ RecordProj(e, name) => check(e -> tipe) {
            case _: RecordType => noMessages
            case _             => message(e, s"type error: expected record but got ${e->tipe}")
          }

          // Badly formed `IfThenElse`
          // TODO: Either one of the other error will be fired, but they could both be fired at the same time
          case i @ IfThenElse(e1, _, _) if e1 -> tipe != BoolType() =>
            message(i, s"type error: expected a predicate but got ${e1->tipe}")
          case i @ IfThenElse(_, e2, e3) if e2 -> tipe != e3 -> tipe =>
            message(i, s"type error: 'then' type is ${e2->tipe} and 'else' type is ${e3->tipe}")

          //  Mismatch in function application            
          case FunApp(f, e) => f -> tipe match {
            case FunType(t1, _) if (t1 == e -> tipe) => noMessages
            case FunType(t1, _)                      => message(f, s"type error: function expects ${t1} but got ${e->tipe}")
            case t                                   => message(f, s"type error: function required but got ${t}")
          }

          // ...
          case e @ MergeMonoid(m: PrimitiveMonoid, e1, e2) => check(e1 -> tipe, e2 -> tipe) {
            case (t1: PrimitiveType, t2) if t1 == t2 && m.isOfType(t1) => noMessages
            case (t1, t2)                                              => message(e, s"type error: cannot merge ${t1} with ${t2}")
          }

          // ...
          case e @ MergeMonoid(m: CollectionMonoid, e1, e2) => check(e1 -> tipe, e2 -> tipe) {
            case (t1 @ CollectionType(m1, _), t2) if m == m1 && t1 == t2 => noMessages
            case (t1, t2)                                                => message(e, s"type error: cannot merge ${t1} with ${t2}")
          }

          case c @ Comp(m, qs, e) =>
            qs.flatMap(q => q match {
              case Gen(v, g) => {
                g -> tipe match {
                  case CollectionType(m1, _) =>
                    if (m1.commutative && m1.idempotent) {
                      if (!m.commutative && !m.idempotent)
                        message(m, "expected a commutative and idempotent monoid")
                      else if (!m.commutative)
                        message(m, "expected a commutative monoid")
                      else if (!m.idempotent)
                        message(m, "expected an idempotent monoid")
                      else
                        noMessages
                    } else if (m1.commutative) {
                      if (!m.commutative)
                        message(m, "expected a commutative monoid")
                      else
                        noMessages
                    } else
                      noMessages
                  case t => message(t, s"expected collection type but got ${t}")
                }
              }
              case e: Exp => e -> tipe match {
                case _: BoolType => noMessages
                case _           => message(e, s"expected predicate but got ${e->tipe}")
              }
              case _: Bind => noMessages
            }).toIndexedSeq

          case e @ BinaryExp(op: ComparisonOperator, e1, e2) => check(e1 -> tipe, e2 -> tipe) {
            case (t1: NumberType, t2: NumberType) if t1 == t2 => noMessages
            case (t1, t2)                                     => message(e, s"type error: cannot use operator '${op}' with ${t1} and ${t2}")
          }

          case e @ BinaryExp(op: EqualityOperator, e1, e2) => check(e1 -> tipe, e2 -> tipe) {
            case (t1, t2) if t1 == t2 => noMessages
            case (t1, t2)             => message(e, s"type error: cannot use operator '${op}' with ${t1} and ${t2}")
          }

          case e @ BinaryExp(op: ArithmeticOperator, e1, e2) => check(e1 -> tipe, e2 -> tipe) {
            case (t1: NumberType, t2: NumberType) if t1 == t2 => noMessages
            case (t1, t2)                                     => message(e, s"type error: cannot use operator '${op}' with ${t1} and ${t2}")
          }

          case e @ UnaryExp(op: Neg, e1) => check(e1 -> tipe) {
            case _: NumberType => noMessages
            case t             => message(e, s"type error: cannot use operator '${op}' with ${t}")
          }
        }
  })

  def isCompatible(t1: Type, t2: Type): Boolean =
    (t1 == UnknownType()) || (t2 == UnknownType()) || (t1 == t2)

  def entityFromDecl(n: IdnDef, i: String): Entity =
    n.parent match {
      case Bind(_, e)      => BindVar(e)
      case Gen(_, e)       => GenVar(e)
      case FunAbs(_, t, _) => FunArg(t)
    }

  lazy val entity: IdnNode => Entity =
    attr {
      case n @ IdnDef(i) =>
        if (isDefinedInScope(n -> (env.in), i))
          MultipleEntity()
        else
          entityFromDecl(n, i)
      case n @ IdnUse(i) =>
        lookup(n -> (env.in), i, UnknownEntity())
    }

  lazy val env: Chain[TreeNode, Environment] =
    chain(envin, envout)

  def envin(in: TreeNode => Environment): TreeNode ==> Environment = {
    case c: Comp if  c.isRoot       => rootenv(catalog.map{ case entity @ ClassEntity(name, _) => (name, entity) }.toSeq: _*)
    case c: Comp                    => enter(in(c))

    // If we are in an expression and the parent is a `Bind` or `Gen`, then the `in` environment
    // of the expression is the same as that of the `Bind`/`Gen` (i.e. it does not include the
    // lhs of the assignment).
    case HasParent(_: Exp, b: Bind) => b -> (env.in)
    case HasParent(_: Exp, g: Gen)  => g -> (env.in)

    case f: FunAbs => enter(in(f))
  }

  def envout(out: TreeNode => Environment): TreeNode ==> Environment = {
    case c: Comp       => leave(out(c))
    case f: FunAbs     => leave(out(f))

    case n @ IdnDef(i) => define(n -> out, i, n -> entity)

    // The `out` environment of a `Bind`/`Gen` is the environment after the assignment.
    case Bind(idn, _)  => idn -> env
    case Gen(idn, _)   =>  idn -> env

    // `Exp` cannot define new variables, so the `out` environment is the same as the `in`
    // environment (i.e. no need to go "inside" the expression to finding bindings).
    case e: Exp        => e -> (env.in)
  }

  /** The expected type of an expression.
   *  Returns `UnknownType` if any type will do.
   */
  lazy val expectedType: Exp => Set[Type] = attr {
    case e =>
      (e.parent) match {
        case UnaryExp(_: Not, _)      => Set(BoolType())
        case UnaryExp(_: ToBool, _)   => Set(FloatType(), IntType())
        case UnaryExp(_: ToInt, _)    => Set(BoolType(), FloatType())
        case UnaryExp(_: ToFloat, _)  => Set(IntType())
        case UnaryExp(_: ToString, _) => Set(BoolType(), FloatType(), IntType())
        case _                        => Set(UnknownType())
      }
  }

  /** The dereferenced type of an expression.
   *
   *  Given a type defined as:
   *    RecordType(List(
   *      AttrType("foo", IntType()),
   *      AttrType("bar", ClassType("baz"))))
   *
   *  The `realTipe` attribute for bar is the user-defined type ClassType("baz").
   *  The code, however, wants to compare with the type that "baz" points to.
   *  This attribute transparently deferences user-defined ClassTypes to their
   *  actual type,  by looking up the type definition in the `userTypes` catalog.
   */
  lazy val tipe: Exp => Type = attr {
    case e => e -> realType match {
      case ClassType(name) => userTypes(name)
      case t               => t
    }
  }

  /** The type of an expression.
   *  Returns `UnknownType` if any type will do, including when there is a type error.
   */
  lazy val realType: Exp => Type = attr {

    // Rule 1
    case _: BoolConst   => BoolType()
    case _: IntConst    => IntType()
    case _: FloatConst  => FloatType()
    case _: StringConst => StringType()

    // Rule 2
    case _: Null       => UnknownType()

    // Rule 3
    case IdnExp(idn)   => entityTipe(idn -> entity)

    // Rule 4
    case RecordProj(e, idn) => e -> tipe match {
      case t: RecordType => t.atts.find(_.idn == idn) match {
        case Some(att: AttrType) => att.tipe
        case _                   => UnknownType()
      }
      case t => UnknownType()
    }

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, att.e -> tipe)))

    // Rule 6
    case IfThenElse(e1, e2, e3) => (e1 -> tipe, e2 -> tipe, e3 -> tipe) match {
      case (_: BoolType, t2, t3) if t2 == t3 => t2
      case _                                 => UnknownType()
    }

    // Rule 7 
    // TODO: Missing preconditions?
    case FunAbs(_, t, e) => FunType(t, e -> tipe)

    // Rule 8
    case FunApp(f, e) => f -> tipe match {
      case FunType(t1, t2) if t1 == e -> tipe => t2
      case _                                  => UnknownType()
    }

    // Rule 9
    case ZeroCollectionMonoid(m)    => CollectionType(m, UnknownType())

    // Rule 10
    case ConsCollectionMonoid(m, e) => CollectionType(m, e -> tipe)

    // Rule 11
    case MergeMonoid(m: PrimitiveMonoid, e1, e2) => (e1 -> tipe, e2 -> tipe) match {
      case (t1: PrimitiveType, t2) if t1 == t2 && m.isOfType(t1) => t1
      case _ => UnknownType()
    }

    // Rule 12
    case MergeMonoid(m: CollectionMonoid, e1, e2) => (e1 -> tipe, e2 -> tipe) match {
      case (t1 @ CollectionType(m1, _), t2) if m == m1 && t1 == t2 => t1
      case _ => UnknownType()
    }

    // Rule 13
    case Comp(m: PrimitiveMonoid, Nil, e) if m.isOfType(e -> tipe) => e -> tipe

    // Rule 14
    case Comp(m: CollectionMonoid, Nil, e)                         => CollectionType(m, e -> tipe)

    // Rule 15
    case Comp(m, Gen(idn, e2) :: r, e1) => e2 -> tipe match {
      case CollectionType(m2, t2) if m.greaterOrEqThan(m2) => Comp(m, r, e1) -> tipe
      case _ => UnknownType()
    }

    // Rule 16
    case Comp(m, (e2: Exp) :: r, e1) => e2 -> tipe match {
      case _: BoolType => Comp(m, r, e1) -> tipe
      case _           => UnknownType()
    }

    // Skip Bind
    case Comp(m, (_: Bind) :: r, e1) => Comp(m, r, e1) -> tipe

    // Binary Expression type
    case BinaryExp(_: ComparisonOperator, e1, e2) => (e1 -> tipe, e2 -> tipe) match {
      case (t1: NumberType, t2: NumberType) if t1 == t2 => BoolType()
      case _                                            => UnknownType()
    }
    case BinaryExp(_: EqualityOperator, e1, e2) => (e1 -> tipe, e2 -> tipe) match {
      case (t1, t2) if t1 == t2 => BoolType()
      case _                    => UnknownType()
    }
    case BinaryExp(_: ArithmeticOperator, e1, e2) => (e1 -> tipe, e2 -> tipe) match {
      case (t1: NumberType, t2: NumberType) if t1 == t2 => t1
      case _                                            => UnknownType()
    }

    // Unary Expression type
    case UnaryExp(_: Not, _) => BoolType()
    case UnaryExp(_: Neg, e) => e -> tipe match {
      case t: NumberType => t
      case _             => UnknownType()
    }
    case UnaryExp(_: ToBool, _)   => BoolType()
    case UnaryExp(_: ToInt, _)    => IntType()
    case UnaryExp(_: ToFloat, _)  => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case _                        => UnknownType()
  }

  val entityTipe: Entity => Type = attr {
    case BindVar(e)        => e -> tipe
    case GenVar(e)         => e -> tipe match {
      case t: CollectionType => t.innerType
      case _                 => UnknownType()
    }
    case FunArg(t)         => t
    case ClassEntity(_, t) => t
    case _                 => UnknownType()
  }

}