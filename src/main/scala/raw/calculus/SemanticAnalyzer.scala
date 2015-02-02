package raw
package calculus

// TODO:
//semantic analyzer rules are wrong.
//ALPHA is a TYPE VARIABLE, not an unknown type.
//that is NOT the same thing
//a TYPE VARIABLE always compares and SETS THE TYPE
//    introduce type variable!!

import org.kiama.attribution.Attribution

/** Analyzes the semantics of an AST.
  * This includes the type checker and errors in monoid composition.
  *
  * The semantic analyzer reads the user types and the catalog of user-defined class entities from the World object.
  * User types are the type definitions available to the user.
  * Class entities are the data sources available to the user.
  *   e.g., in expression `e <- Events` the class entity is `Events`.
  */
class SemanticAnalyzer(tree: Calculus.Calculus, world: World) extends Attribution {

  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.util.{Entity, MultipleEntity, UnknownEntity}
  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}
  import Calculus._
  import SymbolTable._

  /** Decorators on the tree.
   */
  lazy val decorators = new Decorators(tree)
  import decorators.{chain, Chain}

  /** The semantic errors for the tree.
    */
  lazy val errors: Messages =
    collectmessages(tree) {
      case n => check(n) {
        // Variable declared more than once in the same comprehension
        case d@IdnDef(i) if entity(d) == MultipleEntity() =>
          message(d, s"$i is declared more than once")

        // Identifier used without being declared
        case u@IdnUse(i) if entity(u) == UnknownEntity() =>
          message(u, s"$i is not declared")

        case e: Exp =>
          // Mismatch between type expected and actual type
          message(e, s"type error: expected ${expectedType(e)} got ${tipe(e)}",
            !expectedType(e).exists(isCompatible(_, tipe(e)))) ++
            check(e) {

              // Record type required
              case r@RecordProj(e, idn) => check(tipe(e)) {
                case _: RecordType => noMessages
                case _             => message(e, s"type error: expected record but got ${tipe(e)}")
              }

              // Badly formed `IfThenElse`
              // TODO: Either one of the other error will be fired, but they could both be fired at the same time
              case i@IfThenElse(e1, _, _) if tipe(e1) != BoolType() =>
                message(i, s"type error: expected a predicate but got ${tipe(e1)}")
              case i@IfThenElse(_, e2, e3) if tipe(e2) != tipe(e3)  =>
                message(i, s"type error: 'then' type is ${tipe(e2)} and 'else' type is ${tipe(e3)}")

              //  Mismatch in function application
              case FunApp(f, e) => tipe(f) match {
                case FunType(t1, _) if t1 == tipe(e) => noMessages
                case FunType(t1, _)                  => message(f, s"type error: function expects ${t1} but got ${tipe(e)}")
                case t                               => message(f, s"type error: function required but got ${t}")
              }

              // ...
              case e@MergeMonoid(m: PrimitiveMonoid, e1, e2) => check(tipe(e1), tipe(e2)) {
                case (t1: PrimitiveType, t2) if t1 == t2 && m.isOfType(t1) => noMessages
                case (t1, t2)                                              => message(e, s"type error: cannot merge ${t1} with ${t2}")
              }

              // ...
              case e@MergeMonoid(m: CollectionMonoid, e1, e2) => check(tipe(e1), tipe(e2)) {
                case (t1@CollectionType(m1, _), t2) if m == m1 && isCompatible(t1, t2) => noMessages
                case (t1, t2)                                                          => message(e, s"type error: cannot merge ${t1} with ${t2}")
              }

              case c@Comp(m, qs, e) =>
                qs.flatMap(q => q match {
                  case Gen(v, g) => {
                    tipe(g) match {
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
                      case t                     => message(t, s"expected collection type but got ${t}")
                    }
                  }
                  case e: Exp    => tipe(e) match {
                    case _: BoolType => noMessages
                    case _           => message(e, s"expected predicate but got ${tipe(e)}")
                  }
                  case _: Bind   => noMessages
                }).toIndexedSeq

              case e@BinaryExp(op: ComparisonOperator, e1, e2) => check(tipe(e1), tipe(e2)) {
                case (t1: NumberType, t2: NumberType) if t1 == t2 => noMessages
                case (t1, t2)                                     => message(e, s"type error: cannot use operator '${op}' with ${t1} and ${t2}")
              }

              case e@BinaryExp(op: EqualityOperator, e1, e2) => check(tipe(e1), tipe(e2)) {
                case (t1, t2) if t1 == t2 => noMessages
                case (t1, t2)             => message(e, s"type error: cannot use operator '${op}' with ${t1} and ${t2}")
              }

              case e@BinaryExp(op: ArithmeticOperator, e1, e2) => check(tipe(e1), tipe(e2)) {
                case (t1: NumberType, t2: NumberType) if t1 == t2 => noMessages
                case (t1, t2)                                     => message(e, s"type error: cannot use operator '${op}' with ${t1} and ${t2}")
              }

              case e@UnaryExp(op: Neg, e1) => check(tipe(e1)) {
                case _: NumberType => noMessages
                case t             => message(e, s"type error: cannot use operator '${op}' with ${t}")
              }
            }
      }
    }

  def isCompatible(t1: Type, t2: Type): Boolean =
    (t1 == UnknownType()) || (t2 == UnknownType()) || (t1 == t2)

  /** Looks up identifier in the World catalog. If it is in catalog returns a new `ClassEntity` instance, otherwise
    * returns `UnknownType`. Note that when looking up a given identifier, a new `ClassEntity` instance is generated
    * each time, to ensure that reference equality comparisons work later on.
    */
  def lookupCatalog(idn: String): Entity =
    if (world.catalog.contains(idn))
      ClassEntity(idn, world.catalog(idn).tipe)
    else
      UnknownEntity()

  def entityFromDecl(n: IdnDef, i: String): Entity =
    n match {
      case tree.parent(p) => {
        p match {
          case Bind(_, e)      => BindVar(e)
          case Gen(_, e)       => GenVar(e)
          case FunAbs(_, t, _) => FunArg(t)
        }
      }
    }

  lazy val entity: IdnNode => Entity = attr {
    case n @ IdnDef(idn) =>
      if (isDefinedInScope(env.in(n), idn))
        MultipleEntity()
      else
        entityFromDecl(n, idn)
    case n @ IdnUse(idn) =>
      lookup(env.in(n), idn, lookupCatalog(idn))
  }

  lazy val env: Chain[Environment] =
    chain(envin, envout)

  def envin(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()
    case c: Comp                   => enter(in(c))

    // If we are in a function abstraction, we must open a new scope for the variable argument. But if the parent is a
    // `Bind`, then the `in` environment of the function abstractionmust be the same as the `in` environment of the
    // `Bind`.
    case tree.parent.pair(_: FunAbs, b: Bind) => enter(env.in(b))
    case f: FunAbs                            => enter(in(f))

    // If we are in an expression and the parent is a `Bind` or a `Gen`, then the `in` environment of the expression is
    // the same as that of the parent `Bind` or `Gen`. That is, it does not include the lhs of the assignment.
    case tree.parent.pair(_: Exp, b: Bind) => env.in(b)
    case tree.parent.pair(_: Exp, g: Gen)  => env.in(g)
  }

  def envout(out: RawNode => Environment): RawNode ==> Environment = {
    // The `out` environment of a comprehension must remove the scope that was inserted.
    case c: Comp       => leave(out(c))

    // The `out` environment of a function abstraction must remove the scope that was inserted.
    case f: FunAbs     => leave(out(f))

    // A new variable was defined in the current scope.
    case n @ IdnDef(i) => define(out(n), i, entity(n)) //defineIfNew(out(n), i, defentity(n))

    // The `out` environment of a `Bind`/`Gen` is the environment after the assignment.
    case Bind(idn, _)  => env(idn)
    case Gen(idn, _)   => env(idn)

    // `Exp` cannot define variables for the nodes that follow, so its `out` environment is always the same as its `in`
    // environment. That is, there is no need to go "inside" the expression to finding any bindings.
    case e: Exp        => env.in(e)
  }

  /** The expected type of an expression.
   *  Returns `UnknownType` if any type will do.
   */
  lazy val expectedType: Exp => Set[Type] = attr {
    case tree.parent.pair(e, p) => p match {
      case UnaryExp(_: Not, _)      => Set(BoolType())
      case UnaryExp(_: ToBool, _)   => Set(FloatType(), IntType())
      case UnaryExp(_: ToInt, _)    => Set(BoolType(), FloatType())
      case UnaryExp(_: ToFloat, _)  => Set(IntType())
      case UnaryExp(_: ToString, _) => Set(BoolType(), FloatType(), IntType())
      case _                        => Set(UnknownType())
    }
    case _                          => Set(UnknownType()) // There is no parent, i.e. the root node.
  }

  /** The dereferenced type of an expression.
   *
   *  Given a type defined as:
   *    RecordType(List(
   *      AttrType("foo", IntType()),
   *      AttrType("bar", ClassType("baz"))))
   *
   *  The `realTipe` attribute for bar is the user-defined type ClassType("baz"). The code, however, wants to compare
    *  with the type that "baz" points to. This attribute transparently de-references user-defined ClassTypes to their
   *  actual type,  by looking up the type definition in the `userTypes` catalog.
   */
  lazy val tipe: Exp => Type = attr {
    case e => realType(e) match {
      case ClassType(name) => world.userTypes(name)
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
    case IdnExp(idn)   => entityTipe(entity(idn))

    // Rule 4
    case RecordProj(e, idn) => tipe(e) match {
      case t: RecordType => t.atts.find(_.idn == idn) match {
        case Some(att: AttrType) => att.tipe
        case _                   => UnknownType()
      }
      case t => UnknownType()
    }

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, tipe(att.e))))

    // Rule 6
    case IfThenElse(e1, e2, e3) => (tipe(e1), tipe(e2), tipe(e3)) match {
      case (_: BoolType, t2, t3) if t2 == t3 => t2
      case _                                 => UnknownType()
    }

    // Rule 7 
    // TODO: Missing preconditions?
    case FunAbs(_, t, e) => FunType(t, tipe(e))

    // Rule 8
    case FunApp(f, e) => tipe(f) match {
      case FunType(t1, t2) if t1 == tipe(e) => t2
      case _                                => UnknownType()
    }

    // Rule 9
    case ZeroCollectionMonoid(m)    => CollectionType(m, UnknownType())

    // Rule 10
    case ConsCollectionMonoid(m, e) => CollectionType(m, tipe(e))

    // Rule 11
    case MergeMonoid(m: PrimitiveMonoid, e1, e2) => (tipe(e1), tipe(e2)) match {
      case (t1: PrimitiveType, t2) if t1 == t2 && m.isOfType(t1) => t1
      case _ => UnknownType()
    }

    // Rule 12
    case MergeMonoid(m: CollectionMonoid, e1, e2) => (tipe(e1), tipe(e2)) match {
      case (t1 @ CollectionType(m1, _), t2) if m == m1 && t1 == t2 => t1
      case _ => UnknownType()
    }

    // Rule 13
    case Comp(m: PrimitiveMonoid, Nil, e) if m.isOfType(tipe(e)) => tipe(e)

    // Rule 14
    case Comp(m: CollectionMonoid, Nil, e)                         => CollectionType(m, tipe(e))

    // Rule 15
    case Comp(m, Gen(idn, e2) :: r, e1) => tipe(e2) match {
      case CollectionType(m2, t2) if m.greaterOrEqThan(m2) => tipe(Comp(m, r, e1))
      case _ => UnknownType()
    }

    // Rule 16
    case Comp(m, (e2: Exp) :: r, e1) => tipe(e2) match {
      case _: BoolType => tipe(Comp(m, r, e1))
      case _           => UnknownType()
    }

    // Skip Bind
    case Comp(m, (_: Bind) :: r, e1) => tipe(Comp(m, r, e1))

    // Binary Expression type
    case BinaryExp(_: ComparisonOperator, e1, e2) => (tipe(e1), tipe(e2)) match {
      case (t1: NumberType, t2: NumberType) if t1 == t2 => BoolType()
      case _                                            => UnknownType()
    }
    case BinaryExp(_: EqualityOperator, e1, e2) => (tipe(e1), tipe(e2)) match {
      case (t1, t2) if t1 == t2 => BoolType()
      case _                    => UnknownType()
    }
    case BinaryExp(_: ArithmeticOperator, e1, e2) => (tipe(e1), tipe(e2)) match {
      case (t1: NumberType, t2: NumberType) if t1 == t2 => t1
      case _                                            => UnknownType()
    }

    // Unary Expression type
    case UnaryExp(_: Not, _) => BoolType()
    case UnaryExp(_: Neg, e) => tipe(e) match {
      case t: NumberType => t
      case _             => UnknownType()
    }
    case UnaryExp(_: ToBool, _)   => BoolType()
    case UnaryExp(_: ToInt, _)    => IntType()
    case UnaryExp(_: ToFloat, _)  => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case _                        => UnknownType()
  }

  lazy val entityTipe: Entity => Type = attr {
    case BindVar(e)        => tipe(e)
    case GenVar(e)         => tipe(e) match {
      case t: CollectionType => t.innerType
      case _                 => UnknownType()
    }
    case FunArg(t)         => t
    case ClassEntity(_, t) => t
    case _                 => UnknownType()
  }

}