package raw
package calculus

import org.kiama.attribution.Attribution

/** Analyzes the semantics of an AST.
  * This includes the type checker and monoid composition.
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
        case d @ IdnDef(i) if entity(d) == MultipleEntity() =>
          message(d, s"$i is declared more than once")

        // Identifier used without being declared
        case u @ IdnUse(i) if entity(u) == UnknownEntity() =>
          message(u, s"$i is not declared")

        case e: Exp =>
          // Mismatch between type expected and actual type
          message(e, s"expected ${expectedType(e) mkString "or"} got ${tipe(e)}",
            !expectedType(e).exists(Types.compatible(_, tipe(e)))) ++
            check(e) {
              // Semantic error in monoid composition
              case Comp(m, qs, _) =>
                qs.flatMap{
                  case Gen(v, g) => {
                    tipe(g) match {
                      case _: SetType =>
                        if (!m.commutative && !m.idempotent)
                          message(m, "expected a commutative and idempotent monoid")
                        else if (!m.commutative)
                          message(m, "expected a commutative monoid")
                        else if (!m.idempotent)
                          message(m, "expected an idempotent monoid")
                        else
                          noMessages
                      case _: BagType =>
                        if (!m.commutative)
                          message(m, "expected a commutative monoid")
                        else
                          noMessages
                      case _: ListType =>
                        noMessages
                      case t =>
                        message(t, s"expected collection but got $t")
                    }
                  }
                  case _ => noMessages
                }.toIndexedSeq
            }
      }
    }

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
      case RecordProj(_, idn) => Set(RecordType(List(AttrType(idn, UnknownType()))))

      case IfThenElse(e1, _, _) if e eq e1 => Set(BoolType())
      case IfThenElse(_, e2, e3) if e eq e3 => Set(tipe(e2))

      case BinaryExp(_: ComparisonOperator, e1, _) if e eq e1 => Set(NumberType())
      case BinaryExp(_: ArithmeticOperator, e1, _) if e eq e1 => Set(NumberType())

      // Right-hand side of any binary expression must have the same type as the left-hand side
      case BinaryExp(_, e1, e2) if e eq e2 => Set(tipe(e1))

      // Function application on a non-function type
      case FunApp(f, _) if e eq f => Set(FunType(UnknownType(), UnknownType()))

      // Mismatch in function application
      case FunApp(f, e1) if e eq e1 => tipe(f) match {
        case FunType(t1, _) => Set(t1)
        case _              => Set(UnknownType())
      }

      case MergeMonoid(_: NumberMonoid, e1, _) if e eq e1 => Set(NumberType())
      case MergeMonoid(_: BoolMonoid, e1, _) if e eq e1   => Set(BoolType())

      // Merge of collections must be with same monoid collection types
      case MergeMonoid(_: BagMonoid, e1, _) if e eq e1  => Set(BagType(UnknownType()))
      case MergeMonoid(_: ListMonoid, e1, _) if e eq e1 => Set(ListType(UnknownType()))
      case MergeMonoid(_: SetMonoid, e1, _) if e eq e1  => Set(SetType(UnknownType()))

      // Right-hand side of any merge must have the same type as the left-hand side
      case MergeMonoid(_, e1, e2) if e eq e2 => Set(tipe(e1))

      // Comprehension with a primitive monoid must have compatible projection type
      case Comp(_: NumberMonoid, _, e1) if e eq e1 => Set(NumberType())
      case Comp(_: BoolMonoid, _, e1) if e eq e1   => Set(BoolType())

      // Qualifiers that are expressions (i.e. where there is an `expectedType`) must be predicates
      case Comp(_, qs, _) if qs.exists{case q => q eq e} => Set(BoolType())

      case UnaryExp(_: Neg, _)      => Set(NumberType())
      case UnaryExp(_: Not, _)      => Set(BoolType())
      case UnaryExp(_: ToBool, _)   => Set(NumberType())
      case UnaryExp(_: ToNumber, _) => Set(BoolType(), NumberType())
      case UnaryExp(_: ToString, _) => Set(BoolType(), NumberType())

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
    case _: NumberConst => NumberType()
    case _: StringConst => StringType()

    // Rule 2
    case _: Null => UnknownType()

    // Rule 3
    case IdnExp(idn) => idnNodeType(idn)

    // Rule 4
    case RecordProj(e, idn) => tipe(e) match {
      case t: RecordType => t.atts.find(_.idn == idn) match {
        case Some(att: AttrType) => att.tipe
        case _                   => UnknownType()
      }
      case _             => UnknownType()
    }

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, tipe(att.e))))

    // Rule 6
    case IfThenElse(_, e2, _) => tipe(e2)

    // Rule 7 
    case FunAbs(_, t, e) => FunType(t, tipe(e))

    // Rule 8
    case FunApp(f, _) => tipe(f) match {
      case FunType(_, t2) => t2
      case _               => UnknownType()
    }

    // Rule 9
    case ZeroCollectionMonoid(_: BagMonoid)  => BagType(UnknownType())
    case ZeroCollectionMonoid(_: ListMonoid) => ListType(UnknownType())
    case ZeroCollectionMonoid(_: SetMonoid)  => SetType(UnknownType())

    // Rule 10
    case ConsCollectionMonoid(_: BagMonoid, e)  => BagType(tipe(e))
    case ConsCollectionMonoid(_: ListMonoid, e) => ListType(tipe(e))
    case ConsCollectionMonoid(_: SetMonoid, e)  => SetType(tipe(e))

    // Rule 11
    case MergeMonoid(_: PrimitiveMonoid, e1, _) => tipe(e1)

    // Rule 12
    case MergeMonoid(_: CollectionMonoid, e1, _) => tipe(e1)

    // Rule 13
    case Comp(m: PrimitiveMonoid, Nil, e) => tipe(e)

    // Rule 14
    case Comp(_: BagMonoid, Nil, e)  => BagType(tipe(e))
    case Comp(_: ListMonoid, Nil, e) => ListType(tipe(e))
    case Comp(_: SetMonoid, Nil, e)  => SetType(tipe(e))

    // Rule 15
    case Comp(m, (_: Gen) :: r, e1) => tipe(Comp(m, r, e1))

    // Rule 16
    case Comp(m, (_: Exp) :: r, e1) => tipe(Comp(m, r, e1))

    // Skip Bind
    case Comp(m, (_: Bind) :: r, e1) => tipe(Comp(m, r, e1))

    // Binary Expression type
    case BinaryExp(_: ComparisonOperator, _, _)  => BoolType()
    case BinaryExp(_: EqualityOperator, _, _)    => BoolType()
    case BinaryExp(_: ArithmeticOperator, e1, _) => tipe(e1)

    // Unary Expression type
    case UnaryExp(_: Not, _)      => BoolType()
    case UnaryExp(_: Neg, e)      => tipe(e)
    case UnaryExp(_: ToBool, _)   => BoolType()
    case UnaryExp(_: ToNumber, _) => NumberType()
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

  def idnNodeType(idn: IdnNode): Type = entityTipe(entity(idn))

}