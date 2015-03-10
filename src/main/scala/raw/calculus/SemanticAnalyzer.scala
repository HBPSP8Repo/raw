package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution

/** Analyzes the semantics of an AST.
  * This includes the type checker and monoid composition.
  *
  * The semantic analyzer reads the user types and the catalog of user-defined class entities from the World object.
  * User types are the type definitions available to the user.
  * Class entities are the data sources available to the user.
  *   e.g., in expression `e <- Events` the class entity is `Events`.
  */
class SemanticAnalyzer(tree: Calculus.Calculus, world: World) extends Attribution with LazyLogging {

  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.rewriting.Rewriter._
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
        case d @ IdnDef(i, _) if entity(d) == MultipleEntity() =>
          message(d, s"$i is declared more than once")

        // Identifier used without being declared
        case u @ IdnUse(i) if entity(u) == UnknownEntity() =>
          message(u, s"$i is not declared")

        case e: Exp =>
          // Mismatch between type expected and actual type
          message(e, s"expected ${expectedType(e).map{ case p => PrettyPrinter(p) }.mkString(" or ")} got ${PrettyPrinter(tipe(e))}",
            !typesCompatible(e)) ++
            check(e) {
              // Semantic error in monoid composition
              case Comp(m, qs, _) =>
                qs.flatMap {
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
                        message(g, s"expected collection but got ${PrettyPrinter(t)}")
                    }
                  }
                  case _         => noMessages
                }.toVector
            }
      }
    }

  /** Looks up identifier in the World catalog. If it is in catalog returns a new `ClassEntity` instance.
    * Note that when looking up a given identifier, a new `ClassEntity` instance is generated each time, to ensure that
    * reference equality comparisons work later on.
    */
  def lookupCatalog(idn: String): Entity =
    if (world.catalog.contains(idn))
      ClassEntity(idn, world.catalog(idn).tipe)
    else
      UnknownEntity()

    lazy val entity: IdnNode => Entity = attr {
      case n @ IdnDef(idn, t) => {
        if (isDefinedInScope(env.in(n), idn))
          MultipleEntity()
        else n match {
          case tree.parent(p) => {
            p match {
              case Bind(_, e) => BindEntity(t, e)
              case Gen(_, e) => GenEntity(t, e)
              case _: FunAbs => FunAbsEntity(t)
            }
          }
        }
      }
      case n @ IdnUse(idn) => lookup(env.in(n), idn, lookupCatalog(idn))
    }

  def freshVar(t: Type): Variable = {
    val v = new Variable()
    variableMap.update(v, t)
    v
  }

  lazy val env: Chain[Environment] =
    chain(envin, envout)

  def envin(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()

    // Entering new scopes
    case c: Comp     => enter(in(c))
    case b: ExpBlock => enter(in(b))

    // If we are in a function abstraction, we must open a new scope for the variable argument. But if the parent is a
    // `Bind`, then the `in` environment of the function abstraction must be the same as the `in` environment of the
    // `Bind`.
    case tree.parent.pair(_: FunAbs, b: Bind) => enter(env.in(b))
    case f: FunAbs                            => enter(in(f))

    // If we are in an expression and the parent is a `Bind` or a `Gen`, then the `in` environment of the expression is
    // the same as that of the parent `Bind` or `Gen`. That is, it does not include the lhs of the assignment.
    case tree.parent.pair(_: Exp, b: Bind) => env.in(b)
    case tree.parent.pair(_: Exp, g: Gen)  => env.in(g)
  }

  def envout(out: RawNode => Environment): RawNode ==> Environment = {
    // Leaving a scope
    case c: Comp     => leave(out(c))
    case b: ExpBlock => leave(out(b))

    // The `out` environment of a function abstraction must remove the scope that was inserted.
    case f: FunAbs => leave(out(f))

    // A new variable was defined in the current scope.
    case n @ IdnDef(i, _) => define(out(n), i, entity(n))

    // The `out` environment of a `Bind`/`Gen` is the environment after the assignment.
    case Bind(idn, _) => env(idn)
    case Gen(idn, _)  => env(idn)

    // `Exp` cannot define variables for the nodes that follow, so its `out` environment is always the same as its `in`
    // environment. That is, there is no need to go "inside" the expression to finding any bindings.
    case e: Exp => env.in(e)
  }

  /** The expected type of an expression.
    */
  lazy val expectedType: Exp => Set[Type] = attr {

    case tree.parent.pair(e, p) => p match {
      case RecordProj(_, idn) => Set(RecordType(List(AttrType(idn, AnyType()))))

      // If condition must be a boolean
      case IfThenElse(e1, _, _) if e eq e1 => Set(BoolType())

      // Arithmetic operation must be over Int or Float
      case BinaryExp(_: ArithmeticOperator, e1, _) if e eq e1 => Set(IntType(), FloatType())

      // Function application must be on a function type
      case FunApp(f, _) if e eq f => Set(FunType(AnyType(), AnyType()))

      // Mismatch in function application
      case FunApp(f, e1) if e eq e1 => tipe(f) match {
        case FunType(t1, _) => Set(t1)
        case _              => Set(NothingType())
      }

      case MergeMonoid(_: NumberMonoid, e1, _) if e eq e1 => Set(IntType(), FloatType())
      case MergeMonoid(_: BoolMonoid, e1, _)   if e eq e1 => Set(BoolType())

      // Merge of collections must be with same monoid collection types
      case MergeMonoid(_: BagMonoid, e1, _)  if e eq e1 => Set(BagType(AnyType()))
      case MergeMonoid(_: ListMonoid, e1, _) if e eq e1 => Set(ListType(AnyType()))
      case MergeMonoid(_: SetMonoid, e1, _)  if e eq e1 => Set(SetType(AnyType()))

      // Comprehension with a primitive monoid must have compatible projection type
      case Comp(_: NumberMonoid, _, e1) if e eq e1 => Set(IntType(), FloatType())
      case Comp(_: BoolMonoid, _, e1)   if e eq e1 => Set(BoolType())

      // Qualifiers that are expressions must be predicates
      case Comp(_, qs, _) if qs.contains(e) => Set(BoolType())

      case UnaryExp(_: Neg, _)      => Set(IntType(), FloatType())
      case UnaryExp(_: Not, _)      => Set(BoolType())
      case UnaryExp(_: ToBool, _)   => Set(IntType(), FloatType())
      case UnaryExp(_: ToInt, _)    => Set(BoolType(), FloatType())
      case UnaryExp(_: ToFloat, _)  => Set(BoolType(), IntType())
      case UnaryExp(_: ToString, _) => Set(BoolType(), IntType(), FloatType())

      case _ => Set(AnyType())
    }
    case _ => Set(AnyType()) // There is no parent, i.e. the root node.
  }

  /** Checks for type compatibility between expected and actual types of an expression.
    * Uses type unification but overrides it to handle the special case of record projections.
    */
  def typesCompatible(e: Exp): Boolean = {
    val actual = tipe(e)
    for (expected <- expectedType(e)) {
      (expected, actual) match {
        case (RecordType(atts1), RecordType(atts2)) =>
          // Handle the special case of an expected type being a record type containing a given identifier.
          val idn = atts1(0).idn
          if (atts2.collect { case AttrType(`idn`, _) => true }.nonEmpty) return true
        case _ => if (unify(expected, actual) != NothingType()) return true
      }
    }
    false
  }

  var variableMap = scala.collection.mutable.Map[Variable, Type]()

  /** Run once for its side-effect, which is to type the nodes and build the `variableMap`.
    */
  pass1(tree.root)

  def tipe(e: Exp): Type = {
    def walk(t: Type): Type = t match {
      case TypeVariable(v) => walk(variableMap(v))
      case RecordType(atts) => RecordType(atts.map{case AttrType(iAtt, tAtt) => AttrType(iAtt, walk(tAtt))})
      case ListType(innerType) => ListType(walk(innerType))
      case SetType(innerType) => SetType(walk(innerType))
      case BagType(innerType) => BagType(walk(innerType))
      case FunType(aType, eType) => FunType(walk(aType), walk(eType))
      // case ProductType?
      case _ => t
    }
    walk(pass1(e))
  }

  lazy val realEntityType: Entity => Type = attr {
    case BindEntity(Some(t), e) => unify(t, pass1(e))
    case BindEntity(None, e) => unify(TypeVariable(freshVar(AnyType())), pass1(e))
    case GenEntity(Some(t), e) => pass1(e) match {
      case c: CollectionType => unify(t, c.innerType)
      case _ => NothingType()
    }
    case GenEntity(None, e) => pass1(e) match {
      case c: CollectionType => unify(TypeVariable(freshVar(AnyType())), c.innerType)
      case _ => NothingType()
    }
    case FunAbsEntity(Some(t)) => t
    case FunAbsEntity(None) => TypeVariable(freshVar(AnyType()))
    case ClassEntity(_, t) => t
  }

  def entityType(e: Entity): Type = realEntityType(e) match {
    case ClassType(name) => world.userTypes(name)
    case t => t
  }

  def idnType(idn: IdnNode): Type = entityType(entity(idn))

  lazy val pass1: Exp => Type = attr {

    // Rule 1
    case _: BoolConst   => BoolType()
    case _: IntConst    => IntType()
    case _: FloatConst  => FloatType()
    case _: StringConst => StringType()

    // Rule 2
    case _: Null => TypeVariable(freshVar(AnyType()))

    // Rule 3
    case IdnExp(idn) => idnType(idn)

    // Rule 4
    case RecordProj(e, idn) => pass1(e) match {
      case RecordType(atts) => atts.find(_.idn == idn) match {
        case Some(att: AttrType) => att.tipe
        case _                   => NothingType()
      }
      case _                => AnyType()
    }

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, pass1(att.e))))

    // Rule 6
    case IfThenElse(_, e2, e3) => unify(pass1(e2), pass1(e3))

    // Rule 7
    case FunAbs(idn, e) => FunType(idnType(idn), pass1(e))

    // Rule 8
    case FunApp(f, _) => pass1(f) match {
      case FunType(_, t2) => t2
      case _ => AnyType()
    }

    // Rule 9
    case ZeroCollectionMonoid(_: BagMonoid)  => BagType(TypeVariable(freshVar(AnyType())))
    case ZeroCollectionMonoid(_: ListMonoid) => ListType(TypeVariable(freshVar(AnyType())))
    case ZeroCollectionMonoid(_: SetMonoid)  => SetType(TypeVariable(freshVar(AnyType())))

    // Rule 10
    case ConsCollectionMonoid(_: BagMonoid, e)  => BagType(pass1(e))
    case ConsCollectionMonoid(_: ListMonoid, e) => ListType(pass1(e))
    case ConsCollectionMonoid(_: SetMonoid, e)  => SetType(pass1(e))

    // Rule 11
    case MergeMonoid(_: BoolMonoid, e1, e2) =>
      unify(pass1(e1), pass1(e2))
      BoolType()
    case MergeMonoid(_: PrimitiveMonoid, e1, e2) =>
      unify(pass1(e1), pass1(e2))

    // Rule 12
    case MergeMonoid(_: CollectionMonoid, e1, e2) => unify(pass1(e1), pass1(e2))

    // Rule 13
    case Comp(m: PrimitiveMonoid, Nil, e) => pass1(e)

    // Rule 14
    case Comp(_: BagMonoid, Nil, e)  => BagType(pass1(e))
    case Comp(_: ListMonoid, Nil, e) => ListType(pass1(e))
    case Comp(_: SetMonoid, Nil, e)  => SetType(pass1(e))

    // Rule 15
    case Comp(m, (_: Gen) :: r, e1) => pass1(Comp(m, r, e1))

    // Rule 16
    case Comp(m, (_: Exp) :: r, e1) => pass1(Comp(m, r, e1))

    // Skip Bind
    case Comp(m, (_: Bind) :: r, e1) => pass1(Comp(m, r, e1))

    // Binary Expression type
    case BinaryExp(_: EqualityOperator, e1, e2) =>
      unify(pass1(e1), pass1(e2))
      BoolType()

    case BinaryExp(_: ComparisonOperator, e1, e2) =>
      unify(pass1(e1), pass1(e2))
      BoolType()

    case BinaryExp(_: ArithmeticOperator, e1, e2) =>
      unify(pass1(e1), pass1(e2))

    // Unary Expression type
    case UnaryExp(_: Not, _)      => BoolType()
    case UnaryExp(_: Neg, e)      => pass1(e)
    case UnaryExp(_: ToBool, _)   => BoolType()
    case UnaryExp(_: ToInt, _)    => IntType()
    case UnaryExp(_: ToFloat, _)  => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    // Expression block type
    case ExpBlock(_, e) => pass1(e)

    case _ => NothingType()
  }

  def unify(t1: Type, t2: Type): Type = (t1, t2) match {
    case (n: NothingType, _) => n
    case (_, n: NothingType) => n
    case (_: AnyType, t) => t
    case (t, _: AnyType) => t
    case (a: PrimitiveType, b: PrimitiveType) if a == b => a
    case (BagType(a), BagType(b)) => BagType(unify(a, b))
    case (ListType(a), ListType(b)) => ListType(unify(a, b))
    case (SetType(a), SetType(b)) => SetType(unify(a, b))
    case (FunType(a1, a2), FunType(b1, b2)) => FunType(unify(a1, b1), unify(a2, b2))
    case (RecordType(atts1), RecordType(atts2)) =>
      if (atts1.collect { case AttrType(idn, _) => idn } != atts2.collect { case AttrType(idn, _) => idn })
        NothingType()
      else
        RecordType(atts1.zip(atts2).map{ case (att1, att2) => AttrType(att1.idn, unify(att1.tipe, att2.tipe))})
    case (t1 @ TypeVariable(a), TypeVariable(b)) =>
      val ta = variableMap(a)
      val tb = variableMap(b)
      val nt = unify(ta, tb)
      variableMap.update(a, nt)
      variableMap.update(b, t1)
      nt
    case (TypeVariable(a), b) =>
      val ta = variableMap(a)
      val nt = unify(ta, b)
      variableMap.update(a, nt)
      nt
    case (a, b: TypeVariable) =>
      unify(b, a)
    case _ => NothingType()
  }

  def debugTreeTypes =
    everywherebu(query[Exp] {
      case n => logger.error(CalculusPrettyPrinter(tree.root, debug = Some({ case `n` => s"[${PrettyPrinter(tipe(n))}] " })))
    })(tree.root)

}
