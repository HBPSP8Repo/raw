package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution

/** Analyzes the semantics of an AST.
  * This includes the type checker/inference as well as monoid compatibility.
  *
  * The semantic analyzer reads the user types and the catalog of user-defined class entities from the World object.
  * User types are the type definitions available to the user.
  * Class entities are the data sources available to the user.
  *   e.g., in expression `e <- Events` the class entity is `Events`.
  */
class SemanticAnalyzer(tree: Calculus.Calculus, world: World) extends Attribution with LazyLogging {

  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.util.{Entity, MultipleEntity, UnknownEntity}
  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}
  import Calculus._
  import SymbolTable._

  /** Decorators on the tree.
    */
  private lazy val decorators = new Decorators(tree)

  import decorators.{chain, Chain}

  /** The semantic errors for the tree.
    */
  lazy val errors: Messages =
    collectmessages(tree) {
      case n => check(n) {

        // Identifier declared more than once in the same scope
        case d @ IdnDef(i) if entity(d) == MultipleEntity() =>
          message(d, s"$i is declared more than once")

        // Identifier used without being declared
        case u @ IdnUse(i) if entity(u) == UnknownEntity() =>
          message(u, s"$i is not declared")

        // Identifier declared has no inferred type
        case d @ IdnDef(i) if entityType(entity(d)) == NothingType() =>
          message(d, s"$i has no type")

        // Identifier used has no inferred type
        case u @ IdnUse(i) if entityType(entity(u)) == NothingType() =>
          message(u, s"$i has no type")

        case e: Exp =>
          // Mismatch between type expected and actual type
          message(e, s"expected ${expectedType(e).map{ case p => PrettyPrinter(p) }.mkString(" or ")} got ${PrettyPrinter(tipe(e))}",
            !typesCompatible(e)) ++
            check(e) {
              // Semantic error in monoid composition
              case c @ Comp(m, qs, _) =>
                qs.flatMap {
                  case Gen(_, g) => monoidsCompatible(m, g)
                  case _         => noMessages
                }.toVector
            }
      }
    }

  /** Check whether monoid is compatible with the generator expression.
    */
  private def monoidsCompatible(m: Monoid, g: Exp): Messages = {
    def errors(t: Type) = t match {
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
      case _: ConstraintCollectionType =>
        noMessages
      case _: TypeVariable =>
        noMessages
      case t =>
        message(g, s"expected collection but got ${PrettyPrinter(t)}")
    }

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
    case n @ IdnDef(idn) =>
      if (isDefinedInScope(env.in(n), idn))
        MultipleEntity()
      else
        VariableEntity(n, TypeVariable(SymbolTable.next()))
    case n @ IdnUse(idn) =>
      lookup(env.in(n), idn, lookupDataSource(idn))
  }

  private lazy val env: Chain[Environment] =
    chain(envin, envout)

  private def envin(in: RawNode => Environment): RawNode ==> Environment = {
    case n if tree.isRoot(n) => rootenv()

    // Entering new scopes
    case c: Comp     => enter(in(c))
    case b: ExpBlock => enter(in(b))

    // If we are in a function abstraction, we must open a new scope for the variable argument. But if the parent is a
    // bind, then the `in` environment of the function abstraction must be the same as the `in` environment of the
    // bind.
    case tree.parent.pair(_: FunAbs, b: Bind)               => enter(env.in(b))
    case f: FunAbs                                          => enter(in(f))

    // If we are in an expression and the parent is a bind or a generator, then the `in` environment of the expression
    // is the same as that of the parent: the environment does not include the left hand side of the assignment.
    case tree.parent.pair(_: Exp, b: Bind)        => env.in(b)
    case tree.parent.pair(_: Exp, g: Gen)         => env.in(g)
  }

  private def envout(out: RawNode => Environment): RawNode ==> Environment = {
    // Leaving a scope
    case c: Comp     => leave(out(c))
    case b: ExpBlock => leave(out(b))

    // The `out` environment of a function abstraction must remove the scope that was inserted.
    case f: FunAbs        => leave(out(f))

    // A new variable was defined in the current scope.
    case n @ IdnDef(i)    => define(out(n), i, entity(n))

    // The `out` environment of a bind or generator is the environment after the assignment.
    case Bind(p, _)       => env(p)
    case Gen(p, _)        => env(p)

    // Expressions cannot define new variables, so their `out` environment is always the same as their `in`
    // environment. The chain does not need to go "inside" the expression to finding any bindings.
    case e: Exp => env.in(e)
  }

  /** The expected type of an expression.
    */
  private lazy val expectedType: Exp => Set[Type] = attr {

    case tree.parent.pair(e, p) => p match {
      // Record projection must be over a record type that contains the identifier to project
      case RecordProj(_, idn) =>
        Set(ConstraintRecordType(Set(AttrType(idn, AnyType()))))

      // If condition must be a boolean
      case IfThenElse(e1, _, _) if e eq e1 => Set(BoolType())

      // Arithmetic operation must be over Int or Float
      case BinaryExp(_: ArithmeticOperator, e1, _) if e eq e1 => Set(IntType(), FloatType())

      // The type of the right-hand-side of a binary expression must match the type of the left-hand-side
      case BinaryExp(_, e1, e2) if e eq e2 => Set(tipe(e1))

      // Function application must be on a function type
      case FunApp(f, _) if e eq f => Set(FunType(AnyType(), AnyType()))

      // Mismatch in function application
      case FunApp(f, e1) if e eq e1 => tipe(f) match {
        case FunType(t1, _) => Set(t1)
        case _              => Set(NothingType())
      }

      // Merging number monoids requires a number type
      case MergeMonoid(_: NumberMonoid, e1, _) if e eq e1 => Set(IntType(), FloatType())

      // Merging boolean monoids requires a bool type
      case MergeMonoid(_: BoolMonoid, e1, _)   if e eq e1 => Set(BoolType())

      // Merge of collections must be with same monoid collection types
      case MergeMonoid(_: BagMonoid, e1, _)  if e eq e1 => Set(BagType(AnyType()))
      case MergeMonoid(_: ListMonoid, e1, _) if e eq e1 => Set(ListType(AnyType()))
      case MergeMonoid(_: SetMonoid, e1, _)  if e eq e1 => Set(SetType(AnyType()))

      // The type of the right-hand-side of a merge expression must match the type of the left-hand-side
      case MergeMonoid(_, e1, e2) if e eq e2 => Set(tipe(e1))

      // Comprehension with a primitive monoid must have compatible projection type
      case Comp(_: NumberMonoid, _, e1) if e eq e1 => Set(IntType(), FloatType())
      case Comp(_: BoolMonoid, _, e1)   if e eq e1 => Set(BoolType())

      // Qualifiers that are expressions must be predicates
      case Comp(_, qs, _) if qs.contains(e) => Set(BoolType())

      // Expected types of unary expressions
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
  private def typesCompatible(e: Exp): Boolean = {
    val actual = tipe(e)
    // Type checker keeps quiet on nothing types because the actual error will be signalled in one of its children
    // through the `expectedType` comparison.
    if (actual == NothingType())
      return true
    for (expected <- expectedType(e)) {
      if (expected == NothingType() || unify(expected, actual) != NothingType())
        return true
    }
    false
  }

  // TODO: Move it into a separate class to isolate its behavior with an interface!
  private var variableMap = scala.collection.mutable.Map[String, Type]()


  def VMapToStr(): String = {
    return variableMap.map{case (v: String, t: Type) => s"$v => ${PrettyPrinter(t)}"}.mkString("{\n",",\n", "}")
  }
  // TODO: Call pass1(tree.root) from a private non-lay val, to trigger the pass1 at startup.

  lazy val tipe: Exp => Type = {
    case e => {
      // Run `pass1` from the root for its side-effect, which is to type the nodes and build the `variableMap`.
      // Subsequent runs are harmless since they hit the cached value.
      pass1(tree.root)

      logger.debug(VMapToStr())

      def walk(t: Type): Type = t match {
        case _: AnyType             => t
        case _: PrimitiveType       => t
        case _: UserType            => t
        case TypeVariable(v)        => if (variableMap.contains(v) && variableMap(v) != t) walk(variableMap(v)) else t
        case RecordType(atts, name) => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, walk(t1)) }, name)
        case ListType(innerType)    => ListType(walk(innerType))
        case SetType(innerType)     => SetType(walk(innerType))
        case BagType(innerType)     => BagType(walk(innerType))
        case FunType(t1, t2)        => FunType(walk(t1), walk(t2))
        case ConstraintRecordType(atts) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, walk(t1)) })
        case ConstraintCollectionType(innerType, c, i) => ConstraintCollectionType(walk(innerType), c, i)
      }

      // Walk through type variables until the root type
      walk(pass1(e))
    }
  }

  private lazy val patternDef: Pattern => CalculusNode = attr {
    case tree.parent(b: Bind)   => b
    case tree.parent(g: Gen)    => g
    case tree.parent(f: FunAbs) => f
    case tree.parent(p: Pattern) => patternDef(p)
  }

  private lazy val entityType: Entity => Type = attr {
    case VariableEntity(idn, t)    => {
      idn match {
        case tree.parent(p: Pattern) => patternDef(p) match {
          case Bind(p1, e) =>
            unify(patternType(p1), pass1(e))
          case Gen(p1, e) =>
            unify(ConstraintCollectionType(patternType(p1), None, None), pass1(e))
          case _: FunAbs =>
        }
      }
      t
    }
    case DataSourceEntity(name) => world.sources(name)
    case _: UnknownEntity       => NothingType()
  }

  private def idnType(idn: IdnNode): Type = entityType(entity(idn))

  private def pass1(n: Exp): Type = realPass1(n) match {
    case UserType(name) => world.userTypes.get(name) match {
      case Some(t) => t
      case _       => NothingType()
    }
    case t => t
  }

  private lazy val realPass1: Exp => Type = attr {

    // Rule 1
    case _: BoolConst   => BoolType()
    case _: IntConst    => IntType()
    case _: FloatConst  => FloatType()
    case _: StringConst => StringType()

    // Rule 2
    case _: Null => TypeVariable(SymbolTable.next())

    // Rule 3
    case IdnExp(idn) => idnType(idn)

    // Rule 4
    case RecordProj(e, idn) =>
      val v = SymbolTable.next()
      val inner = TypeVariable(SymbolTable.next())
      variableMap.update(v, ConstraintRecordType(Set(AttrType(idn, inner))))
      unify(pass1(e), TypeVariable(v))
      inner

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, pass1(att.e))), None)

    // Rule 6
    case IfThenElse(_, e2, e3) => unify(pass1(e2), pass1(e3))

    // Rule 7
    case FunAbs(p, e) => FunType(patternType(p), pass1(e))

    // Rule 8
    case FunApp(f, _) => pass1(f) match {
      case FunType(_, t2) => t2
      case _ => TypeVariable(SymbolTable.next()) //??? // TODO: Add a fancy contraint that it is a FunAbs?
    }

    // Rule 9
    case ZeroCollectionMonoid(_: BagMonoid)  => BagType(TypeVariable(SymbolTable.next()))
    case ZeroCollectionMonoid(_: ListMonoid) => ListType(TypeVariable(SymbolTable.next()))
    case ZeroCollectionMonoid(_: SetMonoid)  => SetType(TypeVariable(SymbolTable.next()))

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

    // Skip Binds
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

  /** The type corresponding to a given pattern.
    */
  private def patternType(p: Pattern): Type = p match {
    case PatternIdn(idn) => entity(idn) match {
      case VariableEntity(_, t) => t
      case _                    => NothingType()
    }
    case PatternProd(ps) => RecordType(ps.zipWithIndex.map{ case (p1, idx) => AttrType(s"_${idx + 1}", patternType(p1))}, None)
  }

  /** Hindley-Milner unification algorithm.
    */
  private def unify(t1: Type, t2: Type): Type = {
    logger.debug(s"t1 is ${PrettyPrinter(t1)} and t2 is ${PrettyPrinter(t2)}")
    val nt1 = t1 match {
      case UserType(t) => world.userTypes(t)
      case t => t
    }
    val nt2 = t2 match {
      case UserType(t) => world.userTypes(t)
      case t => t
    }

    (nt1, nt2) match {
      case (n: NothingType, _) => n
      case (_, n: NothingType) => n
      case (_: AnyType, t) => t
      case (t, _: AnyType) => t
      case (t1: PrimitiveType, t2: PrimitiveType) if t1 == t2 => t1
      case (SetType(a), SetType(b)) => SetType(unify(a, b))
      case (BagType(a), BagType(b)) => BagType(unify(a, b))
      case (ListType(a), ListType(b)) => ListType(unify(a, b))
      case (FunType(a1, a2), FunType(b1, b2)) => FunType(unify(a1, b1), unify(a2, b2))
      case (RecordType(atts1, name1), RecordType(atts2, name2)) if atts1.map(_.idn) == atts2.map(_.idn) && name1 == name2 =>
        RecordType(atts1.zip(atts2).map { case (att1, att2) => AttrType(att1.idn, unify(att1.tipe, att2.tipe)) }, name1)
      case (t1 @ ConstraintRecordType(atts1), t2 @ ConstraintRecordType(atts2)) =>
        val common = atts1.map(_.idn).intersect(atts2.map(_.idn))
        val commonAttr = common.map { case idn => AttrType(idn, unify(t1.getType(idn).head, t2.getType(idn).head)) }
        ConstraintRecordType(atts1.filter { case att => !common.contains(att.idn) } ++ atts2.filter { case att => !common.contains(att.idn) } ++ commonAttr)
      case (t1@ConstraintRecordType(atts1), t2@RecordType(atts2, name)) =>
        if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet))
          NothingType()
        else
          RecordType(atts2.map { case att => t1.getType(att.idn) match {
            case Some(t) => AttrType(att.idn, unify(t, att.tipe))
            case None => att
          }
          }, name)
      case (t1: RecordType, t2: ConstraintRecordType) =>
        unify(t2, t1)
      case (ConstraintCollectionType(a1, c1, i1), ConstraintCollectionType(b1, c2, i2)) =>
        if (c1.isDefined && c2.isDefined && (c1.get != c2.get)) {
          NothingType()
        } else if (i1.isDefined && i2.isDefined && (i1.get != i2.get)) {
          NothingType()
        } else {
          val nc = if (c1.isDefined) c1 else c2
          val ni = if (i1.isDefined) i1 else i2
          ConstraintCollectionType(unify(a1, b1), nc, ni)
        }

      case (ConstraintCollectionType(a1, c1, i1), t2: SetType) =>
        if (((c1.isDefined && c1.get) || c1.isEmpty) &&
          ((i1.isDefined && i1.get) || i1.isEmpty))
          SetType(unify(a1, t2.innerType))
        else
          NothingType()

      case (ConstraintCollectionType(a1, c1, i1), t2: BagType) =>
        if (((c1.isDefined && c1.get) || c1.isEmpty) &&
          ((i1.isDefined && !i1.get) || i1.isEmpty))
          BagType(unify(a1, t2.innerType))
        else
          NothingType()

      case (ConstraintCollectionType(a1, c1, i1), t2: ListType) =>
        if (((c1.isDefined && !c1.get) || c1.isEmpty) &&
          ((i1.isDefined && !i1.get) || i1.isEmpty))
          ListType(unify(a1, t2.innerType))
        else
          NothingType()

      case (t1: CollectionType, t2: ConstraintCollectionType) =>
        unify(t2, t1)

      case (t1 @ TypeVariable(a), t2 @ TypeVariable(b)) =>
        if (t1 == t2)
          t1
        else if (variableMap.contains(a) && variableMap.contains(b)) {
          val ta = variableMap(a)
          val tb = variableMap(b)
          val nt = unify(ta, tb)
          nt match {
            case _: NothingType =>
              nt
            case _ =>
              variableMap.update(a, nt)
              variableMap.update(b, t1)
              t1
          }
        } else if (variableMap.contains(a)) {
          variableMap.update(b, t1)
          t1
        } else if (variableMap.contains(b)) {
          unify(t2, t1)
        } else {
          val nt = TypeVariable(SymbolTable.next())
          variableMap.update(a, nt)
          variableMap.update(b, t1)
          t1
        }
      case (t1 @ TypeVariable(a), t2) =>
        logger.debug(s"* t1 is ${PrettyPrinter(t1)} and t2 is ${PrettyPrinter(t2)}")
        if (variableMap.contains(a)) {
          logger.debug("**")
          val ta = variableMap(a)
          val nt = unify(ta, t2)
          nt match {
            case _: NothingType => nt
            case _ =>
              variableMap.update(a, nt)
              logger.debug(s"nt is ${PrettyPrinter(nt)}")
              nt
          }
        } else {
          logger.debug("**|")
          variableMap.update(a, t2)
          t2
        }
      case (t1, t2: TypeVariable) =>
        unify(t2, t1)
      case _ => NothingType()
    }
  }

}
