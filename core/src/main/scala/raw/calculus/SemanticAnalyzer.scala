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
  import scala.collection.immutable.Seq

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
        case d @ IdnDef(i, _) if entity(d) == MultipleEntity() =>
          message(d, s"$i is declared more than once")

        // Identifier used without being declared
        case u @ IdnUse(i) if entity(u) == UnknownEntity() =>
          message(u, s"$i is not declared")

        // Identifier declared has no inferred type
        case d @ IdnDef(i, _) if entityType(entity(d)) == NothingType() =>
          message(d, s"$i has no type")

        // Identifier used has no inferred type
        case u @ IdnUse(i) if entityType(entity(u)) == NothingType() =>
          message(u, s"$i has no type")

        // Check whether pattern structure matches expression type
//        case Bind(p, e) => patternCompatible(p, tipe(e), e)
//        case Gen(p, e)  => tipe(e) match {
//          case t: CollectionType => patternCompatible(p, t.innerType, e)
//          case _                 => noMessages // Initial error is already signaled elsewhere
//        }

          // TODO: REname 'e'
        case e: Qual =>
          // Mismatch between type expected and actual type
          message(e, s"expected ${expectedType(e).map{ case p => PrettyPrinter(p) }.mkString(" or ")} got ${PrettyPrinter(tipe(e))}",
            !typesCompatible(e)) ++
            check(e) {

              // Semantic error in monoid composition
              case c @ Comp(m, qs, _) =>
                logger.debug(s"========================================================== Comp is ${CalculusPrettyPrinter(c)}")
                qs.flatMap {
                  case Gen(_, g)        => monoidsCompatible(m, g)
                  case _                => noMessages
                }.toVector
            }
      }
    }

//  // Return error messages if the pattern is not compatible with the type.
//  // `e` is only used as a marker to position the error message
//  private def patternCompatible(p: Pattern, t: Type, e: Exp): Messages = (p, t) match {
//    case (PatternProd(ps), RecordType(atts, _)) =>
//      if (ps.length != atts.length)
//        message(e, s"expected record with ${ps.length} attributes but got record with ${atts.length} attributes")
//      else
//        ps.zip(atts).flatMap { case (p1, att) => patternCompatible(p1, att.tipe, e) }.toVector
//    case (p1: PatternProd, t1) =>
//      message(e, s"expected ${PrettyPrinter(patternType(p1))} but got ${PrettyPrinter(t1)}")
//    case (_: PatternIdn, _) => noMessages
//  }

  // Return error message if the monoid is not compatible with the generator expression type.
  private def monoidsCompatible(m: Monoid, g: Exp) = {
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
      case t =>
        message(g, s"expected collection but got ${PrettyPrinter(t)}")
    }

    tipe(g) match {
      case UserType(t) => errors(world.userTypes(t))
      case t => errors(t)
    }
  }

  // Looks up the identifier in the World. If present, return a a new entity instance.
  private def lookupDataSource(idn: String): Entity =
    if (world.sources.contains(idn))
      DataSourceEntity(idn)
    else
      UnknownEntity()

  lazy val entity: IdnNode => Entity = attr {
    case n @ IdnDef(idn, _) =>
      if (isDefinedInScope(env.in(n), idn))
        MultipleEntity()
      else
        VariableEntity(n)

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
    case n @ IdnDef(i, _) => define(out(n), i, entity(n))

    // The `out` environment of a bind or generator is the environment after the assignment.
    case Bind(p, _)       => env(p)
    case Gen(p, _)        => env(p)

    // Expressions cannot define new variables, so their `out` environment is always the same as their `in`
    // environment. The chain does not need to go "inside" the expression to finding any bindings.
    case e: Exp => env.in(e)
  }

  // The expected type of an expression.
  private lazy val expectedType: Qual => Set[Type] = attr {

    case tree.parent.pair(e, p) => p match {
      // Record projection must be over a record type that contains the identifier to project
      case RecordProj(_, idn) => Set(RecordType(List(AttrType(idn, AnyType())), None))

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

      // TODO: Add here rules for Bind and Gen: if in an expression, its type must be compatible with the type of the pattern?
      // TODO: And btw, in a pattern, its type must be compatiblee with the expression
      // TODO: So make one of them match the other; as usual, the 2nd matches the 1st.

      case _ => Set(AnyType())
    }
    case _ => Set(AnyType()) // There is no parent, i.e. the root node.
  }

  /** Checks for type compatibility between expected and actual types of an expression.
    * Uses type unification but overrides it to handle the special case of record projections.
    */
  private def typesCompatible(n: Qual): Boolean = {
    val actual = tipe(n)
    // Type checker keeps quiet on nothing types because the actual error will be signalled in one of its children
    // through the `expectedType` comparison.
    if (actual == NothingType())
      return true
    for (expected <- expectedType(n)) {
      // Type checker keeps quiet on nothing types because the actual error will be signalled in one of its children
      // through the `expectedType` comparison.
      if (expected == NothingType())
        return true

      (expected, actual) match {
        case (RecordType(atts1, _), RecordType(atts2, _)) =>
          // Handle the special case of an expected type being a record type containing a given identifier.
          val idn = atts1.head.idn
          if (atts2.collect { case AttrType(`idn`, _) => true }.nonEmpty) return true
        case _ => if (unify(expected, actual) != NothingType()) return true
      }
    }
    false
  }

  // TODO: Move it into a separate class to isolate its behavior with an interface!
  private var variableMap = scala.collection.mutable.Map[Variable, Type]()

  def VMapToStr(): String = {
    return variableMap.map{case (v: Variable, t: Type) => s"$v => ${PrettyPrinter(t)}"}.mkString("{\n",",\n", "}")
  }

  private def getVariable(v: Variable) = {
    if (!variableMap.contains(v))
      variableMap.put(v, AnyType())
    variableMap(v)
  }

  lazy val tipe: Qual => Type = {
    case n => {
      // Run `pass1` from the root for its side-effect, which is to type the nodes and build the `variableMap`.
      // Subsequent runs are harmless since they hit the cached value.
      //val cucu = query[Qual] { case n1: Qual => println(">>> " + n1); pass1(n1) }(tree.root)

      logger.debug("Typing...")

      pass1(tree.root)


      logger.debug(s"Looking for type of ${CalculusPrettyPrinter(n)}")
      logger.debug(s"*** FINAL varMap")
      logger.debug(VMapToStr())

      def walk(t: Type): Type = t match {
        case TypeVariable(v)        => walk(getVariable(v))
        case RecordType(atts, name) => RecordType(atts.map { case AttrType(iAtt, tAtt) => AttrType(iAtt, walk(tAtt)) }, name)
        case ListType(innerType)    => ListType(walk(innerType))
        case SetType(innerType)     => SetType(walk(innerType))
        case BagType(innerType)     => BagType(walk(innerType))
        case FunType(aType, eType)  => FunType(walk(aType), walk(eType))
        case _                      => t
      }

      val t = walk(pass1(n))
      logger.debug(s"Found type ${PrettyPrinter(t)}")
      t
    }
  }

  private lazy val patternDef: Pattern => CalculusNode = attr {
    case tree.parent(b: Bind)   => b
    case tree.parent(g: Gen)    => g
    case tree.parent(f: FunAbs) => f
    case tree.parent(p: Pattern) => patternDef(p)
  }

  private lazy val entityType: Entity => Type = attr {
    case VariableEntity(idnDef @ IdnDef(idn, t))    => {
      idnDef match {
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

  private def pass1(n: Qual): Type = realPass1(n) match {
    case UserType(name) => world.userTypes.get(name) match {
      case Some(t) => t
      case _       => NothingType()
    }
    case t => t
  }

  private lazy val realPass1: Qual => Type = attr {

    // Rule 1
    case _: BoolConst   => BoolType()
    case _: IntConst    => IntType()
    case _: FloatConst  => FloatType()
    case _: StringConst => StringType()

    // Rule 2
    case _: Null => TypeVariable(new Variable())

    // Rule 3
    case IdnExp(idn) => idnType(idn)

    // Rule 4
//    case RecordProj(e, idn) =>
//      logger.debug(s"We got here for idn $idn and e $e and pass1(e) is ${pass1(e)}")
//      pass1(e) match {
//        case RecordType(atts, _) => atts.find(_.idn == idn) match {
//          case Some(att: AttrType) => att.tipe
//          case _                   => NothingType()
//        }
//        case _                   => AnyType()
//      }
    case RecordProj(e, idn) =>
      logger.debug(s"*** RecordProj e ${CalculusPrettyPrinter(e)} and idn is $idn")
      logger.debug(s"e type is ${pass1(e)}")
      logger.debug(s"pre ${VMapToStr()}")
      val v = new Variable()
      val inner = TypeVariable(new Variable())
      variableMap.put(v, ConstraintRecordType(Set(AttrType(idn, inner))))
      val x = unify(pass1(e), TypeVariable(v))
      logger.debug(s"post ${VMapToStr()}")
      logger.debug(s"unify is ${PrettyPrinter(x)}")
      logger.debug(s"return type is ${PrettyPrinter(inner)}")
      inner
//      x match {
//        case r: RecordType => r.getType(idn).get
//        case r: ConstraintRecordType => r.getType(idn).get
//        case t: TypeVariable => variableMap.get(t.v)
//      }

//      pass1(e) match {
//        case RecordType(atts, _) => atts.find(_.idn == idn) match {
//          case Some(att: AttrType) => att.tipe
//          case _                   => NothingType()
//        }
//        case t: TypeVariable =>
//
//
//          val x = unify(t, RecordType(Seq(AttrType(idn, TypeVariable(new Variable()))), None)); logger.debug(s"x is $x"); x match {
//          case RecordType(atts, _) => atts.find(_.idn == idn) match {
//            case Some(att: AttrType) => att.tipe
//            case _ => ???
//          }
//        }
//        case _ => NothingType() // ???
//        //case _                   => AnyType() // ???
//      }



//      if i unified 'e' iwth a record type with 'idn'
//      then my own type is the projection of a another type?
//    so i must point to the other type variable... and project out?
//
//    so my type is the projection
//
//    so first i unify
//    then i project out

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, pass1(att.e))), None)

    // Rule 6
    case IfThenElse(_, e2, e3) => unify(pass1(e2), pass1(e3))

    // Rule 7
    case FunAbs(p, e) => FunType(patternType(p), pass1(e))

    // Rule 8
    case FunApp(f, _) => pass1(f) match {
      case FunType(_, t2) => t2
      case _ => TypeVariable(new Variable()) //??? // TODO: Add a fancy contraint that it is a FunAbs?
    }

    // Rule 9
    case ZeroCollectionMonoid(_: BagMonoid)  => BagType(TypeVariable(new Variable()))
    case ZeroCollectionMonoid(_: ListMonoid) => ListType(TypeVariable(new Variable()))
    case ZeroCollectionMonoid(_: SetMonoid)  => SetType(TypeVariable(new Variable()))

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

//    // Bind unifies the type of the pattern with the type of the expression although it has NothingType()
//    case Bind(p, e) =>
//      unify(patternType(p), pass1(e))
//      NothingType()
//
//    // Gen is similar to Bind but extracts the inner type of the expression, which must be a collection
//    case Gen(p, e) =>
//      logger.debug(s"*** Gen(p, e) p ${CalculusPrettyPrinter(p)} e ${CalculusPrettyPrinter(e)}")
//      val v = new Variable()
//      logger.debug(s"p type is ${patternType(p)}")
//      logger.debug(s"e type is ${pass1(e)}")
//      logger.debug(s"pre ${VMapToStr()}")
//      variableMap.put(v, ConstraintCollectionType(patternType(p), None, None))
//      val x = unify(pass1(e), TypeVariable(v))
//      logger.debug(s"post ${VMapToStr()}")
//      logger.debug(s"unify is ${PrettyPrinter(x)}")
//      NothingType()

    case _ => NothingType()
  }

  // Return the type corresponding to a given pattern
  private def patternType(p: Pattern): Type = p match {
    case PatternIdn(idn) => entity(idn) match {
      case VariableEntity(idn) => idn.t
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
      case (a: PrimitiveType, b: PrimitiveType) if a == b => a
      case (SetType(a), SetType(b)) => SetType(unify(a, b))
      case (BagType(a), BagType(b)) => BagType(unify(a, b))
      case (ListType(a), ListType(b)) => ListType(unify(a, b))
      case (FunType(a1, a2), FunType(b1, b2)) => FunType(unify(a1, b1), unify(a2, b2))
      case (RecordType(atts1, Some(name1)), RecordType(atts2, Some(name2))) if atts1.map(_.idn) == atts2.map(_.idn) && name1 == name2 =>
        // Create a record type with same name
        RecordType(atts1.zip(atts2).map { case (att1, att2) => AttrType(att1.idn, unify(att1.tipe, att2.tipe)) }, Some(name1))
      case (RecordType(atts1, None), RecordType(atts2, None)) if atts1.map(_.idn) == atts2.map(_.idn) =>
        // Create an 'anonymous' record type
        RecordType(atts1.zip(atts2).map { case (att1, att2) => AttrType(att1.idn, unify(att1.tipe, att2.tipe)) }, None)

      case (a@ConstraintRecordType(atts1), b@ConstraintRecordType(atts2)) =>
        val common = atts1.map(_.idn).intersect(atts2.map(_.idn))
        val commonAttr = common.map { case idn => AttrType(idn, unify(a.getType(idn).head, b.getType(idn).head)) }
        ConstraintRecordType(atts1.filter { case att => !common.contains(att.idn) } ++ atts2.filter { case att => !common.contains(att.idn) } ++ commonAttr)
      case (a@ConstraintRecordType(atts1), b@RecordType(atts2, name)) =>
        if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet))
          NothingType()
        else
          RecordType(atts2.map { case att => a.getType(att.idn) match {
            case Some(t) => AttrType(att.idn, unify(t, att.tipe))
            case None => att
          }
          }, name)
      case (a: RecordType, b: ConstraintRecordType) =>
        unify(b, a)

      case (a@ConstraintCollectionType(t1, c1, i1), b@ConstraintCollectionType(t2, c2, i2)) =>
        if (c1.isDefined && c2.isDefined && (c1.get != c2.get)) {
          NothingType()
        } else if (i1.isDefined && i2.isDefined && (i1.get != i2.get)) {
          NothingType()
        } else {
          val nc = if (c1.isDefined) c1 else c2
          val ni = if (i1.isDefined) i1 else i2
          ConstraintCollectionType(unify(t1, t2), nc, ni)
        }

      case (a@ConstraintCollectionType(t1, c1, i1), b: SetType) =>
        if (((c1.isDefined && c1.get) || !c1.isDefined) &&
          ((i1.isDefined && i1.get) || !i1.isDefined))
          SetType(unify(t1, b.innerType))
        else
          NothingType()

      case (a@ConstraintCollectionType(t1, c1, i1), b: BagType) =>
        if (((c1.isDefined && c1.get) || !c1.isDefined) &&
          ((i1.isDefined && !i1.get) || !i1.isDefined))
          BagType(unify(t1, b.innerType))
        else
          NothingType()

      case (a@ConstraintCollectionType(t1, c1, i1), b: ListType) =>
        if (((c1.isDefined && !c1.get) || !c1.isDefined) &&
          ((i1.isDefined && !i1.get) || !i1.isDefined))
          ListType(unify(t1, b.innerType))
        else
          NothingType()

      case (a: CollectionType, b: ConstraintCollectionType) =>
        unify(b, a)

      case (t1@TypeVariable(a), t2@TypeVariable(b)) =>
        if (t1 == t2)
          t1
        else {
          val ta = getVariable(a)
          val tb = getVariable(b)
          val nt = unify(ta, tb)
          nt match {
            case _: NothingType =>
              nt
            case _ =>
              variableMap.update(a, nt)
              variableMap.update(b, t1)
              t1
          }
        }
      case (TypeVariable(a), b) =>
        val ta = getVariable(a)
        val nt = unify(ta, b)
        nt match {
          case _: NothingType => nt
          case _ =>
            variableMap.update(a, nt)
            nt
        }
      case (a, b: TypeVariable) =>
        unify(b, a)
      case _ => NothingType()
    }
  }

  def debugTreeTypes =
    everywherebu(query[Exp] {
      case n => logger.error(CalculusPrettyPrinter(tree.root, debug = Some({ case `n` => s"[${PrettyPrinter(tipe(n))}] " })))
    })(tree.root)

}
