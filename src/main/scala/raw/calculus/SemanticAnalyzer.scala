package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution
import org.kiama.util.Environments

/** Analyzes the semantics of an AST.
  * This includes the type checker and monoid composition.
  *
  * The semantic analyzer reads the user types and the catalog of user-defined class entities from the World object.
  * User types are the type definitions available to the user.
  * Class entities are the data sources available to the user.
  *   e.g., in expression `e <- Events` the class entity is `Events`.
  */
class SemanticAnalyzer(tree: Calculus.Calculus, world: World) extends Environments with Attribution with LazyLogging {

  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import org.kiama.rewriting.Rewriter._
  import org.kiama.util.{Entity, MultipleEntity, UnknownEntity}
  import org.kiama.util.Messaging.{check, collectmessages, Messages, message, noMessages}
  import Calculus._
//  import SymbolTable._

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
          // TODO: FIX expectedType pretty printer!!!
          message(e, s"expected ${expectedType(e)} got ${tipe(e)}",
            !Types.compatible(expectedType(e), tipe(e))) ++
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
                        message(g, s"expected collection but got $t")
                    }
                  }
                  case _         => noMessages
                }.toIndexedSeq
            }
      }
    }

  /** Looks up identifier in the World catalog. If it is in catalog returns a new `ClassEntity` instance.
    * Note that when looking up a given identifier, a new `ClassEntity` instance is generated each time, to ensure that
    * reference equality comparisons work later on.
    */
  def lookupCatalog(idn: String): Entity =
    if (world.catalog.contains(idn))
      freshVar(world.catalog(idn).tipe)
    else
      UnknownEntity()

//  def entityFromDecl(n: IdnDef): Entity =
//    n match {
//      case tree.parent(p) => {
//        p match {
//          case Bind(_, e)   =>
//
//
//            BindEntity(e)
//          case Gen(_, e)    => GenEntity(e)
//          case FunAbs(_, _) => FunAbsEntity(t) i can know if from idn! so shouldnt this return the ... idndef type? or should i just unify it here already?
//        }
//      }
//    }

  lazy val entity: IdnNode => Entity = attr {
    case n @ IdnDef(idn) =>
      if (isDefinedInScope(env.in(n), idn))
        MultipleEntity()
      else
        entityFromDecl(n)
    case n @ IdnUse(idn) =>
      lookup(env.in(n), idn, lookupCatalog(idn))
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
    case c: Comp => enter(in(c))

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
    // The `out` environment of a comprehension must remove the scope that was inserted.
    case c: Comp => leave(out(c))

    // The `out` environment of a function abstraction must remove the scope that was inserted.
    case f: FunAbs => leave(out(f))

    // A new variable was defined in the current scope.
    case n @ IdnDef(i) => define(out(n), i, entity(n))

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
      case RecordProj(_, idn) => RecordType(List(AttrType(idn, TypeVariable())))

      case IfThenElse(e1, _, _) if e eq e1 => BoolType()
      case IfThenElse(_, e2, e3) if e eq e3 => tipe(e2)

      case BinaryExp(_: ArithmeticOperator, e1, _) if e eq e1 => TypeVariable(Set(IntType(), FloatType()))

      // Right-hand side of any binary expression must have the same type as the left-hand side
      case BinaryExp(_, e1, e2) if e eq e2 => tipe(e1)

      // Function application on a non-function type
      case FunApp(f, _) if e eq f => FunType(TypeVariable(), TypeVariable())

      // Mismatch in function application
      case FunApp(f, e1) if e eq e1 => tipe(f) match {
        case FunType(t1, _) => t1
        case _              => TypeVariable()
      }

      case MergeMonoid(_: NumberMonoid, e1, _) if e eq e1 => TypeVariable(Set(IntType(), FloatType()))
      case MergeMonoid(_: BoolMonoid, e1, _) if e eq e1   => BoolType()

      // Merge of collections must be with same monoid collection types
      case MergeMonoid(_: BagMonoid, e1, _) if e eq e1  => BagType(TypeVariable())
      case MergeMonoid(_: ListMonoid, e1, _) if e eq e1 => ListType(TypeVariable())
      case MergeMonoid(_: SetMonoid, e1, _) if e eq e1  => SetType(TypeVariable())

      // Right-hand side of any merge must have the same type as the left-hand side
      case MergeMonoid(_, e1, e2) if e eq e2 => tipe(e1)

      // Comprehension with a primitive monoid must have compatible projection type
      case Comp(_: NumberMonoid, _, e1) if e eq e1 => TypeVariable(Set(IntType(), FloatType()))
      case Comp(_: BoolMonoid, _, e1) if e eq e1   => BoolType()

      // Qualifiers that are expressions (i.e. where there is an `expectedType`) must be predicates
      case Comp(_, qs, _) if qs.exists { case q => q eq e} => BoolType()

      case UnaryExp(_: Neg, _)      => TypeVariable(Set(IntType(), FloatType()))
      case UnaryExp(_: Not, _)      => BoolType()
      case UnaryExp(_: ToBool, _)   => TypeVariable(Set(IntType(), FloatType()))
      case UnaryExp(_: ToInt, _)    => TypeVariable(Set(BoolType(), FloatType()))
      case UnaryExp(_: ToFloat, _)  => TypeVariable(Set(BoolType(), IntType()))
      case UnaryExp(_: ToString, _) => TypeVariable(Set(BoolType(), IntType(), FloatType()))

      case _ => TypeVariable()
    }
    case _                      => TypeVariable() // There is no parent, i.e. the root node.
  }

  /** Phase 1 Typer.
    * TODO: :)
    *
    * The de-referenced type of an expression.
    *
    * Given a type defined as:
    * RecordType(List(
    * AttrType("foo", IntType()),
    * AttrType("bar", ClassType("baz"))))
    *
    * The `realTipe` attribute for bar is the user-defined type ClassType("baz"). The code, however, wants to compare
    * with the type that "baz" points to. This attribute transparently de-references user-defined ClassTypes to their
    * actual type,  by looking up the type definition in the `userTypes` catalog.
    *
    */
    // TODO: This now belongs in the unification algorithm!
//  lazy val pass1: Exp => Type = dynAttr {
//    case e => pass1Internal(e) match {
//      case ClassType(name) => world.userTypes(name)
//      case t               => t
//    }
//  }

  // The method below will do simple checking.
  // It will unify and create a new type.
  // It will NOT infer identifier types.
  // But it will flag all types of errors.
  // Then, on another method, we can do the inference, which needs to propagate that type up to the identifier.
  // After we change the identifier, all types should just transparently "flow down".

  // What about we build the constraint "formula" equation?

  // But then will need to use that to "fix" the identifiers....
//
//  the type variable holds a pointer to the defining instance???

  // if i did the constraint version
  // i would go "up" and build constraints
  // if there are errors, they'd already be flagged (even though the error message wouldn't be pretty: "expected bool got int/float")
  // i could flag an error constraint but actually not do anything just yet.
  // when i reach the root node,
  // then i propagate down.
  // and this is again recursively.
  // but the kiama way with attributes achieves the same and is somewhat more elegant.
  // since there is memoization, it even works just as well.
  // no it doesnt.
  // because i have to reset every child
  // and depends what i've inferred so far. ah yep.
  // so either i do simple recursive function...
  // or... well, what is the kiama adv here?
  // none me thinks

  // how about: infer constraints all the way up. mark errors as well.
  // or even simpler: infer types all the way up. mark errors as error type.
  // when we get to the root, prune..
//
//  pass1 can be as is, either defining types or contraints. It can be memoized because it is consistent: it only depends on the children.
//  we then call it from the root
//  and then we must propagate those types down
//  from the root
//    at this point we can actually memoize them
//
//  no no no.. even pass1 has to be recalled
//
//  the kiama way of writing WOULD work
//  if pass2 was made to always depend on the parent
//  basically, pass2 could be made to EXPLICITELY GET THE TYPE OF THE PARENT
//  INSTEAD OF USING tree.parent.pair
//  sure; even makes sense.
//    get parent (pass2) type. now let's reset ours.
//    the root of pass2 type is the ONLY place where we call pass1.
// but there is a big overhead in stack calls doing this stuff...
//

  //
  //      so, when a type variable unifies with a type, i call the method again with the type being pointed to by the entity!
  //  if there is type variable only on side e3, i must swap it for side e2
  //          I DONT THINK THIS WORKS.. I MUST RETURN THE NEW UNIFIED TYPE ACTUALLY
  //
  //      the only issue is unification has to happen "here"
  //      so move that code here
  //
  //
  //
  //what i thought late at night is that this must be called on the root node
  //      or the intermediate nodes will only be partially inited
  //      actually no.
  //      the mutable stuff on the entities - the constraints - will be partly set.
  //      the intermediate stuff here, well, that is correct.
  // i need to run it


  var variableMap = scala.collection.mutable.Map[Variable, Type]()

  // Run for its side-effect: types the nodes and builds the variableMap
  pass1(tree.root)

  def tipe(e: Exp) = {
    def walk(t: Type): Type = t match {
      case TypeVariable(v) => walk(variableMap(v))
      case _ => t
    }
    walk(pass1(e))
  }

  def idnType(idn: IdnNode) = entity(idn) match {
    case v: Variable => TypeVariable(v)
    case _           => NothingType()
  }

  lazy val pass1: Exp => Type = attr {

    // Rule 1
    case _: BoolConst   => BoolType()
    case _: IntConst    => IntType()
    case _: FloatConst  => FloatType()
    case _: StringConst => StringType()

    // Rule 2
    case _: Null => val v = freshVar(AnyType()); TypeVariable(v)

    // Rule 3
    case IdnExp(idn) => idnType(idn)

    // Rule 4
    case RecordProj(e, idn) => pass1(e) match {
      case RecordType(atts) => atts.find(_.idn == idn) match {
        case Some(att: AttrType) => att.tipe
        case _                   => NothingType()
      }
      case _                => NothingType()
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
      case _ => NothingType()
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
    case MergeMonoid(_: PrimitiveMonoid, e1, e2) => unify(pass1(e1), pass1(e2))

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
    case BinaryExp(_: ComparisonOperator, e1, e2) =>
      unify(pass1(e1), pass1(e2))
      BoolType()

    case BinaryExp(_: EqualityOperator, e1, e2) =>
      unify(pass1(e1), pass1(e2))
      BoolType()

    case BinaryExp(_: ArithmeticOperator, e1, e2) =>
      unify(pass1(e1), pass1(e2))
//
//      unify(v, e1)
//
//      variableMap.update(e1, unify( v))
//
//
//      add constraint to e1 that it must be int float; add constraint to e2 that it must be int float
//      val t1 = pass1(e1)
//      val t2 = pass1(e2)
//      t1 match { case TypeVariable(v) => variableMap.update(v, tE)}
////      val v = freshVar(unify(pass1(e1), pass1(e2))); variableMap.update()
////
////      Types.intersect(pass1(e1), pass1(e2))

    // Unary Expression type
    case UnaryExp(_: Not, _)      => BoolType()
    case UnaryExp(_: Neg, e)      => pass1(e)
    case UnaryExp(_: ToBool, _)   => BoolType()
    case UnaryExp(_: ToInt, _)    => IntType()
    case UnaryExp(_: ToFloat, _)  => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case _ => NothingType()
  }



//    def entityType(e: Entity): Type = e match {
//      case BindEntity(e)     => tipe(e)
//      case GenEntity(e)      => tipe(e) match {
//        case t: CollectionType => t.innerType
//        case _ => ErrorType()
//      }
//      case FunAbsEntity(t)   => t
//      case ClassEntity(_, t) => t
//    }


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

//
//
//  shouldnt the identifier itself be an entity?
//
//
//  def idnType(idn: IdnNode): Type = entityType(entity(idn))
//
//  /** Phase 2 typer.
//    *
//    */
//  lazy val pass2: Exp => Type = dynAttr {
//    case tree.parent.pair(e: Exp, p) => p match {
//
//      // TODO: This is wrong!!!
//       // what we must do is that when we infer the type of 'v' (of an identifier) to be something,
//       //we must intersect that type with the type of the identity, and change the identity.
//      case Bind(idn, _) => val t = Types.intersect(idnNodeType(idn), pass1(e)); entity(idn) match {
//        case b: BindVar => b.t = t
//      }; t
//
//      case Gen(idn, _) => val t = pass1(e) match {
//        case t: SetType  => Types.intersect(SetType(idnNodeType(idn)), t)
//        case t: BagType  => Types.intersect(BagType(idnNodeType(idn)), t)
//        case t: ListType => Types.intersect(ListType(idnNodeType(idn)), t)
//      }; entity(idn) match {
//        case g: GenVar => g.t = t match {
//          case c: CollectionType => c.innerType
//        }
//      }; t
//
//        // so on pass two, intersect the IdnExp that we inferred with the type on the entity.
//        // but we do that.
//        // but then we must go back and change the body of the type being BOUND TO.
//
////      case Bind(idn, _) => entity(idn) match {
////        case BindVar(e1) => Types.intersect(pass2(e1), pass1(e))
////      }
//
//
//      case r @ RecordProj(_, idn) =>
//        // The parent node `RecordProj` is no longer of `RecordType`. To reconstruct the `RecordType` with the more
//        // precise type given by the parent for a given `idn`, we use the `intersect` method, which, for record types,
//        // always creates a new record type with all identifiers and the more precise type in each one.
//        // For example:
//        //  Given the parent node `p` being a record projection on `idn1` of type `StringType`,
//        //  and the current node `e` of type `RecordType(List(AttrType(idn1, TypeVariable()), AttrType(idn2, IntType()))),
//        //  then the current node new type should be:
//        //    `RecordType(List(AttrType(idn1, StringType()), AttrType(idn2, IntType())))
//        //  that is, `idn1` becomes more precise but `idn2`, which is unknown to the parent, remains unchanged.
//        Types.intersect(RecordType(List(AttrType(idn, pass2(r)))), pass1(e))
//
//      // Skip `AttrCons`, which is in between `e` and `RecordCons`.
//      case tree.parent(r @ RecordCons(atts)) =>
//        // Find the identifier for `e` in the parent `RecordCons`.
//        val myIdn = atts.collectFirst{ case AttrCons(idn, e1) if e eq e1 => idn }.head
//        // Get more precise type for `e` from the parent `RecordCons`.
//        pass2(r) match {
//          case RecordType(tipes) => tipes.find(_.idn == myIdn) match {
//            case Some(att: AttrType) => att.tipe
//            case _                   => TypeVariable()
//          }
//          case _                => TypeVariable()
//        }
//
//      case i @ IfThenElse(_, e2, e3) if e eq e2 => pass2(i)
//      case i @ IfThenElse(_, e2, e3) if e eq e3 => pass2(i)
//
//      // For some binary operators such as comparison or equality, the parent type is always `BoolType`, so we intersect
//      // both `e1` and `e2` types to ensure we obtain the most precise type (which are not necessarily a `BoolType`).
//      // For arithmetic binary operators, we take the parent operator result type.
//      case BinaryExp(_: ComparisonOperator, e1, e2) if e eq e1 => Types.intersect(pass1(e1), pass1(e2))
//      case BinaryExp(_: ComparisonOperator, e1, e2) if e eq e2 => pass2(e1)
//      case b @ BinaryExp(_: ArithmeticOperator, _, _) => pass2(b)
//
//      case f @ FunAbs(_, _, e1) if e eq e1 => pass2(f) match {
//        case FunType(_, t2) => t2
//      }
//
//      case f @ FunApp(f1, e1) if e eq f1 => pass1(f1) match {
//        case FunType(t, _) => FunType(Types.intersect(t, pass1(e1)), pass2(f))
//        case t => t
//      }
//      case FunApp(f1, e1) if e eq e1 => pass1(f1) match {
//        case FunType(t, _) => Types.intersect(t, pass1(e1))
//        case t => t
//      }
//
//      case c: ConsCollectionMonoid => pass2(c) match {
//        case t: CollectionType => t.innerType
//      }
//
//      case m: MergeMonoid => pass2(m)
//
//      case c @ Comp(_: PrimitiveMonoid, _, e1) if e eq e1 => pass2(c)
//      case c @ Comp(_: CollectionMonoid, _, e1) if e eq e1 => pass2(c) match {
//        case t: CollectionType => t.innerType
//      }
//
//      case u @ UnaryExp(_: Neg, _) => pass2(u)
//
//      case _ => pass1(e)
//    }
//    case e => pass1(e)
//  }
//
//  def tipe(e: Exp): Type = {
//    // get all idn exps
//    var idns = scala.collection.mutable.Set[IdnExp]()
//    everywhere(query[IdnExp] { case idn: IdnExp => idns += idn})(tree.root)
//    //println("idns are " + idns)
//
//    // split them into groups based on entity
//    //val idnsByEntity: Map[Entity, scala.collection.mutable.Set[IdnExp]] = idns.groupBy{ case IdnExp(idn) => entity(idn) }
//    val idnsByEntity = idns
//      .groupBy{ case IdnExp(idn) => entity(idn) }
//    //println("idnsByEntity are " + idnsByEntity)
//
//    // for each, run phase 2
//    var updated = false
//    for ( (entity, idns) <- idnsByEntity) {
//      val typedIdns = idns.map(pass2)
//      val finalType = typedIdns.tail.foldLeft(typedIdns.head)(Types.intersect)
//      //println("Entity is " + entity + " and finalType is " + finalType)
//      entity match {
//        case f: FunArg => if (f.t != finalType) { f.t = finalType; updated = true }//println("Here1 " + f.t + " " + finalType);
//        case c: ClassEntity => if (c.t != finalType) { c.t = finalType; updated = true } // println("Here2 " + c.t + " " + finalType)
//        case b: BindVar => if (b.t != finalType) { b.t = finalType; updated = true }
//        case g: GenVar => if (g.t != finalType) { g.t = finalType; updated = true }
//      }
//    }
//
//
//    //
//
//    if (!updated) {
//      pass2(e)
//    } else {
//      pass1.reset() // rename to bottom  up typer
//      pass2.reset() // rename to top down typer
//      tipe(e)
//    }
//
//    // TODO: FunAbs + FunApp
//
//    // check if any entity changed
//
//    // then unify them all
//
//    // check if anything changed anywhere
//    // if it did, fix associated entity and recurse.
//
//    // if nothing changed,
//    // return the phase 2 type of 'e'
//  }

  def debugTreeTypes =
    everywherebu(query[Exp] {
      case n => logger.error(CalculusPrettyPrinter(tree.root, debug = Some({ case `n` => s"[${PrettyPrinter(tipe(n))}] " })))
    })(tree.root)

}

//idnexp would not be typed in phase1. they'd be unknown.
//in phase 3 (after 2) we go through every usage of an identifier - class entities to bind vars, etc, and figure out if the
//inferred type matches the user type (for class entities and fun args) or the body type (for binds and gens).
//we then intersect the whole thing to niche the class entities, fun args, body types, etc.
//
//
//phase3 could unify all the usages, update their respectives types if consistent / possible, then go back to phase 1
//if smtg changed.
//
//
//
//every idn def could be typed
//that means bind, generator, function argument
//
//the idndef node them would have an optional type argument
//
