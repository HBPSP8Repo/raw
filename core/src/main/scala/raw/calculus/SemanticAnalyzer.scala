package raw
package calculus

import com.typesafe.scalalogging.LazyLogging
import org.kiama.attribution.Attribution
import org.kiama.rewriting.Strategy
import raw.World._

import scala.collection.GenTraversableOnce
import scala.util.parsing.input.Position

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
class SemanticAnalyzer(val tree: Calculus.Calculus, world: World, val queryString: Option[String] = None) extends Attribution with LazyLogging {

  import scala.collection.immutable.Seq
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
      case _: SetType  =>
        if (!m.commutative && !m.idempotent)
          Some(CommutativeIdempotentRequired(t))
        else if (!m.commutative)
          Some(CommutativeRequired(t))
        else if (!m.idempotent)
          Some(IdempotentRequired(t))
        else
          None
      case _: BagType  =>
        if (!m.commutative)
          Some(CommutativeRequired(t))
        else
          None
      case _: ListType =>
        None
      case _: ConstraintCollectionType =>
        None
      case UserType(sym)   =>
        errors(world.tipes(sym))
      case _: TypeVariable =>
        None
      case _               =>
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
      logger.debug(s"Called with idn $idn at pos ${n.pos}")
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

  /** Return a set with all type variables within a type.
    */
  private def getVariableTypes(t: Type): Set[VariableType] = t match {
    case _: NothingType                                    => Set()
    case _: AnyType                                        => Set()
    case _: BoolType                                       => Set()
    case _: IntType                                        => Set()
    case _: FloatType                                      => Set()
    case _: StringType                                     => Set()
    case _: UserType                                       => Set()
    case RecordType(atts, _)                               => atts.flatMap { case att => getVariableTypes(att.tipe) }.toSet
    case ListType(innerType)                               => getVariableTypes(innerType)
    case SetType(innerType)                                => getVariableTypes(innerType)
    case BagType(innerType)                                => getVariableTypes(innerType)
    case FunType(p, e)                                     => getVariableTypes(p) ++ getVariableTypes(e)
    case t1: PrimitiveType                                 => Set(t1)
    case t1: NumberType                                    => Set(t1)
    case t1 @ ConstraintRecordType(atts, _)                => Set(t1) ++ atts.flatMap { case att => getVariableTypes(att.tipe) }
    case t1 @ ConstraintCollectionType(innerType, _, _, _) => Set(t1) ++ getVariableTypes(innerType)
    case t1: TypeVariable                                  => Set(t1)
  }

  private lazy val entityType: Entity => Type = attr {
    case VariableEntity(idn, t) => {

      logger.debug(s"Here in entity $idn")

      // Go up until we find the Bind
      def getDef(n: RawNode): Option[RawNode] = n match {
        case b: Bind                          => Some(b)
        case g: Gen                           => Some(g)
        case f: FunAbs                        => Some(f)
        case tree.parent.pair(_: Pattern, n1) => getDef(n1)
      }

      // TODO: Here, if them unresolved TypeVars come from UserType/Source, don't include them as free vars!!

      val df = getDef(tree.parent(idn).head)
      df match {
        case Some(b: Bind)      =>

          logger.debug(s"Here in idn $idn")
          logger.debug(s"Mappings $mappings")
          logger.debug(s"Bind $b")
          logger.debug(s"t $t")

          val t1 = walk(t)
          logger.debug(s"t1 $t1")
          val freeVars = getVariableTypes(t1).filter { case vt => !mappings.contains(vt )}.map(_.sym)
          logger.debug(s"freeVars $freeVars")
          TypeScheme(t1, freeVars)

          //i think i am confusing free vars with free type variables actually
          // x1 is NOT a free var
          // but when i instantiate it,
          // i must STILL CREATE A COPY, right?
          // it HAS the same type
          // but it is a copy
          // well, i think that is what i achieved.
          // but then unification occurs across f1 and f2 in the if then else
          // but that is OK
          // SO WAHAT BREAKS?
          // maybe an issue of where i do things?
          // a bind to a funabs vs returning the funbas?
          // i am confused dam n it

//          val freeVars = getTypeVariables(t).filter { case vt => !mappings.contains(vt )}.map(_.sym)
//          logger.debug(s"freeVars $freeVars")
//          TypeScheme(t, freeVars)

//
//          find all inner idn defs
//          for each collect its root type
//
//
//        collect all idn uses
//          then collect their entities
//
//        for each idn use
//          look it up in the current environment
//            if found, check that entity is the same
//              if so, that idn is not free
//
//
//          no.. check that they are in the same group!!!
//
//
//          if i ask the types of the inner things, i'll go into a loop
//
//          remember that i am a particular identifier
//
//          i can find my 'root' since i'm actually typed already? no. i'm just a type var.
//          but i am already part of a group.
//          so i just need to know if
//
//
//          then, for all other idndefs,
//
////
//
//          i basically need to know if i am bound to smtg from the outside
//          but must i walk myself?
//
//          what is the case where i am an identifier with part that is from the outside, part that it is not?
//          if i am a function... whose arg1 is outside, arg2 is from the inside
//      y := false
//      \x, z -> if (x) then z + 1 else z - 1
//
//          so
//
//      The bottom line is, generalizing type T to a type scheme should only abstract type variables that don't appear in the current environment.
//
//


//          val origMappings = mappings
//          logger.debug(s"post-mapping
//
//
//
// s $mappings")
//
          // walk new type
          // find all its new variables
          // remove the ones that are ..

//          val t1 = tipe(b.e) // walk(t, mappings)

////          solution(b)
//          val t1 = walk(t)
//
//          logger.debug(s"walked type $t1")
//
//          val x = collect[List, TypeVariable] {
//            case idn: IdnDef if entity(idn).isInstanceOf[VariableEntity] => entity(idn).asInstanceOf[VariableEntity].t.asInstanceOf[TypeVariable]
//          }
//
//          logger.debug(s"Bind is $b")
//          logger.debug(s"idn is $idn")
//
//          // Collect the type (variable) of each identifier defined in the body of the Bind.
//          // These are the "candidates" for polymorphism; we check below whether they have been bound with outer scope variables,
//          // i.e. if they are no longer free.
//          val rhsIdnDefTypes = x(b.e)
//          logger.debug(s"rhsIdnDefTypes $rhsIdnDefTypes")
//
//          // Obtain their symbols
//          val rhsIdnDefSyms = rhsIdnDefTypes.map(_.sym).toSet
//          logger.debug(s"rhsIdnDefSyms $rhsIdnDefSyms")
//
//          logger.debug(s"f1 ${isDefinedInEnv(env(b.e), "f1")}")
//          logger.debug(s"f2 ${isDefinedInEnv(env(b.e), "f2")}")
//          logger.debug(s"z  ${isDefinedInEnv(env(b.e), "z")}")
//          logger.debug(s"x  ${isDefinedInEnv(env(b.e), "x")}")
//
//
//
//
//          // For each candidate for polymorphism,
//
//          //    walk it to their bottom type
//          //    for each variable type left,
//
//
//          //   walk it to their bottom type
//          //   then check if any of the variable types left within its bottom type is in the same group as some other variable defined in the outer scope,
//          //   i.e. whether there is at least one variable in the same group that does not come from the symbols in rhsIdnDefSyms.
//          //   If that is the case, then this is actually NOT a free variable.
//          val freeVars = scala.collection.mutable.MutableList[Symbol]()
//          for (t <- rhsIdnDefTypes) {
//            val nt = walk(t)
//            val tvs = getVariableTypes(nt)
//            for (tv <- tvs) {
//              var free = true
//              if (mappings.contains(tv)) {
//                val others = mappings(tv).tipes
//                for (o <- others) {
//                  if (!getVariableTypes(walk(o)).map(_.sym).subsetOf(rhsIdnDefSyms)) {
//                    free = false
//                  }
//                }
//              }
//              if (free) {
//                freeVars += tv.sym
//              }
//            }
//          }
//          logger.debug(s"freeVars are $freeVars")

//          what is free?
//
//
//          // Walk those type variables to their bottom types.
//          //
//
//          no no no
//
//          cannot walk
//
//          i had it right
//
//          i must associate with the original group
//          stuff...
//          if it is in the same group???
//
//          // Walk those type variables to their bottom types
//          val rhsIdnDefWalkedTypes = rhsIdnDefTypes.map(walk)
//          logger.debug(s"rhsIdnDefWalkedTypes $rhsIdnDefWalkedTypes")
//
//          // Filter out those that remain variable types (the rest has been fully resolved and cannot be polymorphic)
//          val rhsIdnDefsPolymorphicTypes = rhsIdnDefWalkedTypes.collect { case vt: VariableType => vt }
//          logger.debug(s"rhsIdnDefsPolymorphicTypes $rhsIdnDefsPolymorphicTypes")
//
//          // TODO: Fix walk to NOT take mappings as a paremeter

          // Walk each type variable to its "bottom type", i.e. resolve it must as much possible.
//          val resolvedRhsTypes = rhsTypesOfIdnDefs.map(walk)
//          logger.debug(s"resolvedRhsTypes $resolvedRhsTypes")
//
//
////          now, each of these types can be arbitraly complex: if it refers to one of the original type variables, then it remains unresolved
////
////          no;;; it is not ONE OF THE ORIGINAL type variables
////          if it REFERS TO things
//
//
//          val candidates = resolvedRhsTypes.flatMap(getVariableTypes).map(_.sym).toSet
//          logger.debug(s"candidates $candidates")
//
//          val rhsIdnDefsPolymorphicSyms = rhsIdnDefsPolymorphicTypes.map(_.sym).toSet
//          logger.debug(s"rhsIdnDefsPolymorphicSyms $rhsIdnDefsPolymorphicSyms")
//
//          val freeVars = scala.collection.mutable.MutableList[Symbol]()
//
//          for (t <- rhsIdnDefsPolymorphicTypes) {
//            logger.debug(s"Processing type $t")
//            val tvs = getVariableTypes(t)
//            val syms = tvs.map(_.sym)
//            logger.debug(s"... its syms are $syms")
//
//            if (syms.subsetOf(rhsIdnDefsPolymorphicSyms)) {
//              // For the given type, the symbols we use inside, after we resolve them fully,
//              // are a subset of the symbols of the FunAbs parameters.
//              // This means this type is really free
//              freeVars += t.sym
//            }
//          }
//          logger.debug(s"freeVars are $freeVars")
//          // collect all identifier types defined on the rhs; walk them to their "bottom type". Get their variable types and extract the symbols in them



          // now let's intersect those two:
          // the intersection of the two is only things defined inside (i.e. even after their bottom resolution, they still refer to identifiers defined in the rhs)
//          val freeVars = rhsTypesOfIdnDefs.map(_.sym).toSet.intersect(candidates)
///**///          logger.debug(s"freeVars are $freeVars")

//
//
//          so i have the type scheme
//          that can include ALL vars that are needed
//            basically.
//
//              is x a free variable inside?
//          is x1 a free variable inside?
//          well, NO. it MUST BE the same type as 'x'
//      it MUST BE the same type as 'x'
//      so, no, it is NOT free.
//      that is the problem.
//
//      when i walk it initially,
//i get x1
//          then i walk
//          it
//          i should get to x
//          x is ALREADY in the environment, right?
//          yes.
//          so for sure it is NOT free.
//
//
//          so, symbols already in the environment - already known - are not free
//          but the environment is totally full
//          so how do i know that?
//
//          type variables that are defined "before" me ?
//
//          so basically, after i walk
//          the IDs that are NOT parameters inside?
//
//
//
//
//          this still gets tricky
//          when i walk the type and get its set of type variableas
//          only some can be replicated when recostructing the type
//
//
//          can a constraint record type be the same w/ different type vars inside?
//          keep the same symbol but have different inner vars?
//          no. it is not the same type.
//
//          are only type vars affected?
//          dont think so

        // replace walk by a thing that gets ride of vars that point to each other.
          // again, cant just be a find because we need to recurse inside the definition


          // get all type variables
          // for each type variable
          //    get all other type variables in the group, in the new mapping
          //    if any of those was contained in the original group, skip it altogether
          //
          // REMEMBER TO REPLACE THE NEW TYPE BY THE OLD TYPE SO THAT THE OLD TYPE KEPPS ON BEING CHANGED

          // btw, also skip yourself


          //

          // for each type variable,
          // that i cannot walk further - i.e. no in the map -
          // check all other member of the map
          // if any of those other is in the map, we replace it by the original one in the map (old root)

          // now we end up only with either totally new things, or things that were in the map

//          do test case with arg of funabs being a project attribute (constrained record) with polymorphism

//          def reconstructType(t: Type, occursCheck: Set[Type]): Type = {
//            if (occursCheck.contains(t)) {
//              t
//            } else {
//              t match {
//                case _: NothingType   => t
//                case _: AnyType       => t
//                case _: IntType       => t
//                case _: BoolType      => t
//                case _: FloatType     => t
//                case _: StringType    => t
//                case _: PrimitiveType => if (!mappings.contains(t)) t else reconstructType(mappings(t).root, occursCheck + t)
//                case _: NumberType    => if (!mappings.contains(t)) t else reconstructType(mappings(t).root, occursCheck + t)
//                case _: UserType      => t
//                case RecordType(atts, name) => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, name)
//                case ListType(innerType) => ListType(reconstructType(innerType, occursCheck + t))
//                case SetType(innerType)  => SetType(reconstructType(innerType, occursCheck + t))
//                case BagType(innerType)  => BagType(reconstructType(innerType, occursCheck + t))
//                case FunType(p, e)       => FunType(reconstructType(p, occursCheck + t), reconstructType(e, occursCheck + t))
//                case ConstraintRecordType(atts, sym) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, sym)
//                case ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(reconstructType(innerType, occursCheck + t), c, i, sym)
//                case t1: TypeVariable => if (!mappings.contains(t1)) t1 else reconstructType(mappings(t1).root, occursCheck + t)
//              }
//            }
//          }
//
//




          //

//          TypeScheme(t1, getVariableTypes(t1).filter { case tv: TypeVariable => !origMappings.contains(tv) && (tv != t1) case _ => true }.map(_.sym))
//          TypeScheme(t1, freeVars.to)
//          t
        case Some(g: Gen)       =>
//          val origMappings = mappings
//          solution(g)
//          val t1 = walk(t)
//          TypeScheme(t1, getVariableTypes(t1).filter { case tv: TypeVariable => !origMappings.contains(tv) && (tv != t1) case _ => true }.map(_.sym))
        t
        case Some(f: FunAbs)                  =>
          logger.debug(s"Passed here with idn $idn")
          t
      }
    }
    case DataSourceEntity(sym) => world.sources(sym.idn)
    case _: UnknownEntity      => NothingType()
    case _: MultipleEntity     => NothingType()
  }

  private def instantiateTypeScheme(t: Type, polymorphic: Set[Symbol]) = {
    val mappings = scala.collection.mutable.HashMap[Symbol, Symbol]()

    def getSym(sym: Symbol): Symbol = {
      if (!mappings.contains(sym))
        mappings += (sym -> SymbolTable.next())
      mappings(sym)
    }

    def recurse(t: Type): Type = {
      t match {
        case TypeVariable(sym) if polymorphic.contains(sym) => TypeVariable(getSym(sym))
        case t1: TypeVariable                               => t1
        case NumberType(sym)                                => NumberType(getSym(sym))
        case PrimitiveType(sym)                             => PrimitiveType(getSym(sym))
        case ConstraintRecordType(atts, sym)                => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, recurse(t1)) }, getSym(sym))
        case ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(recurse(innerType), c, i, getSym(sym))
        case _: NothingType                                 => t
        case _: AnyType                                     => t
        case _: IntType                                     => t
        case _: BoolType                                    => t
        case _: FloatType                                   => t
        case _: StringType                                  => t
        case _: UserType                                    => t
        case RecordType(atts, name)                         => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, recurse(t1)) }, name)
        case BagType(inner)                                 => BagType(recurse(inner))
        case SetType(inner)                                 => SetType(recurse(inner))
        case ListType(inner)                                => ListType(recurse(inner))
        case FunType(p, e)                                  => FunType(recurse(p), recurse(e))
      }
    }

    recurse(t)
  }

  private def idnType(idn: IdnNode): Type = {
    logger.debug(s"Calling idnType with ${idn.idn}"); entityType(entity(idn))
  }

  /** The type corresponding to a given pattern.
    */
  private def patternType(p: Pattern): Type = p match {
    case PatternIdn(idn) =>
      entity(idn) match {
        case VariableEntity(_, t) => t
        case _ => typeWithPos(NothingType(), p)
      }
    case PatternProd(ps) =>
      typeWithPos(RecordType(ps.zipWithIndex.map { case (p1, idx) => AttrType(s"_${idx + 1}", patternType(p1)) }, None), p)
  }

  private def typeWithPos(t: Type, n: RawNode): Type = {
    t.pos = n.pos
    t
  }

  /** The type of an expression.
    * If the type cannot be immediately derived from the expression itself, then type variables are used for unification.
    */

  // TODO: DO we even need the position of types? If when I report errors I use the expression, this is no longer needed!!!

  private lazy val expType: Exp => Type = attr {
    // Place the type position in the operator
    case e @ BinaryExp(op, _, _) => typeWithPos(expType1(e), op)
    case e @ UnaryExp(op, _) => typeWithPos(expType1(e), op)

    case e => typeWithPos(expType1(e), e)
  }

  private def expType1(e: Exp): Type = e match {

    // Rule 1
    case _: BoolConst  => BoolType()
    case _: IntConst   => IntType()
    case _: FloatConst => FloatType()
    case _: StringConst => StringType()

    // Rule 3
    case IdnExp(idn) =>
      logger.debug(s"We are in IdnExp of ${idn.idn} at ${idn.pos}")
      idnType(idn) match {
        case TypeScheme(t, vars) =>
          if (vars.isEmpty)
            t
          else {
            val nt = instantiateTypeScheme(t, vars)
            logger.debug(s"This is a typescheme ${TypesPrettyPrinter(t)} with vars $vars")
            logger.debug(s"And the instance is ${TypesPrettyPrinter(nt)}")
            nt
          }
        case t                   => logger.debug(s"and here with t.pos ${t.pos}"); t
      }

    // Rule 5
    case RecordCons(atts) => RecordType(atts.map(att => AttrType(att.idn, expType(att.e))), None)

    // Rule 7
//    case FunAbs(p, e1) => FunType(patternType(p), expType(e1))

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
    case UnaryExp(_: Not, _)     => BoolType()
    case UnaryExp(_: ToBool, _)  => BoolType()
    case UnaryExp(_: ToInt, _)   => IntType()
    case UnaryExp(_: ToFloat, _) => FloatType()
    case UnaryExp(_: ToString, _) => StringType()

    case n => TypeVariable()
  }

  /** Hindley-Milner unification algorithm.
    */
  private def unify(t1: Type, t2: Type): Boolean = {

    def recurse(t1: Type, t2: Type, occursCheck: Set[(Type, Type)]): Boolean = {
      logger.debug(s"recurse t1 ${TypesPrettyPrinter(t1)} t2 ${TypesPrettyPrinter(t2)} occursCheck $occursCheck")
      if (occursCheck.contains((t1, t2)))
        return true
      val nt1 = find(t1)
      val nt2 = find(t2)
      (nt1, nt2) match {
        // TODO: Add NothingType

        case (_: AnyType, t) =>
          true
        case (t, _: AnyType) =>
          true

        case (_: IntType, _: IntType)     =>
          true
        case (_: BoolType, _: BoolType)   =>
          true
        case (_: FloatType, _: FloatType) =>
          true
        case (_: StringType, _: StringType) =>
          true

        case (SetType(inner1), SetType(inner2)) =>
          recurse(inner1, inner2, occursCheck + ((t1, t2)))
        case (BagType(inner1), BagType(inner2)) =>
          recurse(inner1, inner2, occursCheck + ((t1, t2)))
        case (ListType(inner1), ListType(inner2)) =>
          recurse(inner1, inner2, occursCheck + ((t1, t2)))

        case (FunType(p1, e1), FunType(p2, e2)) =>
          recurse(p1, p2, occursCheck + ((t1, t2))) && recurse(e1, e2, occursCheck + ((t1, t2)))

        case (RecordType(atts1, name1), RecordType(atts2, name2)) if name1 == name2 && atts1.length == atts2.length && atts1.map(_.idn) == atts2.map(_.idn) =>
          atts1.zip(atts2).map { case (att1, att2) => recurse(att1.tipe, att2.tipe, occursCheck + ((t1, t2))) }.forall(identity)

        case (r1 @ ConstraintRecordType(atts1, _), r2 @ ConstraintRecordType(atts2, _)) =>
          val commonIdns = atts1.map(_.idn).intersect(atts2.map(_.idn))
          for (idn <- commonIdns) {
            val att1 = r1.getType(idn).head
            val att2 = r2.getType(idn).head
            if (!recurse(att1, att2, occursCheck + ((t1, t2)))) {
              return false
            }
          }
          val commonAttrs = commonIdns.map { case idn => AttrType(idn, r1.getType(idn).head) } // Safe to take from the first attribute since they were already unified in the new map
        val nt = ConstraintRecordType(atts1.filter { case att => !commonIdns.contains(att.idn) } ++ atts2.filter { case att => !commonIdns.contains(att.idn) } ++ commonAttrs)
          mappings.union(r1, r2).union(r2, nt)
          true

        case (r1 @ ConstraintRecordType(atts1, _), r2 @ RecordType(atts2, name)) =>
          if (!atts1.map(_.idn).subsetOf(atts2.map(_.idn).toSet)) {
            false
          } else {
            for (att1 <- atts1) {
              if (!recurse(att1.tipe, r2.getType(att1.idn).get, occursCheck + ((t1, t2)))) {
                return false
              }
            }
            mappings.union(r1, r2)
            true
          }

        case (r1: RecordType, r2: ConstraintRecordType) =>
          recurse(r2, r1, occursCheck + ((t1, t2)))

        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2 @ ConstraintCollectionType(inner2, c2, i2, _)) =>
          if (c1.isDefined && c2.isDefined && (c1.get != c2.get)) {
            false
          } else if (i1.isDefined && i2.isDefined && (i1.get != i2.get)) {
            false
          } else {
            if (!recurse(inner1, inner2, occursCheck + ((t1, t2)))) {
              false
            } else {
              val nc = if (c1.isDefined) c1 else c2
              val ni = if (i1.isDefined) i1 else i2
              val nt = ConstraintCollectionType(inner1, nc, ni)
              mappings.union(col1, col2).union(col2, nt)
              true
            }
          }

        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: SetType)  =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && i1.get) || i1.isEmpty)) {
            if (!recurse(inner1, col2.innerType, occursCheck + ((t1, t2)))) {
              false
            } else {
              mappings.union(col1, col2)
              true
            }
          } else {
            false
          }
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: BagType)  =>
          if (((c1.isDefined && c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty)) {
            if (!recurse(inner1, col2.innerType, occursCheck + ((t1, t2)))) {
              false
            } else {
              mappings.union(col1, col2)
              true
            }
          } else {
            false
          }
        case (col1 @ ConstraintCollectionType(inner1, c1, i1, _), col2: ListType) =>
          if (((c1.isDefined && !c1.get) || c1.isEmpty) &&
            ((i1.isDefined && !i1.get) || i1.isEmpty)) {
            if (!recurse(inner1, col2.innerType, occursCheck + ((t1, t2)))) {
              false
            } else {
              mappings.union(col1, col2)
              true
            }
          } else {
            false
          }

        case (col1: CollectionType, col2: ConstraintCollectionType) =>
          recurse(col2, col1, occursCheck + ((t1, t2)))

        case (p1: PrimitiveType, p2: PrimitiveType) =>
          mappings.union(p2, p1)
          true
        case (p1: PrimitiveType, _: BoolType)   =>
          mappings.union(p1, nt2)
          true
        case (p1: PrimitiveType, _: IntType)    =>
          mappings.union(p1, nt2)
          true
        case (p1: PrimitiveType, _: FloatType)  =>
          mappings.union(p1, nt2)
          true
        case (p1: PrimitiveType, _: StringType) =>
          mappings.union(p1, nt2)
          true
        case (_: BoolType, _: PrimitiveType)    =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: IntType, _: PrimitiveType)     =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: FloatType, _: PrimitiveType)   =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: StringType, _: PrimitiveType)  =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))

        case (p1: NumberType, p2: NumberType) =>
          mappings.union(p2, p1)
          true
        case (p1: NumberType, _: FloatType) =>
          mappings.union(p1, nt2)
          true
        case (p1: NumberType, _: IntType)   =>
          mappings.union(p1, nt2)
          true
        case (_: FloatType, _: NumberType)  =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))
        case (_: IntType, _: NumberType)    =>
          recurse(nt2, nt1, occursCheck + ((t1, t2)))

        case (UserType(sym1), UserType(sym2)) if sym1 == sym2 =>
          true
        case (v1: TypeVariable, v2: TypeVariable) =>
          mappings.union(v2, v1)
          true
        case (v1: TypeVariable, v2: VariableType) =>
          mappings.union(v1, v2)
          true
        case (v1: VariableType, v2: TypeVariable) =>
          recurse(v2, v1, occursCheck + ((t1, t2)))
        case (v1: TypeVariable, _)                =>
          mappings.union(v1, nt2)
          true
        case (_, v2: TypeVariable)                =>
          recurse(v2, nt1, occursCheck + ((t1, t2)))
        case _                                    =>
          false
      }
    }
    logger.debug(s"ENTER unify(${PrettyPrinter(t1)}, ${PrettyPrinter(t2)})")
    val r = recurse(t1, t2, Set())
    logger.debug(s"EXIT unify ${PrettyPrinter(t1)}, ${PrettyPrinter(t2)}")
    if (r) {
      logger.debug("RESULT is Right");
      printVarMap(mappings)
    } else {
      logger.debug("RESULT is Left"); printVarMap(mappings)
    }

    r
  }

  /** Return a set with all type variables within a type.
    */
  private def getTypeVariables(t: Type): Set[TypeVariable] = t match {
    case _: NothingType                => Set()
    case _: AnyType                    => Set()
    case _: BoolType                   => Set()
    case _: IntType                    => Set()
    case _: FloatType                  => Set()
    case _: StringType                 => Set()
    case _: PrimitiveType              => Set()
    case _: NumberType                 => Set()
    case _: UserType                   => Set()
    case RecordType(atts, _)           => atts.flatMap { case att => getTypeVariables(att.tipe) }.toSet
    case ListType(innerType)           => getTypeVariables(innerType)
    case SetType(innerType)            => getTypeVariables(innerType)
    case BagType(innerType)            => getTypeVariables(innerType)
    case FunType(p, e)                 => getTypeVariables(p) ++ getTypeVariables(e)
    case ConstraintRecordType(atts, _) => atts.flatMap { case att => getTypeVariables(att.tipe) }
    case ConstraintCollectionType(innerType, _, _, _) => getTypeVariables(innerType)
    case t1: TypeVariable => Set(t1)
  }

  /** Return a set with all type variables in a constraint.
    */
  private def getConstraintTypeVariables(c: Constraint): Set[TypeVariable] = c match {
    case SameType(e1, e2, _) =>
      getTypeVariables(expType(e1)) ++ getTypeVariables(expType(e2))
    case HasType(e, expected, _) =>
      getTypeVariables(expType(e)) ++ getTypeVariables(expected)
  }

  /** Type Checker constraint solver.
    * Takes a constraint and a given map of variable types (e.g. type variables, constraint types), and returns either:
    * - a new map, if a solution to the constraint was found;
    * - or a map of the solution found so far plus a list of error messages, if the constraint could not be resolved.
    */
  private def solve(cs: Seq[Constraint]): Boolean = {

    def solver(c: Constraint): Boolean = c match {
      case SameType(e1, e2, desc)     =>
        val t1 = expType(e1)
        val t2 = expType(e2)
        val r = unify(t1, t2)
        if (!r) {
          unifyErrors += IncompatibleTypes(walk(t1), walk(t2))
        }
        r
      case HasType(e, expected, desc) =>
        val t = expType(e)
        val r = unify(t, expected)
        if (!r) {
          unifyErrors += UnexpectedType(walk(t), walk(expected), desc)
        }
        r
    }

    cs match {
      case c :: rest => if (solver(c)) solve(rest) else false
      case _ => true
    }

  }

  private def solve1(cs: Seq[Constraint]): Boolean = {

    def solver(c: Constraint): Boolean = c match {
      case SameType(e1, e2, desc)     =>
        val t1 = expType(e1)
        val t2 = expType(e2)
        val r = unify(t1, t2)
        if (!r) {
          unifyErrors += IncompatibleTypes(walk(t1), walk(t2))
        }
        r
      case HasType(e, expected, desc) =>
        val t = expType(e)
        val r = unify(t, expected)
        if (!r) {
          unifyErrors += UnexpectedType(walk(t), walk(expected), desc)
        }
        r
    }

    logger.debug(s"Processing constraints: $cs")
    logger.debug("Input map:")
    //logger.debug(m.map { case (v: Symbol, t: Type) => s"${v.idn} => ${PrettyPrinter(t)}" }.mkString("{\n", ",\n", "}"))

    // Sort out the 'cs' first based on heuristics
    // TODO: Handle 1st constraint of basic types (e.g. 1 = 1.1)
    // Group constraints into two groups: those that have type variables and those that do not.
    val g = cs.groupBy(c => getConstraintTypeVariables(c).intersect(mappings.typeVariables).nonEmpty)
    // The next constraint to solve is always a constraint with type variables, if it exists.
    // TODO: Prefer constraints with more type variables?

    val next = if (g.contains(true)) g(true).head else if (g.contains(false)) g(false).head else return true
    logger.debug(s"Picked constraint: $next")
    // Solve that constraint and call ourselves recursively with the rest of the And constraints
    if (!solver(next)) {
      false
    } else {
      val rest = cs.filterNot(c => c == next)
      if (rest.nonEmpty) {
        solve(rest)
      } else {
        true
      }
    }
  }

//  /** Flatten all occurrences on nested And constraints.
//    */
//  // TODO: Refactor? Remove And constraint and turn it into Seq?
//  private def flattenAnds(c: Constraint): Constraint = c match {
//    case And(cs @ _*) => And(cs.map(flattenAnds).flatMap {
//      case And(cs1 @ _*) => cs1
//      case _ => Seq(c)
//    }: _*)
//    case c_           => c
//  }

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

  private val mappings: VarMap = {
    val m: VarMap = new VarMap(query = queryString)
    // Add user types
    for ((sym, t) <- world.tipes) {
      m.union(UserType(sym), t)
    }
    // Add type variables from user sources
    for ((_, t) <- world.sources) {
      for (tv <- getVariableTypes(t)) {
        m.union(tv, tv)
      }
    }
    m
  }

  /** Stores unification errors.
    */
  private var unifyErrors =
    scala.collection.mutable.MutableList[Error]()

  /** Run for its side-effect.
    */
  solution(tree.root)

  // TODO: Add check that the *root* type (and only the root type) does not contain ANY type variables, or we can't generate code for it
  // TODO: And certainly no NothingType as well...
  lazy val errors: Seq[Error] =
    badEntities ++ unifyErrors ++ semanticErrors

  /** Given a type, returns a new type that replaces type variables as much as possible, given the map m.
    * This is the type representing the group of types.
    */
  private def find(t: Type): Type =
    if (mappings.contains(t)) mappings(t).root else t

  /** Reconstruct the type into a user-friendly type: in practice this means resolving any "inner" type variables
    * and giving preference to UserType (instead of the root of the unification group) when it exists.
    */
  private def walk(t: Type): Type = {

    def pickMostRepresentativeType(g: Group): Type = {
      // Collect a UserType definition if it exists.
      // We assume the UserType is the most friendly one to return to the user.
      val userType = g.tipes.collectFirst { case u: UserType => u }
      userType match {
        case Some(ut) =>
          ut
        case None =>
          val ct = g.tipes.find { case _: VariableType => false; case _ => true }
          ct match {
            case Some(t) => t
            case None =>
              val vt = g.tipes.find { case _: TypeVariable => false; case _ => true }
              vt match {
                case Some(t) => t
                case None => g.root
              }
          }
      }
    }
//
//            .getOrElse(g.tipes))
//
//          // If we could not find a UserType, try to return anything that is not a VariableType if it exists.
//          // Otherwise, we are forced to return the VariableType (e.g. TypeVariable, Constraint, ...).
//          val ts = g.tipes.filter { case _: TypeVariable => false case _ => true }
//          val nt =
//            if (ts.nonEmpty)
//              ts.head
//            else
//              g.root
//
//          // here narrow down to variable types
//
//          nt
//      }
//    }

    def reconstructType(t: Type, occursCheck: Set[Type]): Type = {

//      here i should always pick the most representative, no?
//      i'm getting a partially constrainted thing, when i need more than that

      if (occursCheck.contains(t)) {
        t
      } else {
        t match {
          case _: NothingType   => t
          case _: AnyType       => t
          case _: IntType       => t
          case _: BoolType      => t
          case _: FloatType     => t
          case _: StringType    => t
          case _: PrimitiveType => if (!mappings.contains(t)) t else reconstructType(pickMostRepresentativeType(mappings(t)), occursCheck + t)
          case _: NumberType    => if (!mappings.contains(t)) t else reconstructType(pickMostRepresentativeType(mappings(t)), occursCheck + t)
          case _: UserType      => t
          case RecordType(atts, name) => RecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, name)
          case ListType(innerType) => ListType(reconstructType(innerType, occursCheck + t))
          case SetType(innerType)  => SetType(reconstructType(innerType, occursCheck + t))
          case BagType(innerType)  => BagType(reconstructType(innerType, occursCheck + t))
          case FunType(p, e)       => FunType(reconstructType(p, occursCheck + t), reconstructType(e, occursCheck + t))
          case ConstraintRecordType(atts, sym) => ConstraintRecordType(atts.map { case AttrType(idn1, t1) => AttrType(idn1, reconstructType(t1, occursCheck + t)) }, sym)
          case ConstraintCollectionType(innerType, c, i, sym) => ConstraintCollectionType(reconstructType(innerType, occursCheck + t), c, i, sym)
          case t1: TypeVariable => if (!mappings.contains(t1)) t1 else reconstructType(pickMostRepresentativeType(mappings(t1)), occursCheck + t)
        }
      }
    }

    reconstructType(t, Set())
  }

  def printVarMap(m: VarMap) = logger.debug(m.toString())

  /** Return the type of an expression.
    */
  def tipe(e: Exp): Type = {
//    logger.debug("FINAL VarMap:")
//    printVarMap(mappings)
//    logger.debug(s"expType of e $e is ${expType(e)}")
//    logger.debug("Final map")
//    logger.debug(mappings.printAllMap())
    walk(expType(e))
  }

  /** Constraints of a node.
    * Each entry represents the constraints (aka. the facts) that a given node adds to the overall type checker.
    */
  def constraint(n: RawNode): Seq[Constraint] = {
    val cs = constraint1(n)
    for (c <- cs) {
      c.pos = n.pos
    }
    cs
  }

  def constraint1(n: RawNode): Seq[Constraint] = {

    import Constraint._

    n match {
      // Rule 4
      case n @ RecordProj(e, idn) =>
        Seq(
          HasType(e, ConstraintRecordType(Set(AttrType(idn, expType(n))))))

      // Rule 6
      case n @ IfThenElse(e1, e2, e3) =>
        Seq(
          HasType(e1, BoolType(), Some("if condition must be a boolean")),
          SameType(e2, e3, Some("then and else must be of the same type")),
          SameType(n, e2))

      // Rule 7
      case n @ FunAbs(p, e1) =>
        Seq(
          HasType(n, FunType(patternType(p), expType(e1))))

      // Rule 8
      case n @ FunApp(f, e) =>
        Seq(
          HasType(f, FunType(expType(e), expType(n))))

      // Rule 11
      case n @ MergeMonoid(_: BoolMonoid, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          HasType(e1, BoolType()),
          HasType(e2, BoolType()))

      case n @ MergeMonoid(_: NumberMonoid, e1, e2) =>
        Seq(
          HasType(n, NumberType()),
          SameType(n, e1),
          SameType(e1, e2)
        )

      // Rule 12
      case n @ MergeMonoid(_: CollectionMonoid, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, ConstraintCollectionType(TypeVariable(), None, None))
        )

      // Rule 13
      case n @ Comp(_: NumberMonoid, _, e) =>
        Seq(
          HasType(e, NumberType()),
          SameType(n, e)
        )
      case n @ Comp(_: BoolMonoid, _, e)   =>
        Seq(
          HasType(e, BoolType()),
          SameType(n, e)
        )

      // Binary Expression type
      case n @ BinaryExp(_: EqualityOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e1, e2))

      case n @ BinaryExp(_: ComparisonOperator, e1, e2) =>
        Seq(
          HasType(n, BoolType()),
          SameType(e2, e1),
          HasType(e1, NumberType()))

      case n @ BinaryExp(_: ArithmeticOperator, e1, e2) =>
        Seq(
          SameType(n, e1),
          SameType(e1, e2),
          HasType(e2, NumberType()))

      // Unary Expression type
      case n @ UnaryExp(_: Neg, e) =>
        Seq(
          SameType(n, e),
          HasType(e, NumberType()))

      // Expression block type
      case n @ ExpBlock(_, e) =>
        Seq(
          SameType(n, e))

      // Generator
      case n @ Gen(p, e) =>
        Seq(
          HasType(e, ConstraintCollectionType(patternType(p), None, None)))

      // Bind
      case n @ Bind(p, e) =>
        Seq(
          HasType(e, patternType(p)))

      case _ =>
        Seq()
    }
  }

  lazy val constraints: RawNode => Seq[Constraint] = attr {
    n => {

      val cs = collect[Seq, Seq[Constraint]] {
        case n: RawNode => constraint(n)
      }

      val alls = scala.collection.mutable.MutableList[Constraint]()

      everywherebu( query[RawNode] { case n: RawNode => alls.++=(constraint(n)) } )  (n)



      val t: Seq[Seq[Constraint]] = cs(n)
      val r = t.flatMap(cs => cs)
      logger.debug(s"We are at $n")
      logger.debug(s"Classic is $r")
      logger.debug(s"New is     $alls")

      def rec(n: RawNode): Seq[Constraint] = n match {
        case n @ Bind(_, e) => rec(e) ++ constraint(n)
        case n @ Gen(_, e) => rec(e) ++ constraint(n)
        case n @ Comp(_, qs, e) => qs.flatMap{ case q => rec(q) } ++ rec(e) ++ constraint(n)
        case n @ FunAbs(_, e) => rec(e) ++ constraint(n)
        case n @ ExpBlock(bs, e) => bs.flatMap{ case b => rec(b) } ++ rec(e) ++ constraint(n)
        case n @ MergeMonoid(_, e1, e2) => rec(e1) ++ rec(e2) ++ constraint(n)
        case n @ BinaryExp(_, e1, e2) => rec(e1) ++ rec(e2) ++ constraint(n)
        case n @ UnaryExp(_, e) => rec(e) ++ constraint(n)
        case _: IdnExp => constraint(n)
        case n @ RecordProj(e, _) => rec(e) ++ constraint(n)
        case _: Const => constraint(n)
        case n @ RecordCons(atts) => atts.flatMap { case att => rec(att.e) } ++ constraint(n)
        case n @ FunApp(f, e) => rec(f) ++ rec(e) ++ constraint(n)
        case n @ ConsCollectionMonoid(_, e) => rec(e) ++ constraint(n)
        case n @ IfThenElse(e1, e2, e3) => rec(e1) ++ rec(e2) ++ rec(e3) ++ constraint(n)
//        case _ => rec(n)
      }
//
//      maybe i dont want to do this way
//      maybe i really want to solve a thing on-demand
//      and let the scope drive it
//      so id really type the rhs



      //alls.to
      rec(n)
    }
  }

  def solution(n: RawNode): Unit = {
    logger.debug(s"CONSTRAINTS ARE ${constraints(n)}")
//    solve(flattenAnds(Constraint.And(constraints(n): _*)))
    solve(constraints(n))
  }

  def printTypedTree(): Unit = {

    if (queryString.isEmpty) {
      return
    }
    val q = queryString.head

    def printMap(pos: Set[Position], t: Type) = {
      val posPerLine = pos.groupBy(_.line)
      var output = s"Type: ${TypesPrettyPrinter(t)}\n"
      logger.debug(s"pos are $pos")
      for ((line, lineno) <- q.split("\n").zipWithIndex) {
        output += line + "\n"
        if (posPerLine.contains(lineno + 1)) {
          val cols = posPerLine(lineno + 1).map(_.column).toList.sortWith(_ < _)
          var c = 0
          for (col <- cols) {
            output += " " * (col - c - 1)
            output += "^"
            c = col
          }
          output += "\n"
        }
      }
      output
    }

    // for each expression in the tree, on a dfs manner
    // find the group it belongs to
    // then find all the groups
    // then print it all
    // then remove the group
    var groups = scala.collection.mutable.Set[Group]()

    val collectMaps = collect[List, (Type, Position)] {
      case e: Exp => {
        val t = expType(e)
        if (!mappings.contains(t)) {
          t -> e.pos
        } else {
          walk(mappings(t).root) -> e.pos
        }
      }
    }

    for ((t, items) <- collectMaps(tree.root).groupBy(_._1)) {
      logger.debug(printMap(items.map(_._2).toSet, t))
    }

  }

}

// TODO: Add more tests to the SemanticAnalyzer with the intuit of testing the error reporting: the error messages may not yet be the most clear.
// TODO: Report unrelated errors by returning multiple Lefts - likely setting things to NothingType and letting them propagate.
//       Related with ths one, is the notion of whether Left(...) on solve should bother to return the bad map, since all we
//       actually need are the error messages.
// TODO: Consider adding syntax like: fun f(x) -> if (x = 0) then 1 else f(x - 1) * x)
//       It should just type to FunType(IntType(), IntType().
//       It is not strictly needed but the notion of a NamedFunction may help code-generation because these are things we don't inline/consider inlining,
// TODO: Do we need to add a closure check, or is that executor-specific?
// TODO: Add check to forbid polymorphic recursion (page 377 or 366 of ML Impl book)
// TODO: Add issue regarding polymorphic code generation:
//       e.g. if Int, Bool on usage generate 2 versions of the method;
//       more interestingly, if ConstraintRecordType, find out the actual records used and generate versions for those.
// TODO: Add notion of declaration. Bind and NamedFunc are now declarations. ExpBlock takes sequence of declarations followed by an expression.
// TODO: Re-do Unnester to use the same tree. Refactor code into new package raw.core
// TODO: Add support for typing an expression like max(students.age) where students is a collection. Or even max(students.personal_info.age)

// TODO: If I do yield bag, I think I also constrain on what the input's commutativity and associativity can be!...
//       success("""\x -> for (y <- x) yield bag (y.age * 2, y.name)""", world,
// TODO: I should be able to do for (x <- col) yield f(x) to keep same collection type as in col
//       This should only happen for a single col I guess?. It helps write the map function.
