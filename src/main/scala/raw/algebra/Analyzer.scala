//package raw
//package algebra
//
//import org.kiama.attribution.Attribution
//
//case class AnalyzerError(err: String) extends RawException
//
//class Analyzer(tree: LogicalAlgebra.Algebra, world: World) extends Attribution {
//
//  import org.kiama.attribution.Decorators
//  import LogicalAlgebra._
//
//  /** Decorators on the tree.
//    */
//  lazy val decorators = new Decorators(tree)
//
//  import decorators.{chain, Chain}
//
//  /** ... */
//  lazy val expressionType: AlgebraNode => Exp => Type = paramAttr {
//    n => {
//          case Null                                    => UnknownType()
//          case _: BoolConst                            => BoolType()
//          case _: IntConst                             => IntType()
//          case _: FloatConst                           => FloatType()
//          case _: StringConst                          => StringType()
//          case Arg(i)                                  => tipe(n)(i)
//          case RecordProj(e, idn)                      => expressionType(e) match {
//            case RecordType(atts) => atts.collectFirst { case AttrType(`idn`, t) => t}.head
//            case t                => throw AnalyzerError(s"Record type expected but got $t")
//          }
//          case RecordCons(atts)                        => RecordType(atts.map(att => AttrType(att.idn, expressionType(n)(att.e))))
//          case IfThenElse(_, e2, _)                    => expressionType(n)(e2)
//          case BinaryExp(_: ComparisonOperator, _, _)  => BoolType()
//          case BinaryExp(_: EqualityOperator, _, _)    => BoolType()
//          case BinaryExp(_: ArithmeticOperator, e1, _) => expressionType(n)(e1)
//          case UnaryExp(_: Not, _)                     => BoolType()
//          case UnaryExp(_: Neg, e)                     => expressionType(n)(e)
//          case UnaryExp(_: ToBool, _)                  => BoolType()
//          case UnaryExp(_: ToInt, _)                   => IntType()
//          case UnaryExp(_: ToFloat, _)                 => FloatType()
//          case UnaryExp(_: ToString, _)                => StringType()
//          case ZeroCollectionMonoid(m)                 => CollectionType(m, UnknownType())
//          case ConsCollectionMonoid(m, e)              => CollectionType(m, expressionType(n)(e))
//          case MergeMonoid(_, e1, _)                   => expressionType(n)(e1)
//        }
//      }
//
//  // TODO: What about Merge between a list and a set?
//  lazy val tipe: AlgebraNode => CollectionType = attr {
//    case Scan(name)              => world.getSource(name).tipe
//
//    /** Rule T17 */
//    case Join(_, left, right) => val m = maxMonoid(tipe(left), tipe(right)); CollectionType(m, tipe(left) * tipe(right))
//
//    /** Rule T18 */
//    case Select(_, child)        => tipe(child)
//
//    /** Rule T19 */
//
//    /** Rule T20 */
//    case r @ Reduce(_: PrimitiveMonoid, e, _, child) => tipe(child) :+ expressionType(child)(e)
//    case r @ Reduce(m: CollectionMonoid, e, _, child) => tipe(child) :+ CollectionType(m, expressionType(child)(e))
//
//    case OuterJoin(_, left, right) => tipe(left) ++ tipe(right) :+
//
//
//    //      /** Nest
//    //        */
//    //      case class Nest(m: Monoid, e: Exp, f: List[Arg], ps: List[Exp], g: List[Arg], child: AlgebraNode) extends AlgebraNode
//    //
//    //      /** Unnest
//    //        */
//    //      case class Unnest(p: Path, ps: List[Exp], child: AlgebraNode) extends AlgebraNode
//    //
//    //      /** OuterUnnest
//    //        */
//    //      case class OuterUnnest(p: Path, ps: List[Exp], child: AlgebraNode) extends AlgebraNode
//    //
//    //      /** Merge
//    //        */
//    //      case class Merge(m: Monoid, left: AlgebraNode, right: AlgebraNode) extends AlgebraNode
//    //  }
//
//  }
//}
