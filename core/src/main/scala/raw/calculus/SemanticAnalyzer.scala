package raw
package calculus

class SemanticAnalyzer(tree: Calculus.Calculus, world: World, queryString: String) extends BaseAnalyzer(tree, world, queryString) {

  import scala.collection.immutable.Seq
  import org.kiama.rewriting.Rewriter._
  import org.kiama.util.UnknownEntity
  import Calculus._
  import SymbolTable._

  /** Add the nullable flag to a type.
    */
  // TODO: Should we use u.sym or also copy the symbol to a new one: e.g. Symbol(u.sym.idn) ?
  private def makeNullable(source: Type, models: Seq[Type], nulls: Seq[Type], nullable: Option[Boolean] = None): Type = {
    val t = (source, models) match {
      case (col @ CollectionType(m, i), colls: Seq[CollectionType]) =>
        val inners = colls.map(_.innerType)
        CollectionType(m, makeNullable(i, inners, inners, nullable))
      case (f @ FunType(p, e), funs: Seq[FunType])                  =>
        val otherP = funs.map(_.t1)
        val otherE = funs.map(_.t2)
        FunType(makeNullable(p, otherP, otherP, nullable), makeNullable(e, otherE, otherE, nullable))
      case (r @ RecordType(recAtts: Attributes), _)         =>
        val recs = models.collect{case r @ RecordType(_: Attributes) => r}
            RecordType(Attributes(
              for ((att, idx) <- recAtts.atts.zipWithIndex) yield {
                val others = for (rec <- recs) yield {
                  rec.recAtts match {
                    case Attributes(atts1) => atts1(idx)
                  }
                }
                AttrType(att.idn, makeNullable(att.tipe, others.map(_.tipe), others.map(_.tipe), nullable))
              }))

      case (r @ RecordType(recAtts: AttributesVariable), _)         =>
        val recs = models.collect{case r @ RecordType(_: AttributesVariable) => r}
            RecordType(AttributesVariable(
              recAtts.atts.map { case att =>
                val others = (recs.map{ rec =>
                  rec.recAtts match {
                    case atts1: AttributesVariable => atts1.getType(att.idn).orElse(None)
                  }
                }).collect{case o if o.isDefined => o.get}
                AttrType(att.idn, makeNullable(att.tipe, others, others, nullable))
              }))

      case (r @ RecordType(recAtts: ConcatAttributes), _)         =>
        val recs = models.collect{case r @ RecordType(_: ConcatAttributes) => r}
        RecordType(recAtts)
      //            atts.zip(recs.map {
      //              _.recAtts
      //            }).map { case (a1, as) => AttrType(a1.idn, makeNullable(a1.tipe, as.atts.map(_.tipe).to, as.atts.map(_.tipe).to, nullable)) }
      //          case AttributesVariable(atts, sym) =>
      //            atts.zip(recs.map {
      //              _.recAtts
      //            }).map { case (a1, as) => AttrType(a1.idn, makeNullable(a1.tipe, as.atts.map(_.tipe).to, as.atts.map(_.tipe).to, nullable)) }
      //        }
      //
      //              _.tipe))
      //            for (otherAtts <- recs.map(_.recAtts)) {
      //              atts.zip(otherAtts.atts).map{case (a1, a2) => AttrType(a1.idn, makeNullable(a1.tipe, a2.tipe))
      //            }
      //              val natts = atts.map { case AttrType(idn, i) =>
      //              val others = recs.map { r => r.getType(idn).get }
      //              AttrType(idn, makeNullable(i, others, others, nullable))
      //            }
      //            RecordType(Attributes(natts), n)
      //          case AttributesVariable(atts, sym) =>
      //            val natts = atts.map { case AttrType(idn, i) =>
      //              val others = recs.map { r => r.getType(idn).get }
      //              AttrType(idn, makeNullable(i, others, others, nullable))
      //            }
      //            RecordType(AttributesVariable(natts, sym), n)
      //        }
      case (PatternType(atts), _) =>
        PatternType(atts.map { att => PatternAttrType(makeNullable(att.tipe, Seq(), Seq(), nullable)) })
      case (_: IntType, _)        => IntType()
      case (_: FloatType, _)      => FloatType()
      case (_: BoolType, _)       => BoolType()
      case (_: StringType, _)     => StringType()
      case (u: UserType, _)       => UserType(u.sym)
      case (v: TypeVariable, _)   => TypeVariable(v.sym)
      case (v: NumberType, _)     => NumberType(v.sym)
    }
    t.nullable = nullable.getOrElse(t.nullable || nulls.collect { case t if t.nullable => t }.nonEmpty) // TODO: nulls.exists ?
    t
  }

  /** Return the type of an expression, including the nullable flag.
    */
  lazy val tipe: Exp => Type = attr {
    e => {
      def innerTipe(t: Type): Type = resolvedType(t) match {
        case CollectionType(_, i) => i
      }

      val te = cloneType(baseType(e)) // regular type (no option except from sources)

      val nt = e match {
        case RecordProj(e1, idn)                 => tipe(e1) match {
          case rt: RecordType =>
            makeNullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
          case ut: UserType   =>
            typesVarMap(ut).root match {
              case rt: RecordType => makeNullable(te, Seq(rt.getType(idn).get), Seq(rt, rt.getType(idn).get))
            }
        }
        case ConsCollectionMonoid(m, e1)         => CollectionType(m, tipe(e1))
        case MultiCons(m, exps)                  => te match {
          case CollectionType(m2, inner) =>
            assert(m == m2)
            val others = exps.map(tipe(_))
            CollectionType(m, makeNullable(inner, others, others))
        }
        case IfThenElse(e1, e2, e3)              => (tipe(e1), tipe(e2), tipe(e3)) match {
          case (t1, t2, t3) => makeNullable(te, Seq(t2, t3), Seq(t1, t2, t3))
        }
        case FunApp(f, v)                        => tipe(f) match {
          case ft @ FunType(t1, t2) =>
            makeNullable(te, Seq(t2), Seq(ft, t2, tipe(v)))
        }
        case MergeMonoid(_, e1, e2)              => (tipe(e1), tipe(e2)) match {
          case (t1, t2) => makeNullable(te, Seq(t1, t2), Seq(t1, t2))
        }
        case Comp(m: CollectionMonoid, qs, proj) =>
          val inner = tipe(proj)
          makeNullable(te, Seq(CollectionType(m, inner)), qs.collect { case Gen(_, e1) => tipe(e1) })

        case Comp(m: PrimitiveMonoid, qs, proj) =>
          // TODO: ??? no need for cloneType? How should we codegen? Filter out nullables first then get the zero monoid?
          //val output_type = cloneType(tipe(proj), None)
          //output_type.nullable = false
          //makeNullable(te, Seq(output_type), qs.collect { case Gen(_, e1) => tipe(e1) })
          resetNullable(proj)
          makeNullable(te, Seq(tipe(proj)), qs.collect { case Gen(_, e1) => tipe(e1) })

        case Select(froms, d, g, proj, w, o, h)     =>
          val inner = tipe(proj)
          // we don't care about the monoid here, sine we just walk the types to make them nullable or not, not the monoids
          makeNullable(te, Seq(CollectionType(SetMonoid(), inner)), froms.collect { case Gen(_, e1) => tipe(e1) })
        case Reduce(m: PrimitiveMonoid, g, e1)      => makeNullable(te, Seq(tipe(e1)), Seq(tipe(g.e)))
        case Reduce(m: CollectionMonoid, g, e1)     => makeNullable(te, Seq(CollectionType(m, tipe(e1))), Seq(tipe(g.e)))
        case Filter(g, p)                           => makeNullable(te, Seq(tipe(g.e)), Seq(tipe(g.e)))
        case Join(g1, g2, p)                        => te match {
          case CollectionType(m, inner) => {
            val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
            makeNullable(te, Seq(expectedType), Seq(tipe(g1.e), tipe(g2.e)))
          }
        }
        case OuterJoin(g1, g2, p)                   => {
          resetNullable(p)
          val x = te match {
            case CollectionType(m, inner) => {
              val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
              makeNullable(te, Seq(expectedType), Seq(tipe(g1.e), tipe(g2.e)))
            }
          }
          x match {
            case CollectionType(_, RecordType(Attributes(atts))) =>
              assert(atts.length == 2)
              atts(1).tipe.nullable = true
          }
          x
        }
        case Unnest(g1, g2, p)                      =>
          te match {
            case CollectionType(m, inner) => {
              val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
              makeNullable(te, Seq(expectedType), Seq(tipe(g1.e), tipe(g2.e)))
            }
          }
        case OuterUnnest(g1, g2, p)                 =>
          resetNullable(p)
          val x = te match {
            case CollectionType(m, inner) => {
              val expectedType = CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g1.e))), AttrType("_2", innerTipe(tipe(g2.e)))))))
              makeNullable(te, Seq(expectedType), Seq(tipe(g1.e)))
            }
          }
          x match {
            case CollectionType(_, RecordType(Attributes(atts))) =>
              assert(atts.length == 2)
              atts(1).tipe.nullable = true
          }
          x
        case Nest(m: CollectionMonoid, g, k, p, e1) => {
          resetNullable(p)
          resetNullable(e1)
          te match {
            case CollectionType(m2, _) =>
              makeNullable(te, Seq(CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", tipe(k)), AttrType("_2", CollectionType(m2, tipe(e1)))))))), Seq(tipe(g.e)))
          }
        }
        case Nest(m: PrimitiveMonoid, g, k, p, e1)  => {
          resetNullable(p)
          resetNullable(e1)
          te match {
              // TODO: Is overriding 'm' here correct actually? Or is it the m from above???
            case CollectionType(m, _) => makeNullable(te, Seq(CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", tipe(k)), AttrType("_2", tipe(e1))))))), Seq(tipe(g.e)))
          }
        }

        case Nest2(m: CollectionMonoid, g, k, p, e1) => {
          resetNullable(p)
          resetNullable(e1)
          te match {
            case CollectionType(m2, _) =>
              makeNullable(te, Seq(CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g.e))), AttrType("_2", CollectionType(m2, tipe(e1)))))))), Seq(tipe(g.e)))
          }
        }
        case Nest2(m: PrimitiveMonoid, g, k, p, e1)  => {
          resetNullable(p)
          resetNullable(e1)
          te match {
            // TODO: Is overriding 'm' here correct actually? Or is it the m from above???
            case CollectionType(m, _) => makeNullable(te, Seq(CollectionType(m, RecordType(Attributes(Seq(AttrType("_1", innerTipe(tipe(g.e))), AttrType("_2", tipe(e1))))))), Seq(tipe(g.e)))
          }
        }

        case BinaryExp(_, e1, e2)    => makeNullable(te, Seq(), Seq(tipe(e1), tipe(e2)))
        case InExp(e1, e2)           => makeNullable(te, Seq(), Seq(tipe(e1), tipe(e2)))
        case UnaryExp(_, e1)         => makeNullable(te, Seq(), Seq(tipe(e1)))
        case ExpBlock(_, e1)         =>
          val t1 = tipe(e1)
          makeNullable(te, Seq(t1), Seq(t1))
        case idnExp @ IdnExp(idnUse) =>
          entity(idnUse) match {
            case VariableEntity(idnDef, _) => cloneType(idnDefType(idnDef))
            case _: DataSourceEntity       => te
            case _: UnknownEntity          =>
              // It's an anonymous alias
              lookupAttributeEntity(idnExp) match {
                case GenAttributeEntity(_, g, idx) =>
                  resolvedType(resolvedType(tipe(g.e)).asInstanceOf[CollectionType].innerType).asInstanceOf[RecordType].recAtts match {
                    case Attributes(atts) => cloneType(atts(idx).tipe)
                  }
                case ie @ IntoAttributeEntity(_, i, idx) =>
                  resolvedType(tipe(i.e1)).asInstanceOf[RecordType].recAtts match {
                    case Attributes(atts) => cloneType(atts(idx).tipe)
                  }
              }
          }
        case _: IntConst             => te
        case _: FloatConst           => te
        case _: BoolConst            => te
        case _: StringConst          => te
        case _: RegexConst           => te
        case rc: RecordCons          => te match {
          case RecordType(recAtts: Attributes) => RecordType(Attributes(rc.atts.map { case att => AttrType(att.idn, tipe(att.e)) }))
        }
        case _: ZeroCollectionMonoid => te
        case f: FunAbs               =>
          baseType(f) match {
            case FunType(t1, t2) => FunType(makeNullable(t1, Seq(), Seq(), Some(false)), tipe(f.e))
          }
        case _: Partition            =>
          // TODO: Ben: HELP!!!
          // Ben: I think it should inherit the nullables the related froms, something like that?
          te
        case _: Star                 =>
          // TODO: Ben: HELP!!!
          te
        case Sum(e1)                 => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Max(e1)                 => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Min(e1)                 => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Avg(e1)                 => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Count(e1)               => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Exists(e1)              => makeNullable(te, Seq(), Seq(tipe(e1)))
        case Into(e1, e2)            => makeNullable(te, Seq(tipe(e2)), Seq(tipe(e1), tipe(e2)))

          // TODO: Fix later: NoneOnFail will have a different signature for the output than the other cases!!! Add issue!
        case ParseAs(e1, r, Some(_: NoneOnFail))               => ???
        case ParseAs(e1, r, _)               => makeNullable(te, Seq(), Seq(tipe(e1)))

        case ToEpoch(e1, _) => makeNullable(te, Seq(), Seq(tipe(e1)))
      }
      nt
    }
  }

  def resetNullable(e: Exp) = {
    val c = collect[List, Exp] {
      case e: Exp => e
    }
    // TODO: Improve this :-)
    c(e).map { case e =>
      val t = tipe(e); t.nullable = false
    }
  }

  /** Return the type of an identifier definition.
    * Finds the declaration of the identifier, then its body, then types the body and projects the pattern.
    */
  lazy val idnDefType: IdnDef => Type = attr {
    idn =>
      def findType(p: Pattern, t: Type): Option[Type] = (p, t) match {
        case (PatternIdn(idn1), t1) if idn == idn1 => Some(t1)
        case (_: PatternIdn, _)                    => None
        case (PatternProd(ps), ResolvedType(t1: RecordType)) =>
          ps.zip(t1.recAtts.atts).flatMap { case (p1, att) => findType(p1, att.tipe) }.headOption
        case (PatternProd(ps), ResolvedType(t1: PatternType)) =>
          ps.zip(t1.atts).flatMap { case (p1, att) => findType(p1, att.tipe) }.headOption
      }

      decl(idn) match {
        case Some(Bind(p, e))       => findType(p, tipe(e)).get
        case Some(Gen(Some(p), e))  => findType(p, resolvedType(tipe(e)).asInstanceOf[CollectionType].innerType).get
        case Some(e @ FunAbs(p, _)) => findType(p, baseType(e).asInstanceOf[FunType].t1).get  // TODO: check: Parameters can never be nullable (?)
      }
  }


}
