package raw
package calculus

import com.typesafe.scalalogging.LazyLogging

import World.MonoidsVarMap

trait MonoidsGraph extends LazyLogging {

  case class MonoidProperties(commutative: Option[Boolean],
                              idempotent: Option[Boolean])

  private case class MonoidOrder(min: Monoid, max: Monoid)

  private val monoidProperties = scala.collection.mutable.HashMap[Monoid, MonoidProperties]()
  private var monoidOrders = scala.collection.mutable.ListBuffer[MonoidOrder]()
  private val monoidsVarMap = new MonoidsVarMap()

  def logMonoidsGraph() = {
    def pp(p: Option[Boolean]) =
      if (p.isDefined) p.get.toString else "?"

    logger.debug(
      s"Monoid details:\n" +
      monoidProperties.map {
      case (m, props) => s"${PrettyPrinter(m)}(commutative:${pp(props.commutative)}, idempotent:${pp(props.idempotent)})"
      }.mkString("\n"))
  }

  def monoidRoots() = monoidProperties.keySet.to[scala.collection.immutable.Set]   // or monoidsVarMap.getRoots ?

  def mFind(t: Monoid): Monoid =
    if (monoidsVarMap.contains(t)) monoidsVarMap(t).root else t

  private def defaultMonoidProperties(m: Monoid) =
    m match {
      case _: MaxMonoid      => MonoidProperties(Some(true), Some(true))
      case _: MinMonoid      => MonoidProperties(Some(true), Some(true))
      case _: MultiplyMonoid => MonoidProperties(Some(true), Some(false))
      case _: SumMonoid      => MonoidProperties(Some(true), Some(false))
      case _: AndMonoid      => MonoidProperties(Some(true), Some(true))
      case _: OrMonoid       => MonoidProperties(Some(true), Some(true))
      case _: SetMonoid      => MonoidProperties(Some(true), Some(true))
      case _: BagMonoid      => MonoidProperties(Some(true), Some(false))
      case _: ListMonoid     => MonoidProperties(Some(false), Some(false))
      case _                 => MonoidProperties(None, None)
    }

  private def addMonoidProperties(m: Monoid) =
    if (!monoidProperties.contains(m))
      monoidProperties.put(m, defaultMonoidProperties(m))

  def addMonoidOrder(min: Monoid, max: Monoid): Boolean = {
    monoidOrders += MonoidOrder(min, max)
    addMonoidProperties(min)
    addMonoidProperties(max)

    validateAndPropagateMonoids()
  }

  private def replaceInMonoidOrders(o: Monoid, n: Monoid) = {
    val norders = scala.collection.mutable.ListBuffer[MonoidOrder]()
    for (order <- monoidOrders) {
      norders += (order match {
        case MonoidOrder(min, max) if min == o && max == o => MonoidOrder(n, n)
        case MonoidOrder(min, max) if min == o => MonoidOrder(n, max)
        case MonoidOrder(min, max) if max == o => MonoidOrder(min, n)
        case _                                 => order
      })
    }
    monoidOrders = norders
  }

  def unifyMonoids(a: Monoid, b: Monoid): Boolean = {
    addMonoidProperties(a)
    addMonoidProperties(b)

    if (a == b) {
      return true
    }

    val aProps = monoidProperties(a)
    val bProps = monoidProperties(b)

    // check if a is compatible with b

    if (aProps.commutative.isDefined && bProps.commutative.isDefined && aProps.commutative != bProps.commutative) {
      return false
    }
    if (aProps.idempotent.isDefined && bProps.idempotent.isDefined && aProps.idempotent != bProps.idempotent) {
      return false
    }

    // new merged properties

    val cProps = MonoidProperties(aProps.commutative.orElse(bProps.commutative), aProps.idempotent.orElse(bProps.idempotent))

    // replace a and/or b

    if (aProps == cProps) {

      // replace b by a
      replaceInMonoidOrders(b, a)

      // delete b from props
      monoidProperties -= b

      if (validateAndPropagateMonoids()) {
        monoidsVarMap.union(b, a)
        true
      } else {
        false
      }

    } else if (bProps == cProps) {

      // replace a by b
      replaceInMonoidOrders(a, b)

      // delete a from props
      monoidProperties -= a

      if (validateAndPropagateMonoids()) {
        monoidsVarMap.union(a, b)
        true
      } else {
        false
      }

    } else {

      val c = MonoidVariable()

      replaceInMonoidOrders(a, c)
      replaceInMonoidOrders(b, c)

      monoidProperties -= a
      monoidProperties -= b
      monoidProperties.put(c, cProps)

      if (validateAndPropagateMonoids()) {
        monoidsVarMap.union(a, b).union(b, c)
        true
      } else {
        false
      }
    }
  }

  private def validateAndPropagateMonoids(): Boolean = {
    var changed = false

    do {
      changed = false

      for (order <- monoidOrders) {
        val min = order.min
        val max = order.max
        val minProps = monoidProperties(min)
        val maxProps = monoidProperties(max)

        // check if min <= max

        if (minProps.commutative.isDefined && maxProps.commutative.isDefined && minProps.commutative.get && !maxProps.commutative.get) {
          return false
        }

        if (minProps.idempotent.isDefined && maxProps.idempotent.isDefined && minProps.idempotent.get && !maxProps.idempotent.get) {
          return false
        }

        // narrow the maxes

        if (maxProps.commutative.isEmpty && minProps.commutative.isDefined && minProps.commutative.get) {
          monoidProperties.put(max, MonoidProperties(Some(true), maxProps.idempotent))
          changed = true
        }

        if (maxProps.idempotent.isEmpty && minProps.idempotent.isDefined && minProps.idempotent.get) {
          monoidProperties.put(max, MonoidProperties(maxProps.commutative, Some(true)))
          changed = true
        }

        // narrow the mins

        if (minProps.commutative.isEmpty && maxProps.commutative.isDefined && !maxProps.commutative.get) {
          monoidProperties.put(min, MonoidProperties(Some(false), minProps.idempotent))
          changed = true
        }

        if (minProps.idempotent.isEmpty && maxProps.idempotent.isDefined && !maxProps.idempotent.get) {
          monoidProperties.put(min, MonoidProperties(minProps.commutative, Some(false)))
          changed = true
        }

      }

    } while (changed)

    true
  }

  def cloneMonoids(syms: Set[Symbol]) = {

    val newSyms = scala.collection.mutable.HashMap[Symbol, Symbol]()

    def freshMonoid(sym: Symbol): MonoidVariable = {
      if (!newSyms.contains(sym))
        newSyms += (sym -> SymbolTable.next())
      MonoidVariable(newSyms(sym))
    }

    // Clone monoid properties

    val nprops = scala.collection.mutable.HashMap[Monoid, MonoidProperties]()
    for ((m, p) <- monoidProperties) {
      m match {
        case MonoidVariable(sym) if syms.contains(sym) =>
          nprops += freshMonoid(sym) -> MonoidProperties(p.commutative, p.idempotent)
        case _ =>
      }
    }
    for ((m, p) <- nprops) {
      monoidProperties += m -> p
    }

    // Clone orders

    val norders = scala.collection.mutable.ListBuffer[MonoidOrder]()
    for (o <- monoidOrders) {
      norders.append(o match {
        case MonoidOrder(MonoidVariable(a), MonoidVariable(b)) if syms.contains(a) && syms.contains(b) => MonoidOrder(freshMonoid(a), freshMonoid(b))
        case MonoidOrder(MonoidVariable(a), b) if syms.contains(a) => MonoidOrder(freshMonoid(a), b)
        case MonoidOrder(a, MonoidVariable(b)) if syms.contains(b) => MonoidOrder(a, freshMonoid(b))
        case _                                 => o
      })
    }
    for (o <- norders) {
      monoidOrders += o
    }

    newSyms
  }

  def monoidProps(m: Monoid): MonoidProperties =
    monoidProperties.getOrElse(m, defaultMonoidProperties(m))

}
