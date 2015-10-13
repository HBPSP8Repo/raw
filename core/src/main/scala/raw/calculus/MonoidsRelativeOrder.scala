///*
//
//The data structure contains:
//- a map of monoids to its commutative & idempotent properties.
//  This is always "up to date". It says what is our lowest bound commutative,greater bound commutative,lowest bound idempotent, greatest bound idempotent
//- a sequence - pairs! - of relative orders.
//
//when we unify a with b, we update 'b' properties with the best known current bounds based on itself and on 'a'
//we then go to the sequence of relative orders and replace 'a' by 'b' so that map always contain roots only
//we then walk that relative order and we do 2 things:
//- we validate if the order is correct (if we are e.g. some(true) and our greater is some(false) we must fail unification altogether and return the previous "world"/graph)
//- we also narrow our greaters and smallers (e.g. if we are sone(true) our greater that is None in its lowest bound or even greater bound, it  must become some(true)
//- whenever any thing - any property - changes in the map, we re-run the whole validation for the entire chains/sequence
//  this is needed to make sure we have a coherent "world"
//  when it stops and stabilizes, if it works, the unification really worked and we can return this new "world".
//the changes are done in a clone of the original data structure.
//
//
//
//monoid properties out of monoid varibles -> otherwise can't update map in place
//
//
//for concats, have a lookup data struture for its froms
//actually storing froms doesnt even exist: this is the bootstrap: this is adding the structures
//
// */
//
//
//package raw
//package calculus
//
//import raw.World.{MonoidsVarMap, VarMap}
//
//
//case class MonoidProperties(commutative: Option[Boolean],
//                      idempotent: Option[Boolean])
//
//case class MonoidOrder(min: Monoid, max: Monoid)
//
//
//class MonoidsGraph(val props: Map[Monoid, MonoidProperties],
//                   val orders: Set[MonoidOrder],
//                   val varMap: VarMap[Monoid]) {
//
//  def copy() = {
//    val nprops = scala.collection.mutable.HashMap[Monoid, MonoidProperties]()
//    for (p <- props)
//      nprops += p
//
//    val norders = scala.collection.mutable.Set[MonoidOrder]()
//    for (o <- orders)
//      norders += o
//
//    (nprops, norders)
//  }
//
//  private def getDefaultProperties(m: Monoid) = m match {
//    case _: MaxMonoid      => MonoidProperties(Some(true), Some(true))
//    case _: MinMonoid      => MonoidProperties(Some(true), Some(true))
//    case _: MultiplyMonoid => MonoidProperties(Some(true), Some(false))
//    case _: SumMonoid      => MonoidProperties(Some(true), Some(false))
//    case _: AndMonoid      => MonoidProperties(Some(true), Some(true))
//    case _: OrMonoid       => MonoidProperties(Some(true), Some(true))
//    case _: SetMonoid      => MonoidProperties(Some(true), Some(true))
//    case _: BagMonoid      => MonoidProperties(Some(true), Some(false))
//    case _: ListMonoid     => MonoidProperties(Some(false), Some(false))
//    case _                 => MonoidProperties(None, None)
//  }
//
//  def addOrder(min: Monoid, max: Monoid): Option[MonoidsGraph] = {
//    val (nprops, norders) = copy()
//    norders += MonoidOrder(min, max)
//    if (!nprops.contains(min))
//      nprops.put(min, getDefaultProperties(min))
//    if (!nprops.contains(max))
//      nprops.put(max, getDefaultProperties(max))
//
//    validateAndPropagate(nprops, norders) match {
//      case Some((nprops1, norders1)) => Some(new MonoidsGraph(nprops1.toMap, norders1.to, varMap))
//      case None => None
//    }
//  }
//
//  type MutablePropertiesMap = scala.collection.mutable.HashMap[Monoid, MonoidProperties]
//  type MutableOrders = scala.collection.mutable.Set[MonoidOrder]
//
//  def replaceInOrders(o: Monoid, n: Monoid, orders: MutableOrders) =
//    orders.map{ case order => order match {
//        case MonoidOrder(min, max) if min == o && max == o => MonoidOrder(n, n)
//        case MonoidOrder(min, max) if min == o => MonoidOrder(n, max)
//        case MonoidOrder(min, max) if max == o => MonoidOrder(min, n)
//        case _ => order
//      }
//    }
//
//  def unify(a: Monoid, b: Monoid): Option[MonoidsGraph] = {
//    val n = copy()
//    val nprops = n._1
//    var norders = n._2
//    if (!nprops.contains(a))
//      nprops.put(a, getDefaultProperties(a))
//    if (!nprops.contains(b))
//      nprops.put(b, getDefaultProperties(b))
//
//    val aProps = nprops(a)
//    val bProps = nprops(b)
//
//    // check if a is compatible with b
//
//    if (aProps.commutative.isDefined && bProps.commutative.isDefined && aProps.commutative != bProps.commutative)
//      return None
//    if (aProps.idempotent.isDefined && bProps.idempotent.isDefined && aProps.idempotent != bProps.idempotent)
//      return None
//
//    // new merged properties
//
//    val cProps = MonoidProperties(aProps.commutative.orElse(bProps.commutative), aProps.idempotent.orElse(bProps.idempotent))
//
//    // replace a and/or b
//
//    if (aProps == cProps) {
//
//      // replace b by a
//      norders = replaceInOrders(b, a, norders)
//
//      // delete b from props
//      nprops -= b
//
//      validateAndPropagate(nprops, norders) match {
//        case Some((nprops1, norders1)) => Some(new MonoidsGraph(nprops1.toMap, norders1.to, varMap.union(b, a)))
//        case None => None
//      }
//
//    } else if (bProps == cProps) {
//
//      // replace a by b
//      norders = replaceInOrders(a, b, norders)
//
//      // delete a from props
//      nprops -= a
//
//      validateAndPropagate(nprops, norders) match {
//        case Some((nprops1, norders1)) => Some(new MonoidsGraph(nprops1.toMap, norders1.to, varMap.union(a, b)))
//        case None => None
//      }
//
//    } else {
//
//      val c = MonoidVariable()
//
//      norders = replaceInOrders(a, c, norders)
//      norders = replaceInOrders(b, c, norders)
//
//      nprops -= a
//      nprops -= b
//      nprops.put(c, cProps)
//
//      validateAndPropagate(nprops, norders) match {
//        case Some((nprops1, norders1)) => Some(new MonoidsGraph(nprops1.toMap, norders1.to, varMap.union(a, b).union(b, c)))
//        case None => None
//      }
//
//    }
//  }
//
//  private def validateAndPropagate(nprops: MutablePropertiesMap,
//                                   norders: MutableOrders): Option[(MutablePropertiesMap, MutableOrders)] = {
//    var changed = false
//
//    do {
//      for (order <- norders) {
//        val min = order.min
//        val max = order.max
//        val minProps = nprops(min)
//        val maxProps = nprops(max)
//
//        // check if min <= max
//
//        if (minProps.commutative.isDefined && maxProps.commutative.isDefined && minProps.commutative.get && !maxProps.commutative.get)
//          return None
//
//        if (minProps.idempotent.isDefined && maxProps.idempotent.isDefined && minProps.idempotent.get && !maxProps.idempotent.get)
//          return None
//
//        // narrow the maxes
//
//        if (maxProps.commutative.isEmpty && minProps.commutative.isDefined && minProps.commutative.get) {
//          nprops.put(max, MonoidProperties(Some(true), maxProps.idempotent))
//          changed = true
//        }
//
//        if (maxProps.idempotent.isEmpty && minProps.idempotent.isDefined && minProps.idempotent.get) {
//          nprops.put(max, MonoidProperties(maxProps.commutative, Some(true)))
//          changed = true
//        }
//
//        // narrow the mins
//
//        if (minProps.commutative.isEmpty && maxProps.commutative.isDefined && !maxProps.commutative.get) {
//          nprops.put(min, MonoidProperties(Some(false), minProps.idempotent))
//          changed = true
//        }
//
//        if (minProps.idempotent.isEmpty && maxProps.idempotent.isDefined && !maxProps.idempotent.get) {
//          nprops.put(min, MonoidProperties(minProps.commutative, Some(false)))
//          changed = true
//        }
//
//      }
//
//    } while (changed)
//
//    Some(nprops.toMap, norders)
//  }
//
//}