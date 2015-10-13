//package raw
//package calculus
//
//import com.typesafe.scalalogging.LazyLogging
//
//
////
//// while this would eventually work
//// it is extremely manual
//// we'd rather be adding a "constraint at a time" into the system
//// and if no solution is found
//// then we know this last thing we added breaks stuff
////
//// ideally this would be a rule based thing
//// which would tell me how to compute smtg from the rules, step by step
////
//// what do i know?
//// that if there is a relaive order thing and i'm less than, i cannot be some(true) and my parent some(false)
//// so, there is some assertion that must stay true on the relative order
////
//// also, i should be able to know the value of the property
////
//
//
//
////////////////
//
//class MonoidGraph extends LazyLogging {
//
//  private case class Properties(leqMonoids: Set[Monoid],
//                                geqMonoids: Set[Monoid],
//                                commutative: Option[Boolean],
//                                idempotent: Option[Boolean])
//
//  private var properties = Map[Monoid, Properties]()
//
//  private def getProperties(m: Monoid): Properties =
//    properties.getOrElse(m, Properties(Set(), Set(), None, None))
//
//  private def addGeqDependency(m: Monoid, geq: Monoid): MonoidGraph = {
//    val mProps = getProperties(m)
//    val geqProps = getProperties(m)
//
//    if (mProps.commutative.isDefined && geqProps.commutative.isDefined && mProps.commutative.get && !geqProps.commutative.get) {
//      return null
//    }
//    if (mProps.idempotent.isDefined && geqProps.idempotent.isDefined && mProps.idempotent.get && !geqProps.idempotent.get) {
//      return null
//    }
//
//    var ngraph = this
//    for (leq <- mProps.leqMonoids) {
//      ngraph = ngraph.addGeqDependency(leq, geq)
//      if (ngraph == null) {
//        return null
//      }
//    }
//
//    val ncommutative = ngraph.commutativeForLeq(m)
//    val nidempotent = ngraph.idempotentForLeq(m)
//
//    if (mProps.commutative != ncommutative) {
//
//    }
//
//    // same for idempotent?
//
//
//    Properties(mProps.leqMonoids, mProps.geqMonoids, mProps.commutative, mProps.idempotent)
//
//    ngraph
//  }
//
//  private def addLeqDependency(m: Monoid, leq: Monoid): MonoidGraph =
//    ???
//
//  private def addDependency(m: Monoid, nm: Monoid): MonoidGraph = {
//    val mProps = getProperties(m)
//    val nmProps = getProperties(nm)
//
//    // unification should take care of this, but ...
//    assert(mProps.commutative.isEmpty || nmProps.commutative.isEmpty || mProps.commutative == nmProps.commutative)
//    assert(mProps.idempotent.isEmpty || nmProps.idempotent.isEmpty || mProps.idempotent == nmProps.idempotent)
//
//    val nprops = Properties(mProps.leqMonoids union nmProps.leqMonoids,
//                            mProps.geqMonoids union nmProps.geqMonoids,
//                            mProps.commutative.orElse(nmProps.commutative.orElse(None)),
//                            mProps.idempotent.orElse(nmProps.idempotent.orElse(None)))
//
//    var ngraph = this
//    for (leq <- nprops.leqMonoids) {
//      ngraph = ngraph.addGeqDependency(leq, m)
//      if (ngraph == null) {
//        return this
//      }
//    }
//    for (geq <- nprops.geqMonoids) {
//      ngraph = ngraph.addLeqDependency(geq, m)
//      if (ngraph == null) {
//        return this
//      }
//    }
//    ngraph
//  }
//
//  /** Compute our property based on properties of our children 'a' and 'b'.
//    */
//
//  private def maxOf(a: Option[Boolean], b: Option[Boolean]) = (a, b) match {
//    case (Some(true), _) => a
//    case (_, Some(true)) => b
//    case _               => None
//  }
//
//  private def minOf(a: Option[Boolean], b: Option[Boolean]) = (a, b) match {
//    case (Some(false), _) => a
//    case (_, Some(false)) => b
//    case _                => None
//  }
//
//  // m is a leqMonoid: is it forcing the monoidVariable holding it as a leq to have a commutative property?
//  private def commutativeForLeq(m: Monoid): Option[Boolean] = m match {
//    case _: PrimitiveMonoid          => Some(true)
//    case _: SetMonoid                => Some(true)
//    case _: BagMonoid                => Some(true)
//    case _: ListMonoid               => None
//    case mv: MonoidVariable =>
//      if (!properties.contains(mv)) {
//        return None
//      }
//      for (leq <- properties(mv).leqMonoids) {
//        if (commutativeForLeq(mFind(leq)) == Some(true))
//          return Some(true)
//      }
//      None
//    //      if (leq.map(mFind).map(commutativeForLeq).contains(Some(true)))
//    //        Some(true)
//    //      else
//    //        None
//  }
//
//  // m is a geqMonoid: is it forcing the monoidVariable holding it as a geq to NOT have a commutative property?
//  private def commutativeForGeq(m: Monoid): Option[Boolean] = m match {
//    case _: PrimitiveMonoid          => None
//    case _: SetMonoid                => None
//    case _: BagMonoid                => None
//    case _: ListMonoid               => Some(false)
//    case mv: MonoidVariable =>
//      if (!monoidGraph.contains(mv)) {
//        return None
//      }
//      for (geq <- monoidGraph(mv).geqMonoids) {
//        if (commutativeForGeq(mFind(geq)) == Some(false))
//          return Some(false)
//      }
//      None
//    //      if (geq.map(mFind).map(commutativeForGeq).contains(Some(false)))
//    //        Some(false)
//    //      else
//    //        None
//  }
//
//  private def idempotentForLeq(m: Monoid): Option[Boolean] = m match {
//    case _: MaxMonoid                => Some(true)
//    case _: MinMonoid                => Some(true)
//    case _: MultiplyMonoid           => None
//    case _: SumMonoid                => None
//    case _: AndMonoid                => Some(true)
//    case _: OrMonoid                 => Some(true)
//    case _: SetMonoid                => Some(true)
//    case _: BagMonoid                => None
//    case _: ListMonoid               => None
//    case mv: MonoidVariable =>
//      if (!properties.contains(mv)) {
//        return None
//      }
//      for (leq <- properties(mv).leqMonoids) {
//        if (idempotentForLeq(mFind(leq)) == Some(true))
//          return Some(true)
//      }
//      None
//    //      if (leq.map(mFind).map(idempotentForLeq).contains(Some(true)))
//    //        Some(true)
//    //      else
//    //        None
//    //      leq.map(mFind).map(idempotentForLeq).fold(None)((a, b) => maxOf(a, b))
//  }
//
//  private def idempotentForGeq(m: Monoid): Option[Boolean] = m match {
//    case _: MaxMonoid                => None
//    case _: MinMonoid                => None
//    case _: MultiplyMonoid           => Some(false)
//    case _: SumMonoid                => Some(false)
//    case _: AndMonoid                => None
//    case _: OrMonoid                 => None
//    case _: SetMonoid                => None
//    case _: BagMonoid                => Some(false)
//    case _: ListMonoid               => Some(false)
//    case mv: MonoidVariable =>
//      if (!properties.contains(mv)) {
//        return None
//      }
//      for (geq <- properties(mv).geqMonoids) {
//        if (idempotentForGeq(mFind(geq)) == Some(false))
//          return Some(false)
//      }
//      None
//    //
//    //      if (geqMonoids.map(mFind).map(idempotentForGeq).contains(Some(false)))
//    //        Some(false)
//    //      else
//    //        None
//    //      geq.map(mFind).map(idempotentForGeq).fold(None)((a, b) => minOf(a, b))
//  }
//
//  private def commutative(m: Monoid): Option[Boolean] = {
//    m match {
//      case _: PrimitiveMonoid          => Some(true)
//      case _: SetMonoid                => Some(true)
//      case _: BagMonoid                => Some(true)
//      case _: ListMonoid               => Some(false)
//      case mv: MonoidVariable =>
//        //        if (leq.map(mFind).map(commutativeForLeq).contains(Some(true)))
//        //          Some(true)
//        //        else if (geq.map(mFind).map(commutativeForGeq).contains(Some(false)))
//        //          Some(false)
//        //        else
//        //          None
//        logger.debug("Start C Min")
//        val myMin = if (monoidGraph.contains(mv)) monoidGraph(mv).leqMonoids.map(mFind).map(commutativeForLeq).foldLeft(None: Option[Boolean])((a, b) => maxOf(a, b)) else None
//        logger.debug("Start C Max")
//        val myMax = if (monoidGraph.contains(mv)) monoidGraph(mv).geqMonoids.map(mFind).map(commutativeForGeq).foldLeft(None: Option[Boolean])((a, b) => minOf(a, b)) else None
//        logger.debug("Done C")
//        // TODO: We now believe that min>max is not an implementation bug. Rather, there should be a point, either here or in unifyMonoids
//        // TODO: where we are missing a check and we could indeed trigger this assert, which would NOT be an implementation bug, but
//        // TODO: rather, a typing bug. The general idea is that at some point my leqs could impose some condition that is incompatible with my geqs.
//        // TODO: We believe now that this could happen, and again, it's a typing error..
//        assert(!(myMin.isDefined && myMax.isDefined && !myMax.get && myMin.get)) // min > max is an implementation bug
//        myMin.orElse(myMax.orElse(None))
//    }
//  }
//
//  private def idempotent(m: Monoid): Option[Boolean] = m match {
//    case _: MaxMonoid                => Some(true)
//    case _: MinMonoid                => Some(true)
//    case _: MultiplyMonoid           => Some(false)
//    case _: SumMonoid                => Some(false)
//    case _: AndMonoid                => Some(true)
//    case _: OrMonoid                 => Some(true)
//    case _: SetMonoid                => Some(true)
//    case _: BagMonoid                => Some(false)
//    case _: ListMonoid               => Some(false)
//    case mv: MonoidVariable =>
//      //      if (leq.map(mFind).map(idempotentForLeq).contains(Some(true)))
//      //        Some(true)
//      //      else if (geq.map(mFind).map(idempotentForGeq).contains(Some(false)))
//      //        Some(false)
//      //      else
//      //        None
//      logger.debug("Start I Min")
//      val myMin = if (monoidGraph.contains(mv)) monoidGraph(mv).leqMonoids.map(mFind).map(idempotentForLeq).foldLeft(None: Option[Boolean])((a, b) => maxOf(a, b)) else None
//      logger.debug("Start I Max")
//      val myMax = if (monoidGraph.contains(mv)) monoidGraph(mv).geqMonoids.map(mFind).map(idempotentForGeq).foldLeft(None: Option[Boolean])((a, b) => minOf(a, b)) else None
//      logger.debug("Done")
//      // TODO: We now believe that min>max is not an implementation bug. Rather, there should be a point, either here or in unifyMonoids
//      // TODO: where we are missing a check and we could indeed trigger this assert, which would NOT be an implementation bug, but
//      // TODO: rather, a typing bug. The general idea is that at some point my leqs could impose some condition that is incompatible with my geqs.
//      // TODO: We believe now that this could happen, and again, it's a typing error..
//      assert(!(myMin.isDefined && myMax.isDefined && !myMax.get && myMin.get)) // min > max is an implementation bug
//      myMin.orElse(myMax.orElse(None))
//  }
//
//}
