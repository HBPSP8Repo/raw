//package raw
//package calculus
//
//import com.typesafe.scalalogging.LazyLogging
//import raw.World.RecordAttributesVarMap
//
//// TODO: Rename filename
//trait AttributesGraph extends LazyLogging {
//
//  case class ConcatProperties(atts: Seq[AttrType], isComplete: Boolean)
//
//  private val concatProperties = scala.collection.mutable.HashMap[ConcatAttributes, ConcatProperties]()
//
//  case class ConcatSlot(prefix: String, t: Type)
//  type ConcatSlots = Seq[ConcatSlot]
//
//  private val concatSlots = scala.collection.mutable.HashMap[ConcatAttributes, ConcatSlots]()
//
//  private val recAttsVarMap = new RecordAttributesVarMap()
//
//  private def getConcatProperties(c: ConcatAttributes): ConcatProperties = {
//    if (!concatSlots.contains(c)) {
//      return ConcatProperties(Seq(), false)
//    }
//
//    val slots = concatSlots(c)
//
//    def getResolvedSlots: ConcatSlots = {
//      val resolvedSlots = scala.collection.mutable.ListBuffer[ConcatSlot]()
//      for (s <- slots) {
//        find(s.t) match {
//          case ResolvedType(RecordType(_: AttributesVariable)) =>
//            // Return early since we cannot know the sequence of attributes from the beginning
//            return resolvedSlots.to
//          case ResolvedType(RecordType(c1: ConcatAttributes)) =>
//            val props1 = getConcatProperties(c1)
//            resolvedSlots += props1.atts
//            if (!props1.isComplete) {
//              return resolvedSlots.to
//            }
//          case t1 =>
//            resolvedSlots += ((p, inner2))
//        }
//      }
//      resolvedSlots.to
//    }
//
//      def getAtts(inners: Seq[(Option[Pattern], Type)]): Seq[AttrType] = {
//        val usedIdns = scala.collection.mutable.Set[Idn]()
//
//        def uniqueIdn(i: Idn, j: Int = 0): Idn = {
//          val ni = if (j == 0) i else s"${i}_$j"
//          if (usedIdns.contains(ni))
//            uniqueIdn(i, j + 1)
//          else {
//            usedIdns += ni
//            ni
//          }
//        }
//
//        val atts = scala.collection.mutable.MutableList[AttrType]()
//        for (inner <- inners) {
//          // TODO: find(inner) ???
//          inner match {
//            case (_, ResolvedType(RecordType(Attributes(atts1)))) =>
//              for (att <- atts1) {
//                atts += AttrType(uniqueIdn(att.idn), att.tipe)
//              }
//            case (Some(PatternIdn(IdnDef(idn))), inner1)          =>
//              atts += AttrType(uniqueIdn(idn), inner1)
//            case (None, inner1)                                   =>
//              // TODO: This _1_2 convention (in case of uniqueIdn) isn't...great
//              atts += AttrType(uniqueIdn(s"_${atts.length + 1}"), inner1)
//          }
//        }
//
//        logger.debug(s"atts is $atts")
//        assert(atts.map(_.idn).toSet.size == atts.map(_.idn).length) // TODO: Ensure that there are no overlapping idn names
//
//        atts.to
//      }
//
//
//      val inners = getInners(gs)
//      if (inners.length == gs.length) {
//        // We solved all generators to actual types.
//        // Let's now check the attributes.
//        val atts = getAtts(inners)
//        Some(Attributes(atts))
//        // TODO: The following optimization is not supported for now
//        //      } else if (inners.nonEmpty) {
//        //        // We at least solved smtg from the beginning
//        //        ConcatAttributes(atts, ...)
//      } else {
//        None
//      }
//    }
//
//
//  // this checks the types in concat attributes and tries to resolve them into seq of attr types if possible
//    // applying also the renaming rules. THat seq of attr types stays fixed forever
//
//    // TODO: uses concatDefinition to compute new concatProperties
//
//
//
//  // concat attributes contained in another
//  // suppose the inner one is resolved (to a fixed size thing)
//  // then the outer one may do some progress as well
//  // ok.
//  // that's where my other impl may help
//  // but what is the root then?
//  // well if concat attributes itself had nothing, could be a new concat attributes just for the sake of it
//  // ok, gotcha
//
//
//  def addConcatAttributes(c: ConcatAttributes, slots: ConcatSlots) = {
//    assert(!concatProperties.contains(c))
//    assert(!concatSlots.contains(c))
//    concatSlots.put(c, slots)
//    concatProperties.put(c, getConcatProperties(c))
//    // do i have to trigger validation here?
//    // i can't see why...
//  }
//
//  def unifyAttributes(a: Attributes, b: Attributes): Boolean =
//
//    // in the case of concat attributes
//    // this needs to get ALL members of the group - att vars, etc etc -
//    // and validate against them all
//
//    // does simple union, since validate and propagate will compute the beginnings/endings separately
//
//    ???
//
//    // i can indeed handle all attributes here, but the only hard work is for concats
//    // and after each unify of attributes
//    // i check if the map is still valid
//    // what does that mean?
//
//  // suppose the inner type of a concat has been resolved - by some other random reason; nothing to do w/ concat unification btw!!! so no real reason to put all atts here -
//  // then, we can update its concat properties because maybe we know some beginnings
//  // so in turn, we must trigger an update to all record types just in case one becomes totally valid
//  // and so forth until things stabilize
//  // can things ever fail?
//  // say i have a concat unified with another concat
//  // when i know the beginnings for one of them, i also know the beginnings for the other
//  // there is no doubt there
//  // if a concat is unified with a full blown record, i check its begin definitions; if compatible so far, it's a go; otherwise, it's a fail
//  // ah, things can fail, during validate and propagate, if i
//  // well the map is always consistent and always knows the most
//  // so what is there unified with, has to work
//  // learning more cannot affect my prev decision to unify
//  // sure; that would mean the unify attributes would fail
//  // but how could learning more about a concat affect a prev decision?
//  // it can't really.
//  // but, how do i handle the unification of two concats? (ah!)
//  // how do i merge their sequences?
//  // ah, maybe here's the thing?
//  // well, i should make a new one that contains both (or them all) and make that the root
//  // but it's hard; so i just put them all into a "pot" and check them all; that's similar to the var map thing
//  // any of them can be the root; but in practice, i must check against them all to let it go through
//
//
//  // select x.age, * from x in xs, ys
//  // so, * is a concat of 'x' and 'y' and also 'x' includes at least 'age'
//  // ah, here the issue is that i still dont know about x and y :)
//  // so actually the concat sig is: * is a concat of the inner types of xs and ys, and also x (the inner of xs) includes at least 'age'
//  //
//  // actually, i am confused..
//  // this makes no sense
//  // so, the SELECT FROM part contraints have to be added BEFORE the projection
//  // so we should be fine, no?
//  // yes, ok, now all good there.
//
//  // so, create a super record type?
//  // that includes a bunch of variable things together... all 'anded'
//  // i can reuse the group code implementation
//  // that is fine
//  // but then again, what is the propagation issue?
//  // if i narrowed things further
//  // then other guys would be narrowed as well, right?
//  // but walk at the end takes care of all this, no?
//  // sure; but i also need to walk inside here...
//  // ...i need to make sure all variable constraints all hold before unifying
//  // this is a slightly different thing, no?
//
//  // this is a recursive function that is going to unify types and all; so we should be alright
//
//  // so the only issue is that i dont have a single type that includes att variables + sets of concat atts
//  // if i had that, i'd make that the root
//  // it it probably safer to add that though
//
//
//  def validateAndPropagateConcatAttributes(): Boolean = {
//
//    so i think this is just propagate; it is not validate
//    i think
//    no; it does validate
//
//
//    var changed = false
//    do {
//      for ((c, props) <- concatProperties) {
//
//        // check if concat properties are better defined
//        val nprops = getConcatProperties(c)
//        if (props != nprops) {
//          concatProperties.put(c, nprops)
//          changed = true
//        }
//
//        // if so, check if it is compatible with all in the same group
//        recAttsVarMap.get(c) match {
//          case Some(g) =>
//            for (r <- g.elements) {
//              // check if compatible
//              // if not, return early with false
//              // TODO: if the newly-found first elements are compatible; how can they not be?  or is it instead about changing their properties as well?
//              // could it be that i became fully defined? if root of two atts variable.. still ok
//              // but now variable is atts variable plus sets of alternative beginnings, right? or of concats...
//              ???
//
//            }
//          case None =>
//        }
//
//      }
//    } while (changed)
//
//    true
//  }
//
//    // for every concat attributes
//    // get again its new concat properties
//    // it it changed, store it
//    // then, go through every element in its group; if not ok, return false
//
//    // repeat while changed == true
//
//  /*
//  this map would contain all the concat attributes
//  theyd be added here at creation time
//
//  is it enough to know the concat attributes and what they use?
//  then i just go through all the concat attributes - after each unify -
//  and check if what they used has changed or is more specified
//
//  if so, i specify it further - i always do that - and then check if all the concat attributes are still ok
//  because this concat attributes can itself be contained in other concat attributes
//  where does it end?
//  when i replace a given concat attributes by a fixed size attribute
//  at that point i can remove it from the map
//
//
//
//  */
//
//}
//
//  /*
//
//
//  at bootstrap,
//  i create a concat attributes and say:
//  this concat attributes contains these guys, in a given order.
//  a relation:
//  case class ConcatContains(the concat itself, Seq[(optional user ID, type)])
//  this is NOT a relation; this is the definition of concat
//
//  when i unify any record type,
//  i may end up resolving some of the inner types here
//  so that if i walk that sequence of types again, i may end up with a more resolved - or even, fully resolved - type.
//  and if it doesn't, it's because that particular unification isn't possible, so it must fail.
//
//  how can an unification here break something somewhere else?
//  it's if, from a concat, i can derive the "beginning of a record" and notice that a given beginning breaks smtg else
//  that is unified but expects a != beginning...
//
//  actually, this is even more nuts, because any type unification can influence this
//  suppose i do nothing but always unify
//  at the end.. well, i know the thing is broken but cannot say what was the 1st breakage cause.
//
//  =====
//
//  otherwise, i have to check the sanity of each and every concat attributes after each and every unify
//
//
//  \xs -> select * from xs
//  a := \xs -> select x.age, * from x in xs
//  b := \xs, ys -> select x.age, y.name, * from x in xs, y in ys where x.foo = y.foo
//
//  type of * is ConcatAttributes
//  that ConcatAttributes is the concat of x and y (the inner of xs and ys)
//  x and y should be remembered since they are 'prefixes' and may be useful
//  but, in a more general setting, i should call it 'prefix'
//
//  now, every time i unify any type,
//  i should "rewalk" the concat to see if it got narrower.
//  if it got narrower from the beginning elements...
//  i have to see if the things it is union'ed to still hold.
//  if not, this last unification failed
//
//  ---
//  there is also
//  ConcatProperties(begin: Seq[AttrType])
//
//  ----
//
//  how do i validate?
//
//
//
//
//
//   */
//
//  //when i unify, i can affect other guys, and this info needs to propagate
////
////  ok, a concat attributes is a placeholder for attributes being concatenated in a given order
////
////
////  of course if the inner guy changes, we should know
////  and, wherever we are used, we must check whether we are still valid.
////
////  in turn, we can also be contained in other concat attributes, and hence, affect those
////
////  what is the right way to do this?
////  think from 1st principles
////
////  the issue - or feature - is that concat attributes unify with other concat attributes
////
////  suppose i ignore all this
////  i unify... i simply unify
////  then, after unification, i walk all records and see if they are coherent
////  if not, i blow up
////  the order here is concat attributes contains these other concat attributes but also, it is contained by these other concat attributes
////
////  the compatibility check is:
////    for all concat attributes,
////
////  [ btw, this is really only about concat attributes??? ]
////
////
////  [ even a concat w/ fixed attributes: under renaming, how can i know things are safe/valid? say i have age and age_1 on the final record and age on concat: which one do i unify? ]
////
////  \xs -> select * from xs
////  /xs,ys -> select * from xs, ys
////
//
