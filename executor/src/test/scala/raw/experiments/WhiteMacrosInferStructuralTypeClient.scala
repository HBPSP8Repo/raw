package raw.experiments

case class Per2(name: String)

@queryResult object Results

object WhiteMacrosInferStructuralTypeClient {

  case class Per3(name: String)

  def main(args: Array[String]) {

//    val person = Results.Person("Joe")
//    println("Person: " + person)

    //    WhiteMacrosInferStructuralType.typedMacro[Int]("String")
    //
    //    WhiteMacrosInferStructuralType.typedMacro[Int](1)
    //
    //
    //    case class Per4(name: String)
    //
    //    def getTypeTag[T: TypeTag](c: T) = typeOf[T]
    //    //    val c = WhiteMacrosInferStructuralType.createCaseClass()
    //    //    val c = WhiteMacrosInferStructuralType.createInstace()
    //    val p1 = WhiteMacrosInferStructuralType.createCase()
    //    val t1: Type = getTypeTag(p1)
    //
    //    val p2 = new Per2("Joan")
    //    val t2: Type = getTypeTag(p2)
    //
    //    val p3 = new Per3("Joan")
    //    val t3: Type = getTypeTag(p3)
    //
    //    val p4 = new Per4("Joan")
    ////    val t4: Type = getTypeTag(p4)
    //
    //    println("Type: " + t1)
    //    println("Members: " + t1.members)
    //    println("Decls: " + t1.decls)
    //    println("Baseclasses: " + t1.baseClasses)
    //    println("Companion: " + t1.companion)
    //    println("Result: " + p1 + " " + p1.name)
    //    println("Result: " + p2 + " " + p2.name)
  }
}
