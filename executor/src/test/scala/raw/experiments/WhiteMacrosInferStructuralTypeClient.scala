package raw.experiments

object WhiteMacrosInferStructuralTypeClient {
  def main(args: Array[String]) {
    val c = WhiteMacrosInferStructuralType.createCase()
    println("Result: " + c)
    println("Fields: " + c.name)
  }
}
