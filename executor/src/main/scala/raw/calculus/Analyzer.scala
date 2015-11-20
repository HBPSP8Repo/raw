package raw
package calculus

import org.kiama.attribution.Attribution

trait Analyzer extends Attribution {

  def tree: Calculus.Calculus

  val queryString: String

}
