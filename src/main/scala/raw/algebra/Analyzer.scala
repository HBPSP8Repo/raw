package raw
package algebra

import org.kiama.attribution.Attribution

class Analyzer(tree: LogicalAlgebra.Algebra, world: World) extends Attribution {

  import org.kiama.==>
  import org.kiama.attribution.Decorators
  import LogicalAlgebra._

  /** Decorators on the tree.
    */
  lazy val decorators = new Decorators(tree)
  import decorators.{chain, Chain}

  /** ... */


}
