package raw.mdcatalog

import raw.calculus.Calculus.Exp

sealed abstract class View(plan: Exp)

case class MaterializedView(plan: Exp, source: Source) extends View(plan)

case class VirtualView(plan: Exp) extends View(plan)