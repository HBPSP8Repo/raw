package raw.algebra

import raw._

/** ExpressionType
 */

sealed abstract class ExpressionType

case class Primitive(p: PrimitiveType) extends ExpressionType

case class Record(r: RecordType) extends ExpressionType
