package raw.algebra

import raw._

/** ExpressionType
 */

sealed abstract class ExpressionType

/** PrimitiveType
 */

sealed abstract class PrimitiveType extends ExpressionType
case object BoolType extends PrimitiveType
case object StringType extends PrimitiveType
case object FloatType extends PrimitiveType
case object IntType extends PrimitiveType

/** RecordType
 */

case class Attribute(name: String, expressionType: ExpressionType)
case class RecordType(atts: List[Attribute]) extends ExpressionType