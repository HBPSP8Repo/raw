package raw

sealed abstract class RawData {
  def id: Int
}

case class RawInt(v: Int, id: Int) extends RawData

