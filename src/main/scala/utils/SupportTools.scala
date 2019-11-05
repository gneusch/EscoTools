package utils

import scala.util.Try

sealed trait Languages { val name: String }
object Languages {
  case object EN extends Languages { val name = "en" }
  case object HU extends Languages { val name = "hu" }
  val values = List(EN, HU)
}


trait SupportTools {
  def tryToInt( int: String ): Option[Int] = Try( int.toInt ).toOption
  def tryToDouble( double: String ): Option[Double] = Try( double.toDouble ).toOption
}
