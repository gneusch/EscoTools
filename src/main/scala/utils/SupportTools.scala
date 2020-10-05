package utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.io.Source
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

  def writeListToFile[T](list: Seq[T], path: String, separator: String = System.lineSeparator): Unit = {
    val bw = Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8)
    list.foreach(
      l => bw.write(s"${l.toString}$separator")
    )
    bw.close()
  }

  def readFile(path: String): Seq[String] = {
    val source = Source.fromFile(path)
    val lines = source.getLines.toList
    source.close()
    lines
  }
}
