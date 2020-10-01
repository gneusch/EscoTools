package textmining

import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatestplus.junit.AssertionsForJUnit
import textmining.TextUtils._

class TextUtilsTest extends AssertionsForJUnit {

  var initText:List[String] = _

  @Before def init(): Unit = {
    initText = ("Bare skin is my wrinkled sack " +
      "When hot Apollo humps my back " +
      "When Jack Frost grabs me in these rags " +
      "I wrap my legs with burlap bags").split(" ").toList
  }

  @Test def genBiGramTest = {
    val biGramOfInitText = genBiGrams(initText)
    val biGramExpected = List(
      ("Bare","skin"), ("skin","is"), ("is","my"), ("my","wrinkled"), ("wrinkled","sack"), ("sack","When"), ("When","hot"), ("hot","Apollo"),
      ("Apollo","humps"), ("humps","my"), ("my","back"), ("back","When"), ("When","Jack"), ("Jack","Frost"), ("Frost","grabs"), ("grabs","me"),
      ("me","in"), ("in","these"), ("these","rags"), ("rags","I"), ("I","wrap"), ("wrap","my"), ("my","legs"), ("legs","with"), ("with","burlap"),
      ("burlap","bags")
    )
    assertEquals(biGramExpected, biGramOfInitText)
  }

  @Test def genTriGramTest = {
    val triGramOfInitText = genTriGrams(initText)
    val triGramExpected = List(
      ("Bare","skin","is"), ("skin","is","my"), ("is","my","wrinkled"), ("my","wrinkled","sack"), ("wrinkled","sack","When"), ("sack","When","hot"),
      ("When","hot","Apollo"), ("hot","Apollo","humps"), ("Apollo","humps","my"), ("humps","my","back"), ("my","back","When"), ("back","When","Jack"),
      ("When","Jack","Frost"), ("Jack","Frost","grabs"), ("Frost","grabs","me"), ("grabs","me","in"), ("me","in","these"), ("in","these","rags"),
      ("these","rags","I"), ("rags","I","wrap"), ("I","wrap","my"), ("wrap","my","legs"), ("my","legs","with"), ("legs","with","burlap"), ("with","burlap","bags")
    )
    assertEquals(triGramExpected, triGramOfInitText)
  }

}
