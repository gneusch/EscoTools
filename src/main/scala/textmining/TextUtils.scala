package textmining

object TextUtils {

  def wordPairs(corpus: Seq[String]) = corpus flatMap { a =>
      corpus flatMap { b =>
        if(a!=b) Option(a, b)
        else None
    }
  }

  def genBiGrams(wordList: Seq[String]): Seq[(String,String)] = wordList match {
    case Seq(x) => Seq()
    case Seq(head, tail@_*) => (head, tail.head) +: genBiGrams(tail)
    case _ => Seq()
  }

  def genTriGrams(wordList: Seq[String]): Seq[(String, String, String)] = wordList match {
    case Seq(first, second) => Seq()
    case Seq(first, second, tail@_*) => (first, second, tail.head) +: genTriGrams(second +: tail)
    case _ => Seq()
  }

  def strListToLower(wordList: Seq[String]) = wordList map { _.toLowerCase }

}
