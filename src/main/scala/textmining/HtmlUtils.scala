package textmining

object HtmlUtils {
  //https://learning.oreilly.com/library/view/learning-scala/9781449368814/ch09.html
  def removeMarkup(input: String) = {
       input
         .replaceAll("""</?\w[^>]*>"""," ")
         .replaceAll("<.*>"," ")
  }
}
