package esco

import utils.Languages
import spray.json._

object GetITSkills extends App with EscoHttp {

  val digitCompetenciesNodeUrl = "https://ec.europa.eu/esco/api/resource/concept?uri=http://data.europa.eu/esco/skill/aeecc330-0be9-419f-bddb-5218de926004"

  val digitCompetenciesNode = getNode(digitCompetenciesNodeUrl, Languages.EN)

  val allDigitCompElement = NodeUtils.getNarrowerNodes(digitCompetenciesNode)

  allDigitCompElement.foreach(node => println(node.toJson.compactPrint))
}
