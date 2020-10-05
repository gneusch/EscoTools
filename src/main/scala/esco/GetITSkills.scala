package esco

import utils.{Languages, SupportTools}
import spray.json._

object GetITSkills extends App with EscoHttp with SupportTools {

  //getITSepcificEscoNodes("ictSkills.jl")
  reortObjectMembersFromFile("ictSkills.jl.bak3", "ictSkills.jl")

  def reortObjectMembersFromFile(fromPath: String, toPath: String): Unit = {
    val nodes = readFile(fromPath).map(_.parseJson.convertTo[Node])
    val distinctNodes = nodes.foldLeft(Seq.empty[Node])(
      (acc, node) => {
        if(acc.exists(_.equals(node))) {
          acc
        } else {
          acc :+ node
        }
      }
    )
    writeListToFile(distinctNodes.map(_.toJson.compactPrint), toPath)
    println(s"Done. Printed: ${distinctNodes.length} skills into file.")
  }

  def getITSepcificEscoNodes(path: String): Unit = {
    val digitCompetenciesNodeUrl = "https://ec.europa.eu/esco/api/resource/concept?uri=http://data.europa.eu/esco/skill/aeecc330-0be9-419f-bddb-5218de926004"

    val digitCompetenciesNode = getNode(digitCompetenciesNodeUrl, Languages.EN)

    val allDigitCompElement = NodeUtils.getNarrowerNodes(digitCompetenciesNode).toSeq //allDigitCompElement.foreach(node => println(node.toJson.compactPrint))

    val skillIctGroupsSearchUrl = "https://ec.europa.eu/esco/api//resource/concept?isInScheme=http://data.europa.eu/esco/concept-scheme/skill-ict-groups"

    val skillIctGroupsSearchResults = getSearchResult(skillIctGroupsSearchUrl, Languages.EN)

    val nodesFromSearch = skillIctGroupsSearchResults.concepts.flatMap(
      address => {
        val firstChildNode = getNode(address.resource_uri, Languages.EN)
        firstChildNode +: NodeUtils.getNarrowerNodes(firstChildNode).toSeq
      }
    )

    val skillIctGroupsTaxonomyUrl = "https://ec.europa.eu/esco/api/resource/taxonomy?uri=http://data.europa.eu/esco/concept-scheme/skill-ict-groups"

    val skillIctGroupsTaxonomyNode = getNode(skillIctGroupsTaxonomyUrl, Languages.EN)

    val allIctGroupsNodes = skillIctGroupsTaxonomyNode.relations.hasTopConcept.get.flatMap(
      topConceptResource => {
        val topConceptNode = getNode(topConceptResource.resource_uri, Languages.EN)
        topConceptNode +: NodeUtils.getNarrowerNodes(topConceptNode).toSeq
      }
    )

    val allICTSkills = (allDigitCompElement ++ nodesFromSearch ++ allIctGroupsNodes).distinct.map(_.toJson.compactPrint)
    writeListToFile(allICTSkills, path)
    println(s"Done. Printed: ${allICTSkills.length} skills into file.")
  }

}
