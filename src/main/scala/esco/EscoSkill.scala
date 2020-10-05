package esco

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import akka.http.scaladsl.unmarshalling.Unmarshal
import spray.json._
import utils.{EscoJsonUtils, HttpTools, Languages, QueueingHttpsTools}

import scala.collection.mutable.{Set => MutableSet}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source


case class Resource(resource_uri: String, title: String, uri: String)
case class ResourceAddress(resource_uri: String, uri: String)

case class NodeConnectionInformation(self: Resource,
                                     //relation types https://ec.europa.eu/esco/api/doc/esco_api_doc.html#list-view-skill
                                     isTopConceptInScheme: Option[Seq[Resource]],
                                     hasTopConcept: Option[Seq[Resource]], //Because of taxonomy class
                                     isInScheme: Option[Seq[Resource]], //e.g. "ESCO transversal skill groups" or "ESCO skills" - optional because of the taxonomy class
                                     hasSkillType: Option[Seq[Resource]], //it it's type (className) is Skill. e.g. skill, knowledge
                                     broaderHierarchyConcept: Option[Seq[Resource]],
                                     broaderConcept: Option[Seq[Resource]],
                                     narrowerConcept: Option[Seq[Resource]],
                                     broaderSkillGroup: Option[Seq[Resource]],
                                     broaderSkill: Option[Seq[Resource]],
                                     narrowerSkill: Option[Seq[Resource]],
                                     hasEssentialSkill: Option[Seq[Resource]],
                                     hasOptionalSkill: Option[Seq[Resource]],
                                     isOptionalForSkill: Option[Seq[Resource]],
                                     isEssentialForSkill: Option[Seq[Resource]],
                                     isEssentialForOccupation: Option[Seq[Resource]],
                                     isOptionalForOccupation: Option[Seq[Resource]],
                                    ) {
  def getNarrowerConnections: Option[Seq[Resource]] = {
    Option((narrowerConcept ++ narrowerSkill ++ hasEssentialSkill ++ hasOptionalSkill).flatten.toSeq).filter(_.nonEmpty)
  }
}

case class TaxonomyConnections(hasTopConcept: List[Resource], self: Resource)

case class Description(enDescription: String)

case class PreferredLabel(enLabel: String, huLabel: String) {
  def getLabel(lang: Languages): String = lang match {
    case Languages.EN => enLabel
    case _ => huLabel
  }
}

case class AlternativeLabel(enLabels: Option[Seq[String]], huLabels: Option[Seq[String]]) {
  def getLabelList(lang: Languages): Seq[String] = lang match {
    case Languages.HU => getList(huLabels)
    case _ => getList(enLabels)
  }

  private def getList(optList: Option[Seq[String]]): Seq[String] = optList match {
    case Some(list) => list
    case None => Seq()
  }
}

//ESCO's Taxonomy class
case class Taxonomy(relations: TaxonomyConnections, //_links
                    classId: String,
                    className: String,
                    preferredLabel: PreferredLabel,
                    title: String,
                    uri: String)

//Represents here both an ESCO Skill and an ESCO Concept class; also works for a Taxonomy (ConceptScheme) Node
case class Node(title: String,
                className: String,
                uri: String,
                description: Option[Description],
                preferredLabel: PreferredLabel,
                alternativeLabel: Option[AlternativeLabel],
                relations: NodeConnectionInformation //_links
                ) {

  override def equals(obj: Any): Boolean = obj.asInstanceOf[Node].getId == this.getId

  def equalsResource(obj: Resource): Boolean =
    uri == obj.uri && title == obj.title

  def getId: String = uri.split("/").last
}

object NodeUtils extends EscoHttp {
  private var accumulator: MutableSet[Node] = _

  def getNarrowerNodes(node: Node): MutableSet[Node] = {
    accumulator = MutableSet()
    fillAccumulator(node)
    accumulator
  }

  private def fillAccumulator(node: Node): Unit = {
    if(accumulator.add(node)) {
      node.relations.getNarrowerConnections.foreach {
        _.foreach(
          innerNode => {
            if(!accumulator.exists(_.equalsResource(innerNode))) {
              fillAccumulator(getNode(innerNode.resource_uri, Languages.EN))
            }
          }
        )
      }
    }
  }

  /*private def getNarrowerNodesRecursively(node: Node, accumulator: Seq[Node] = Seq()): Seq[Node] = node.relations.getNarrowerConnections match {
      case None => accumulator :+ node
      case Some(resources) => resources.flatMap(
        innerNode => {
          println(s"Getting node: ${innerNode.title}")
          getNarrowerNodesRecursively(getNode(innerNode.resource_uri, Languages.EN), accumulator)
        }
      ) :+ node
    }*/
}

case class NodeList(skills: Seq[Node]) {
  def getPreferredLabelList(lang: Languages): Seq[String] = skills.map(_.preferredLabel.getLabel(lang))

  def getAlternativeLabel(lang: Languages): Seq[String] = skills.flatMap(
    skill => skill.alternativeLabel match {
      case Some(alterLabel) => alterLabel.getLabelList(lang)
      case None => Seq()
    }
  )

  def getLabelList(lang: Languages): Seq[String] = getPreferredLabelList(lang) ++ getAlternativeLabel(lang)
}

object NodeList extends EscoJsonUtils {
  def writeToJsonFile(skillList: NodeList, file: File): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(skillList.toJson.toString)
    bw.close()
  }

  def writeToCsvFile(skillList: NodeList, path: String, separator: String): Unit = {
    val bw = Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8)
    val header = s"Id${separator}EnText${separator}HunText"
    bw.write(header)
    bw.write(System.lineSeparator)
    skillList.skills.foreach(
      skill => {
        bw.write(s"${skill.getId}$separator${skill.preferredLabel.enLabel}$separator${skill.preferredLabel.huLabel}")
        bw.write(System.lineSeparator)
        skill.alternativeLabel match {
          case None =>
          case Some(alterLab) =>
            val alterLabels = alterLab.enLabels.getOrElse(Seq.empty).zipAll(alterLab.huLabels.getOrElse(Seq.empty), "null", "null")
            alterLabels.foreach(labelPair => {
              bw.write(s"${skill.getId}$separator${labelPair._1}$separator${labelPair._2}")
              bw.write(System.lineSeparator)
            })
        }
      }
    )
    bw.close()
  }

  def fromJsonFile(path: String): NodeList = {
    val bufferedSource = Source.fromFile(path)
    val skillList = bufferedSource.getLines.mkString.parseJson.convertTo[NodeList]
    bufferedSource.close
    skillList
  }
}

case class SearchResult(count: Int, concepts: Seq[ResourceAddress], offset: Int, total: Int)

trait EscoSkill extends EscoJsonUtils with HttpTools with EscoService {

  final val SKILL_URI = "/skill"
  final val SKILL_URL = s"$API_URL$RESOURCE_URI$SKILL_URI"
  final val SKILL_LIST_URL = "http://data.europa.eu/esco/concept-scheme/skills"
}

trait EscoHttp extends EscoService with EscoSkill {

  def getSkillTitleList(langCode: Languages): Future[List[String]] = {
    val url = getSkillListUrl(langCode)
    val responseFuture = getInFuture(s"$API_URL$RESOURCE_URI$URI_PROP$url")
    responseFuture flatMap {
      response =>
        Unmarshal(response.entity).to[Taxonomy] map {
          skillListResponse =>
            skillListResponse.relations.hasTopConcept map {
              self => self.title
            }
        }
    }
  }

  def getSkillList(langCode: Languages): Future[Taxonomy] = {
    val url = getSkillListUrl(langCode)
    val responseFuture = getInFuture(s"$API_URL$RESOURCE_URI$URI_PROP$url")
    responseFuture flatMap {
      response => Unmarshal(response.entity).to[Taxonomy]
    }
  }

  def getSkillListUrl(langCode: Languages): String = langCode match {
    case Languages.HU => s"$SKILL_LIST_URL$LANGUAGE_PROP${Languages.HU.name}"
    case _ => s"$SKILL_LIST_URL$LANGUAGE_PROP${Languages.EN.name}"
  }

  def getSkill(uri: String, langCode: Languages): Future[Node] = {
    val url = s"$SKILL_URL$URI_PROP$uri$LANGUAGE_PROP${langCode.name}"
    getNodeInFuture(url, langCode)
  }

  def getNodeInFuture(uri: String, langCode: Languages): Future[Node] = {
    val responeseFuture = getInFuture(uri)
    responeseFuture flatMap {
      response => Unmarshal(response.entity).to[Node]
    }
  }

  def getNode(uri: String, langCode: Languages): Node = {
    Await.result(getNodeInFuture(uri, langCode), Duration.apply(60, SECONDS))
  }

  def getSearchResultInFuture(uri: String, langCode: Languages): Future[SearchResult] = {
    val responeseFuture = getInFuture(uri)
    responeseFuture flatMap {
      response => Unmarshal(response.entity).to[SearchResult]
    }
  }

  def getSearchResult(uri: String, langCode: Languages): SearchResult = {
    Await.result(getSearchResultInFuture(uri, langCode), Duration.apply(5, SECONDS))
  }

  def getListOfSkills(lang: Languages): Future[List[Node]] = {
    getSkillList(lang).flatMap(
      skillList => Future.sequence(skillList.relations.hasTopConcept map {
        individualSkill => getSkill(individualSkill.uri, lang)
      })
    )
  }
}

class EscoQueuingHttp(queueSize: Int = 32) extends EscoHttp {

  def queueingHttpTools(url: String) = new QueueingHttpsTools(url, queueSize)

  val httpQuery: QueueingHttpsTools = queueingHttpTools(ESCO_HOST)

  override def getSkill(uri: String, langCode: Languages): Future[Node] = {
    val url = s"$API_URI$RESOURCE_URI$SKILL_URI$URI_PROP$uri$LANGUAGE_PROP${langCode.name}"
    val responeseFuture = httpQuery.getInFuture(url)
    responeseFuture flatMap {
      response => Unmarshal(response.entity).to[Node]
    }
  }
}
