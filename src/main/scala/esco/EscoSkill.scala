package esco

import java.io.{BufferedWriter, File, FileWriter}
import akka.http.scaladsl.unmarshalling.Unmarshal
import utils.{EscoJsonUtils, HttpTools, Languages, QueueingHttpsTools}

import scala.concurrent.Future
import spray.json._
import scala.io.Source


case class Self(resource_uri: String, title: String, uri: String)

case class TopConceptList(hasTopConcept: List[Self], self: Self)

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

case class SelfConceptList(links: TopConceptList, classId: String, className: String, preferredLabel: PreferredLabel, title: String, uri: String)

case class Skill(className: String, uri: String, title: String, description: Option[Description], preferredLabel: PreferredLabel, alternativeLabel: Option[AlternativeLabel]) {
  def getId: String = uri.split("/").last
}

case class SkillList(skills: Seq[Skill]) {
  def getPreferredLabelList(lang: Languages): Seq[String] = skills.map(_.preferredLabel.getLabel(lang))

  def getAlternativeLabel(lang: Languages): Seq[String] = skills.flatMap(
    skill => skill.alternativeLabel match {
      case Some(alterLabel) => alterLabel.getLabelList(lang)
      case None => Seq()
    }
  )

  def getLabelList(lang: Languages): Seq[String] = getPreferredLabelList(lang) ++ getAlternativeLabel(lang)
}

object SkillList extends EscoJsonUtils {
  def writeToJsonFile(skillList: SkillList, file: File): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(skillList.toJson.toString)
    bw.close()
  }

  def writeToCsvFile(skillList: SkillList, file: File): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))
    skillList.skills.foreach(
      skill => {
        bw.write(s"${skill.getId},${skill.preferredLabel.enLabel},${skill.preferredLabel.huLabel}")
        bw.write(System.lineSeparator)
        skill.alternativeLabel match {
          case None =>
          case Some(alterLab) =>
            val alterLabels = alterLab.enLabels.getOrElse(Seq.empty).zipAll(alterLab.huLabels.getOrElse(Seq.empty), "null", "null")
            alterLabels.foreach( labelPair => {
              bw.write(s"${skill.getId},${labelPair._1},${labelPair._2}")
              bw.write(System.lineSeparator)
            })
        }
      }
    )
    bw.close()
  }

  def fromJsonFile(path: String): SkillList = {
    val bufferedSource = Source.fromFile(path)
    val skillList = bufferedSource.getLines.mkString.parseJson.convertTo[SkillList]
    bufferedSource.close
    skillList
  }
}

trait EscoSkill extends EscoJsonUtils with HttpTools with EscoService {

  final val SKILL_URI = "/skill"
  final val SKILL_URL = s"$API_URL$RESOURCE_URI$SKILL_URI"
  final val SKILL_LIST_URL = "http://data.europa.eu/esco/concept-scheme/skills"
}

class EscoSkillHttp extends EscoService with EscoSkill {

  def getSkillTitleList(langCode: Languages): Future[List[String]] = {
    val url = getSkillListUrl(langCode)
    val responseFuture = getInFuture(s"$API_URL$RESOURCE_URI$URI_PROP$url")
    responseFuture flatMap {
      response =>
        Unmarshal(response.entity).to[SelfConceptList] map {
          skillListResponse =>
            skillListResponse.links.hasTopConcept map {
              self => self.title
            }
        }
    }
  }

  def getSkillList(langCode: Languages): Future[SelfConceptList] = {
    val url = getSkillListUrl(langCode)
    val responseFuture = getInFuture(s"$API_URL$RESOURCE_URI$URI_PROP$url")
    responseFuture flatMap {
      response => Unmarshal(response.entity).to[SelfConceptList]
    }
  }

  def getSkillListUrl(langCode: Languages): String = langCode match {
    case Languages.HU => s"$SKILL_LIST_URL$LANGUAGE_PROP${Languages.HU.name}"
    case _ => s"$SKILL_LIST_URL$LANGUAGE_PROP${Languages.EN.name}"
  }

  def getSkill(uri: String, langCode: Languages): Future[Skill] = {
    val url = s"$SKILL_URL$URI_PROP$uri$LANGUAGE_PROP${langCode.name}"
    val responeseFuture = getInFuture(url)
    responeseFuture flatMap {
      response => Unmarshal(response.entity).to[Skill]
    }
  }

  def getListOfSkills(lang: Languages): Future[List[Skill]] = {
    getSkillList(lang).flatMap(
      skillList => Future.sequence(skillList.links.hasTopConcept map {
        individualSkill => getSkill(individualSkill.uri, lang)
      })
    )
  }
}

class EscoQueuingSkillHttp(queueSize: Int = 32) extends EscoSkillHttp {

  def queueingHttpTools(url: String) = new QueueingHttpsTools(url, queueSize)

  val httpQuery: QueueingHttpsTools = queueingHttpTools(ESCO_HOST)

  /*def getSkill(uri: String): Future[Skill] = {
    val responeseFuture = httpQuery.getInFuture(uri)
    responeseFuture flatMap {
      response => Unmarshal(response.entity).to[Skill]
    }
  }*/

  override def getSkill(uri: String, langCode: Languages): Future[Skill] = {
    val url = s"$API_URI$RESOURCE_URI$SKILL_URI$URI_PROP$uri$LANGUAGE_PROP${langCode.name}"
    val responeseFuture = httpQuery.getInFuture(url)
    responeseFuture flatMap {
      response => Unmarshal(response.entity).to[Skill]
    }
  }
}
