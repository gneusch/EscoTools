package utils

import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import esco._
import jobposting.JobPosting
import org.joda.time.{DateTime => JodaDateTime}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.breakOut
import scala.util.{Failure, Success, Try}

trait JsonUtils extends CollectionFormats with AdditionalFormats with StandardFormats {
  def getOptionalField[T: JsonReader](fields: Map[String, JsValue], key: String): Option[T] = {
    fields.get(key) match {
      case Some(jsValue) => Some(jsValue.convertTo[T])
      case None => None
    }
  }

  implicit val jodaDateTimeFormat: JsonFormat[JodaDateTime] =
    new JsonFormat[JodaDateTime] {
      override def read(json: JsValue): JodaDateTime = json match {
        case JsString(string) => Try(JodaDateTime.parse(string)) match {
          case Success(validDateTime) => validDateTime
          case Failure(exception) => deserializationError(s"Could not parse $string as Joda DateTime.", exception)
        }
        case notAJsString => deserializationError(s"Expedted a String but got a $notAJsString")
      }

      override def write(obj: JodaDateTime): JsValue = JsString(obj.toString)
    }
}

trait JobPostingJsonUtils extends JsonUtils with SupportTools {
  implicit val jobPostingFormat: JsonFormat[JobPosting] =
    new JsonFormat[JobPosting] {
      override def read(json: JsValue): JobPosting = {
        val fields = json.asJsObject("JobPosting object expected").fields
        JobPosting(
          crawlingDate = fields("crawling_date").convertTo[JodaDateTime],
          postingId = fields("posting_id").convertTo[String],
          titleResultPage = fields("title_result_page").convertTo[String],
          titlePosting = fields("title_posting").convertTo[String],
          company = fields("company").convertTo[String],
          companyRatingValue = tryToDouble(fields("company_rating_value").convertTo[String]) match {
            case Some(double) => double
            case None => 0d
          },
          companyRatingCount = tryToInt(fields("company_rating_count").convertTo[String]) match {
            case Some(int) => int
            case None => 0
          },
          jobDescription = fields("job_description").convertTo[Seq[String]],
          postingTime = fields("posting_time").convertTo[String],
          jobLocation = fields("job_location").convertTo[String]
        )
      }

      override def write(obj: JobPosting) = ???
    }
}

trait EscoJsonUtils extends JsonUtils {
  def parseToSkillList(jsonStr: String): Taxonomy = {
    jsonStr.parseJson.convertTo[Taxonomy]
  }

  implicit def skillListUnmarshaller: FromEntityUnmarshaller[Taxonomy] = {
    Unmarshaller.stringUnmarshaller.map(parseToSkillList)
  }

  def parseToSkill(jsonStr: String): Node = {
    jsonStr.parseJson.convertTo[Node]
  }

  implicit def skillUnmarshaller: FromEntityUnmarshaller[Node] = {
    Unmarshaller.stringUnmarshaller.map(parseToSkill)
  }

  def parseToSearchResult(jsonStr: String): SearchResult = {
    jsonStr.parseJson.convertTo[SearchResult]
  }

  implicit def searchResultUnmarshaller: FromEntityUnmarshaller[SearchResult] = {
    Unmarshaller.stringUnmarshaller.map(parseToSearchResult)
  }

  implicit val taxonomyFormat: JsonFormat[Taxonomy] =
    new JsonFormat[Taxonomy] {
      override def read(json: JsValue): Taxonomy = {
        val fields = json.asJsObject("SkillList object expected").fields
        Taxonomy(
          relations = fields("_links").convertTo[TaxonomyConnections],
          classId = fields("classId").convertTo[String],
          className = fields("className").convertTo[String],
          preferredLabel = fields("preferredLabel").convertTo[PreferredLabel],
          title = fields("title").convertTo[String],
          uri = fields("uri").convertTo[String]
        )
      }

      override def write(obj: Taxonomy): JsValue = ???
    }

  implicit val skillFormat: JsonFormat[Node] =
    new JsonFormat[Node] {
      override def read(json: JsValue): Node = {
        val fields = json.asJsObject("Skill object expected").fields
        Node(
          className = fields("className").convertTo[String],
          uri = fields("uri").convertTo[String],
          title = fields("title").convertTo[String],
          description = getOptionalField(fields, "description")(descriptionFormat),
          preferredLabel = fields("preferredLabel").convertTo[PreferredLabel],
          alternativeLabel = getOptionalField[AlternativeLabel](fields,"alternativeLabel") match {
            case Some(AlternativeLabel(None, None)) => None
            case altLab => altLab
          },
          relations = fields("_links").convertTo[NodeConnectionInformation]
        )
      }

      override def write(obj: Node): JsValue = {
        val desc = obj.description match {
          case None => JsNull
          case Some(desc) => desc.toJson
        }
        val altLab = obj.alternativeLabel match {
          case None => JsNull
          case Some(altLab) => altLab.toJson
        }

        JsObject(
          "className" -> JsString(obj.className),
          "uri" -> JsString(obj.uri),
          "title" -> JsString(obj.title),
          "description" -> desc,
          "preferredLabel" -> obj.preferredLabel.toJson,
          "alternativeLabel" -> altLab,
          "_links" -> obj.relations.toJson
        )
      }
    }

  implicit val preferredLabelFormat: JsonFormat[PreferredLabel] =
    new JsonFormat[PreferredLabel] {
      def getWithFallback(fields: Map[String, JsValue], key: String): String = {
        Try(fields(key)) match {
          case Success(value) => value.convertTo[String]
          case Failure(_) => "N/A"
        }
      }

      override def read(json: JsValue): PreferredLabel = {
        val fields = json.asJsObject("PreferredLabel object expected").fields
        PreferredLabel(
          huLabel = getWithFallback(fields, Languages.HU.name),
          enLabel = getWithFallback(fields, Languages.EN.name)
        )
      }

      override def write(obj: PreferredLabel): JsValue = JsObject(
        Languages.HU.name -> JsString(obj.huLabel),
        Languages.EN.name -> JsString(obj.enLabel)
      )
    }

  implicit val alternativeLabel: JsonFormat[AlternativeLabel] =
    new JsonFormat[AlternativeLabel] {
      override def read(json: JsValue): AlternativeLabel = {
        val fields = Try(json.asJsObject(s"AlternativeLabel object expected, but got: $json").fields) match {
          case Success(value) => value
          case Failure(e) => Map.empty[String, JsValue]
        }
        AlternativeLabel(
          enLabels = getOptionalField[Seq[String]](fields, Languages.EN.name),
          huLabels = getOptionalField[Seq[String]](fields, Languages.HU.name)
        )
      }

      def getJsAlternateLabel(labelList: Option[Seq[String]], lang: Languages): Option[(String, JsValue)] = {
        labelList match {
          case None => None
          case Some(list) =>
            val labels: Vector[JsValue] = list.map(JsString(_))(breakOut)
            Some((lang.name, JsArray(labels)))
        }
      }

      override def write(obj: AlternativeLabel): JsValue = {
        val alternativeLabels = Seq(getJsAlternateLabel(obj.enLabels,Languages.EN), getJsAlternateLabel(obj.huLabels,Languages.HU)).flatten
        JsObject(alternativeLabels.toMap)
      }
    }

  implicit val resourceFormat: JsonFormat[Resource] =
    new JsonFormat[Resource] {
      override def read(json: JsValue): Resource = {
        val fields = json.asJsObject("Resource object expected").fields
        Resource(
          resource_uri = fields("href").convertTo[String],
          title = fields("title").convertTo[String],
          uri = fields("uri").convertTo[String]
        )
      }

      override def write(obj: Resource): JsValue = JsObject(
        "href" -> JsString(obj.resource_uri),
        "title" -> JsString(obj.title),
        "uri" -> JsString(obj.uri)
      )
    }

  implicit val resourceAddressFormat: JsonFormat[ResourceAddress] =
    new JsonFormat[ResourceAddress] {
      override def read(json: JsValue): ResourceAddress = {
        val fields = json.asJsObject("ResourceAddress object expected").fields
        ResourceAddress(
          resource_uri = fields("href").convertTo[String],
          uri = fields("uri").convertTo[String]
        )
      }

      override def write(obj: ResourceAddress): JsValue = ???
    }

  implicit val searchResultFormat: JsonFormat[SearchResult] = new JsonFormat[SearchResult] {
    override def read(json: JsValue): SearchResult = {
      val fields = json.asJsObject("Resource object expected").fields
      SearchResult(
        count = fields("count").convertTo[Int],
        offset = fields("offset").convertTo[Int],
        total = fields("total").convertTo[Int],
        concepts = fields("concepts").convertTo[Seq[ResourceAddress]]
      )
    }

    override def write(obj: SearchResult): JsValue = ???
  }

  implicit val descriptionFormat: JsonFormat[Description] =
    new JsonFormat[Description] {
      override def read(json: JsValue): Description =  {
        val fields = json.asJsObject("Description object expected").fields
        Description(
          enDescription = fields(Languages.EN.name).asJsObject("enDescription object expected").fields("literal").convertTo[String]
        )
      }

      override def write(obj: Description): JsValue = JsObject(
        "en" -> JsObject(
          "literal" -> JsString(obj.enDescription)
        )
      )
    }

  implicit val conceptConnectionInformationFormat: JsonFormat[NodeConnectionInformation] = jsonFormat17(NodeConnectionInformation)
  implicit val taxonomyConnectionsFormat: JsonFormat[TaxonomyConnections] = jsonFormat2(TaxonomyConnections)
  implicit val skillListFormat: JsonFormat[NodeList] = jsonFormat1(NodeList.apply) //https://github.com/spray/spray-json/issues/74
}
