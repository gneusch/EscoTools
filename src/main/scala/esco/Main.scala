package esco

import jobposting.JobPostingFile
import org.apache.log4j.{Level, Logger}
import utils.Languages

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import org.kohsuke.args4j.Option

object Main extends App {

  Logger.getLogger("org").setLevel(Level.DEBUG)

  //val sparkConf = new SparkConf().setAppName("jobPostingApp").setMaster("local[*]")
  //val sparkContext = new SparkContext(sparkConf)

  def getSkillLabelList = {
    val lang = Languages.EN
    val escoSkillHttp = new EscoQueuingSkillHttp(8192)

    val escoSkillList = escoSkillHttp.getListOfSkills(lang)

    val skillTitleList = escoSkillList map {
      skillList => skillList flatMap {
        skill => {
          val preferredLabel = skill.preferredLabel.enLabel
          val alternativeLabel = skill.alternativeLabel match {
            case Some(alternateLabel) => lang.name match {
              case Languages.HU.name => alternateLabel.getLabelList(Languages.HU)
              case _ => alternateLabel.getLabelList(Languages.EN)
            }
            case None => List()
          }
          preferredLabel +: alternativeLabel
        }
      }
    }

    skillTitleList
  }

  def printLabelList(skillTitleList: Future[List[String]]) = {
    Await.result(skillTitleList, Duration.Inf) map {
      var i = 0
      title => {
        i = i + 1
        println(s"$i: $title")
      }
    }
  }

  def findBiGramsInSkillLabels = {
    import jobposting.JobPosting._
    import textmining.TextUtils._

    val jobPostingDir = s"/home/answeris42/Workspace/scraper/JobPostingScraper/data/"
    val extensions = List("jl")
    val jobPostingTitleDescTupleFuture = Future {
      JobPostingFile.getFromDir(jobPostingDir, extensions) flatMap {
        jobposting => jobposting.getJobPostings
      } map {
        jobposting => (
          jobposting.titlePosting,
          genBiGrams(strListToLower(jobposting.jobDescriptionToStringList)) map {
            case (str1, str2) => s"$str1 $str2"
          }
        )
      }
    }
    val escoLabelsFuture = getSkillLabelList

    val postingDescEscoSkillMap = jobPostingTitleDescTupleFuture flatMap {
      jPTDT => escoLabelsFuture map {
        escoLabels => jPTDT map {
          jobPosting => {
            val t1 = Future {
              jobDescGramListInEscoSkills(jobPosting._2,escoLabels)
            }
            val t2 = Future {
              escoSkillsListWithJobDesc(jobPosting._2,escoLabels)
            }
            (jobPosting._1, Await.result(t1, Duration.Inf), Await.result(t2, Duration.Inf))
          }
        }
      }
    }

    postingDescEscoSkillMap

  }

  //Await.result(findBiGramsInSkillLabels, Duration.Inf) map {
  //  postingTripples => println(postingTripples._1, postingTripples._2, postingTripples._3)
  //}

//  def findBiGramsInSkillLabelsWithSpark = {
//    import textmining.TextUtils._
//
//    val jobPostingDir = s"/home/answeris42/Workspace/scraper/JobPostingScraper/data/"
//    val extensions = List("jl")
//
//    val jobPostingTitleDescTupleFuture = Future {
//      JobPostingFile.getFromDir(jobPostingDir, extensions) flatMap {
//        jobposting => jobposting.getJobPostings
//      } map {
//        jobposting => (
//          jobposting.titlePosting,
//          genBiGrams(strListToLower(jobposting.jobDescriptionToStringList)) map {
//            case (str1, str2) => s"$str1 $str2"
//          }
//        )
//      }
//    }
//    val escoLabelsFuture = getSkillLabelList
//
//    val jobPostingTitleDescTuple = Await.result(jobPostingTitleDescTupleFuture, Duration.Inf)
//
//    println(s"\n\n\nJobPosting reading is finished\n\n\n")
//
//    val escoLabels = Await.result(escoLabelsFuture, Duration.Inf)
//
//    println(s"\n\n\nEscolabels downloaded\n\n\n")
//
//    val jobPostingTitleDescTupleRDD = sparkContext.parallelize(jobPostingTitleDescTuple)
//    val escoLabelsRDD = sparkContext.parallelize(escoLabels)
//
//    val escoLabelsBC = sparkContext.broadcast(escoLabelsRDD.collect)
//
//    val something = jobPostingTitleDescTupleRDD.flatMap(
//      jobDescMap => {
//        val jobDescList = jobDescMap._2.filter( jobDesc => escoLabelsBC.value.exists( _.contains(jobDesc)) )
//        jobDescList.length match {
//          case 0 => None
//          case _ => Some(jobDescMap._1, jobDescList)
//        }
//      }
//    )
//    something.saveAsTextFile("out/experiment1.text")
//
//  }

  @Option(name="-csvpath", usage="Set the path for CSV file")
  private var csvpath: String = _

   def skillsToFile(path: String, lang: Languages): Unit = {
     val escoSkillHttp = new EscoQueuingSkillHttp(8192)
     val escoSkillListFuture = escoSkillHttp.getListOfSkills(lang).map( s => SkillList(s) )
     val escoSkillList = Await.result(escoSkillListFuture, Duration.Inf)

     SkillList.writeToCsvFile(escoSkillList, path)
   }

  skillsToFile(args(0), Languages.EN)

}
