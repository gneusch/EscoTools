package jobposting

import jobposting.JobPosting._
import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatestplus.junit.AssertionsForJUnit
import textmining.TextUtils._

class JobPostingTest extends AssertionsForJUnit {

  val jobPostingJsonLine: String = "{\"title_result_page\": \"Project Manager\", " +
    "\"posting_id\": \"fc915f0fcbf9c0a6\", " +
    "\"company_rating_value\": \"4.199999809265137\", " +
    "\"company\": \"Network IT\", " +
    "\"crawling_date\": \"2019-01-23\", " +
    "\"title_posting\": \"Project Manager\", " +
    "\"job_location\": \"Trafford\", " +
    "\"company_rating_count\": \"5\", " +
    "\"job_description\": [\"<div class=\\\"jobsearch-JobMetadataHeader icl-u-xs-mb--md\\\"><div class=\\\"jobsearch-JobMetadataHeader-item \\\"><span class=\\\"icl-u-xs-mr--xs\\\">\\u00a345,000 - \\u00a350,000 a year</span></div><div class=\\\"jobsearch-JobMetadataHeader-item  icl-u-xs-mt--xs\\\">Permanent</div></div>\", \"<div><div>Are you an ambitious and proven Project Manager, fed up with red tape and bureaucracy preventing successful project delivery? Do you currently run challenging and highly complex technical projects involving either digital, application, data, infrastructure, integration or managing business acquisitions? Are you unable to expand your portfolio to deliver the projects you need to expand your career? Would you like greater responsibility, variety and autonomy to deliver an exciting portfolio of projects?<br>\\n<br>\\nIf the answer is yes to all of these then our Client would like to speak to you! Based in the North Midlands our Client has been recognised and commended for thought leadership within their specialist services arena. They have achieved significant growth both organic and via acquisition and to support this, are looking for a Project Manager to join them.<br>\\n<br>\\nWorking within a new project management and change function the Project Manager will be involved from the early stages of project inception, through the full project life cycle to final delivery.<br>\\n<br>\\nEssential attributes to be the successful Project Manager are;<br>\\n<br>\\n<ul><li>Proven commercial project management experience of application or infrastructure projects</li></ul><br>\\n<ul><li>A good understanding of Prince II and its pragmatic application.</li></ul><br>\\n<ul><li>Experience of working within an Agile Project delivery function</li></ul><br>\\n<ul><li>A true technical appreciation of modern information technology</li></ul><br>\\n<ul><li>A true appetite and desire to progress</li></ul><br>\\n<ul><li>Proactive in nature</li></ul><br>\\nSpecific beneficial project experience<br>\\n<br>\\n<ul><li>Significant scale data migration / integration experience</li></ul><br>\\n<ul><li>Web transaction / on-line client systems</li></ul><br>\\n<ul><li>Business acquisition and operational change</li></ul><br>\\n<ul><li>Insurance services or similar industry experience</li></ul><br>\\nOur Client are entrepreneurial and thought leaders by nature and as such are looking for a like-minded Project Manager. Excellent career opportunities exist within this expanding Organisation.<br>\\n<br>\\nIf you are a proven and dynamic Project Manager who enjoys taking control and being accountable then apply now a starting basic salary of \\u00a345,000 - \\u00a350,000 plus benefits is offered.</div></div>\"], " +
    "\"posting_time\": \" - 3 hours ago\"}"

  var jobPosting: JobPosting = _

  val escoSkillList: List[String] = List("manage lending institution operations",
    "use access control software", "collect goods, bring together goods",
    "group goods", "gather goods", "gather goods together", "gather together goods",
    "sell insurance", "sell insurance products", "sell insurance services",
    "retail insurance", "principles of programme management", "managing of projects",
    "fundamentals of project management", "principles of project management"
  )

  @Before def init: Unit = {

    jobPosting = JobPostingCreator.fromJsonLine(jobPostingJsonLine)

  }

  @Test def jobPostingCreatorTest = {
    assertEquals(jobPosting.titleResultPage, "Project Manager")
    assertEquals(jobPosting.postingId, "fc915f0fcbf9c0a6")
    assertEquals(jobPosting.companyRatingValue, 4.199999809265137, 0)
    assertEquals(jobPosting.company, "Network IT")
    assertEquals(jobPosting.crawlingDate.toString("yyyy-MM-dd"), "2019-01-23")
    assertEquals(jobPosting.titlePosting, "Project Manager")
    assertEquals(jobPosting.jobLocation, "Trafford")
    assertEquals(jobPosting.companyRatingCount, 5)
    assertEquals(jobPosting.jobDescription, List("<div class=\"jobsearch-JobMetadataHeader icl-u-xs-mb--md\"><div class=\"jobsearch-JobMetadataHeader-item \"><span class=\"icl-u-xs-mr--xs\">\u00a345,000 - \u00a350,000 a year</span></div><div class=\"jobsearch-JobMetadataHeader-item  icl-u-xs-mt--xs\">Permanent</div></div>", "<div><div>Are you an ambitious and proven Project Manager, fed up with red tape and bureaucracy preventing successful project delivery? Do you currently run challenging and highly complex technical projects involving either digital, application, data, infrastructure, integration or managing business acquisitions? Are you unable to expand your portfolio to deliver the projects you need to expand your career? Would you like greater responsibility, variety and autonomy to deliver an exciting portfolio of projects?<br>\n<br>\nIf the answer is yes to all of these then our Client would like to speak to you! Based in the North Midlands our Client has been recognised and commended for thought leadership within their specialist services arena. They have achieved significant growth both organic and via acquisition and to support this, are looking for a Project Manager to join them.<br>\n<br>\nWorking within a new project management and change function the Project Manager will be involved from the early stages of project inception, through the full project life cycle to final delivery.<br>\n<br>\nEssential attributes to be the successful Project Manager are;<br>\n<br>\n<ul><li>Proven commercial project management experience of application or infrastructure projects</li></ul><br>\n<ul><li>A good understanding of Prince II and its pragmatic application.</li></ul><br>\n<ul><li>Experience of working within an Agile Project delivery function</li></ul><br>\n<ul><li>A true technical appreciation of modern information technology</li></ul><br>\n<ul><li>A true appetite and desire to progress</li></ul><br>\n<ul><li>Proactive in nature</li></ul><br>\nSpecific beneficial project experience<br>\n<br>\n<ul><li>Significant scale data migration / integration experience</li></ul><br>\n<ul><li>Web transaction / on-line client systems</li></ul><br>\n<ul><li>Business acquisition and operational change</li></ul><br>\n<ul><li>Insurance services or similar industry experience</li></ul><br>\nOur Client are entrepreneurial and thought leaders by nature and as such are looking for a like-minded Project Manager. Excellent career opportunities exist within this expanding Organisation.<br>\n<br>\nIf you are a proven and dynamic Project Manager who enjoys taking control and being accountable then apply now a starting basic salary of \u00a345,000 - \u00a350,000 plus benefits is offered.</div></div>"))
    assertEquals(jobPosting.postingTime, " - 3 hours ago")
  }

  @Test def jobDescGramListInEscoSkillsTest: Unit = {

    val jobDescriptionStringList = jobDescGramListInEscoSkills(genBiGrams(strListToLower(jobPosting.jobDescriptionToStringList))
      .map{ case (str1, str2) => s"$str1 $str2" }, escoSkillList).sorted
    val expected = List("of projects", "project management", "of project", "project management", "insurance services").sorted
    assertEquals(expected, jobDescriptionStringList)

  }

  @Test def escoSkillsListWithJobDescTest: Unit = {

    val escoSkillsList = escoSkillsListWithJobDesc(genBiGrams(strListToLower(jobPosting.jobDescriptionToStringList))
      .map{ case (str1, str2) => s"$str1 $str2" }, escoSkillList).sorted
    val expected = List("managing of projects","fundamentals of project management", "principles of project management", "sell insurance services").sorted
    assertEquals(expected, escoSkillsList)

  }

}
