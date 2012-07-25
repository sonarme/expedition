package com.sonar.expedition.scrawler.jobs

;

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class DatasetJobClassifyTest extends FlatSpec with ShouldMatchers {

    "the regular expression" should "match a url on the page" in {
        """http://www.bls.gov/oes/current/oes111021.htm""" match {
            case DatasetJobClassify.jobs(jobUrl) =>
            case _ => fail("regex didn't match")
        }
    }

    "the job" should "parse the urls" in {
        val job = new DatasetJobClassify(null)
        val result = job.getJobs("http://www.bls.gov/oes/current/oes_nat.htm")

        assert(result.size > 1)
        //assert (result.head === "http://\t  test \n")
    }
}