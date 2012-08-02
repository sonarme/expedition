package com.sonar.expedition.scrawler.pipes


import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import DTOProfileInfoPipe._

class DTOProfileInfoPipeTest extends FlatSpec with ShouldMatchers {

    val testId = "testId"
    val fb = "fb"
    val ln = "ln"
    val fbProfile = "fbProfile"
    val lnProfile = "lnProfile"
    "zipServiceTypeAndJson" should "fail on uneven input" in {
        intercept[IllegalArgumentException] {
            zipServiceTypeAndJson(testId, List(fb), List.empty[String])
        }

    }
    it should "zip empty input" in {
        val result = zipServiceTypeAndJson(testId, List.empty[String], List.empty[String])
        assert(result ===(testId, None, None))

    }
    it should "zip input with facebook" in {
        val result = zipServiceTypeAndJson(testId, List(fb), List(fbProfile))
        assert(result ===(testId, Some(fbProfile), None))

    }

    it should "zip input with linkedin" in {
        val result = zipServiceTypeAndJson(testId, List(ln), List(lnProfile))
        assert(result ===(testId, None, Some(lnProfile)))

    }


    it should "zip input with both fb and li" in {
        val result = zipServiceTypeAndJson(testId, List(ln, fb), List(lnProfile, fbProfile))
        assert(result ===(testId, Some(fbProfile), Some(lnProfile)))

    }
}
