package com.sonar.expedition.scrawler.meetup

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class ScrawlerUtilsTest extends FlatSpec with ShouldMatchers {

    "the meetup member page url" should "return the last offset for member pages with pagination" in {

        val result = ScrawlerUtils.findLastMeetupMemberPage("http://www.meetup.com/Conservative-Christian-Singles/members/")
        assert(result === 360 / 20)
    }

    it should "return the first page for member pages without pagination" in {
        val result = ScrawlerUtils.findLastMeetupMemberPage("http://www.meetup.com/Drumming-in-Plympton/members/")
        assert(result === 0)
    }
}
