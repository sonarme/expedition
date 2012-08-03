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

    it should "print all member links contents" in {
        val res = ScrawlerUtils.extractContentsfromPageLinks("http://www.meetup.com/Drumming-in-Plympton/members/")

        assert(res !== null)
    }


    it should "print all member links" in {
        val res = ScrawlerUtils.extractContentsPageLinks("http://www.meetup.com/NYCSOCCER/members/")

        assert(res !== null)
    }

    it should "profile name grp name" in {
        val res = ScrawlerUtils.extractProfileGrpName("http://www.meetup.com/Drumming-in-Plympton/members/15946481/")

        assert(res !== null)
    }

    it should "profile name meetup id" in {
        val (local, resgion, country) = ScrawlerUtils.extractProfileLocation(ScrawlerUtils.getPageContentsDoc("http://www.meetup.com/members/11363609/"))

    }

}
