package com.sonar.expedition.common.adx.search.rules

import org.scalatest.FlatSpec
import com.sonar.expedition.common.adx.search.model._
import hammurabi.{FailedExecutionException, RuleEngine, WorkingMemory}
import com.sonar.expedition.common.adx.search.model.Impression
import com.sonar.expedition.common.adx.search.model.App
import com.sonar.expedition.common.adx.search.model.BidRequest
import scala.Some
import com.sonar.expedition.common.adx.search.model.Publisher

class BidRequestRulesTest extends FlatSpec {

    "BidRequestRules" should "only service mobile apps" in {
        val bid = BidRequestWrapper(BidRequest("2", List[Impression](), site = Site("123", name = "sonar", domain = "highlig.ht", publisher = Publisher())))
        val res = BidRequestRules.execute(bid)
        assert(bid.failedRules contains BidRequestRules.RuleMessage.NotFromMobileApp)
    }

    "BidRequestRules" should "discard blacklisted domains" in {
        val bid = BidRequestWrapper(BidRequest("1", List[Impression](), app = App("123", name = "sonar", domain = "highlig.ht", publisher = Publisher())))
        BidRequestRules.execute(bid)
        assert(bid.failedRules contains BidRequestRules.RuleMessage.DomainBlacklist)
    }

    "BidRequestRules" should "discard blacklisted publisher categories" in {

    }

    "BidRequestRules" should "stop accepting bids after spending 10 dollars" in {

    }

    "BidRequestRules" should "discard ads of certain dimensions" in {

    }
}