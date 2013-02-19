package com.sonar.expedition.common.adx.search.rules

import org.scalatest.FlatSpec

class BidRequestRulesTest extends FlatSpec {

    // BidRequest.newBuilder.setId("1").setImp(List.empty[BidImpression]) and so on
    /*
        val nonMobileBid = BidRequest("1", List[Impression](), site = Site("123", name = "sonar", domain = "sonar.me", publisher = Publisher()))
        val bldomainBid = BidRequest("1", List[Impression](), app = App("123", name = "sonar", domain = "highlig.ht", publisher = Publisher()))
        val blcategoryBid = BidRequest("1", List[Impression](), app = App("123", name = "sonar", domain = "sonar.me", publisher = Publisher(cat = List[String]("IAB", "IAB2"))))
        val goodBid = BidRequest("1", List[Impression](), app = App("123", name = "sonar", domain = "sonar.me", publisher = Publisher(cat = List[String]("IAB", "IAB3"))), device = Device(geo = Geo(lat = 40.738933f, lon = -73.988113f)))
        val badDimensionBid = BidRequest("1", List[Impression](Impression("a", banner = Banner(w = 10, h = 10))), app = App())
        val tooManyImpressionsBid = BidRequest("1", List[Impression](Impression("a", banner = Banner(w = 12, h = 12)), Impression("a", banner = Banner(w = 11, h = 11))), app = App())
        val unsupportedAutionTypeBid = BidRequest("1", List[Impression](), at = 1, app = App("123", name = "sonar", domain = "sonar.me", publisher = Publisher(cat = List[String]("IAB", "IAB3"))), device = Device(geo = Geo(lat = 40.738933f, lon = -73.988113f)))

        "BidRequestRules" should "only service mobile apps" in {
            val res = BidRequestRules.execute(nonMobileBid).orNull
            assert(res === BidRequestRules.RuleMessage.NotFromMobileApp)
        }

        "BidRequestRules" should "discard blacklisted domains" in {
            val res = BidRequestRules.execute(bldomainBid).orNull
            assert(res === BidRequestRules.RuleMessage.DomainBlacklist)
        }

        "BidRequestRules" should "discard blacklisted publisher categories" in {
            val res = BidRequestRules.execute(blcategoryBid).orNull
            assert(res === BidRequestRules.RuleMessage.CategoryBlacklist)

            val res2 = BidRequestRules.execute(goodBid).orNull
            assert(res2 === null)
        }

        "BidRequestRules" should "stop accepting bids after spending 10 dollars" in {

        }

        "BidRequestRules" should "discard ads of certain dimensions" in {
            val res = BidRequestRules.execute(badDimensionBid).orNull
            assert(res === BidRequestRules.RuleMessage.AdDimensionFilter)
        }

        "BidRequestRules" should "discard ads with more than 1 Impressions" in {
            val res = BidRequestRules.execute(tooManyImpressionsBid).orNull
            assert(res === BidRequestRules.RuleMessage.TooManyImpressions)
        }

        "BidRequestRules" should "discard ads that are not second price auctions" in {
            val res = BidRequestRules.execute(unsupportedAutionTypeBid).orNull
            assert(res == BidRequestRules.RuleMessage.AuctionTypeNotSupported)
        }*/
}
