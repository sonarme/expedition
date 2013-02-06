package com.sonar.expedition.common.adx.search.rules

import hammurabi.{FailedExecutionException, RuleEngine, WorkingMemory, Rule}
import Rule._
import com.sonar.expedition.common.adx.search.model.{BidRequest}

object BidRequestRules {

    //domain blacklist
    val domainBlacklist = Set(
        "highlig.ht"
    )
    //publisher category blacklist
    val categoryBlacklist = Set(
        "IAB2" //Automotive
    )
    //filter out these dimensions
    val adTypeFilter = Set(
        Dimension(10, 10)
    )

    //todo: daily spend and hourly spend max
    object RuleMessage {
        val DomainBlacklist = "BidRequest is on domain blacklist"
        val CategoryBlacklist = "BidRequest is on category blacklist"
        val NotFromMobileApp = "BidRequest is not from a mobile app"
        val AdDimensionFilter = "BidRequest contains filtered Ad dimensions"
        val TooManyImpressions = "BidRequest contains too many Impressions"
    }

    def execute(bidRequest: BidRequest) = {
        val workingMemory = WorkingMemory(bidRequest)
        RuleEngine(ruleSet) execOn workingMemory
    }

    val ruleSet = Set(
        rule(RuleMessage.DomainBlacklist) let {
            val br = kindOf[BidRequest] having (_.app != null)
            when {
                domainBlacklist contains br.app.domain
            } then {
                exitWith(RuleMessage.DomainBlacklist)
            }
        },

        rule(RuleMessage.CategoryBlacklist) let {
            val br = kindOf[BidRequest] having (br => br.app != null && br.app.publisher != null && br.app.publisher.cat != null)
            when {
                br.app.publisher.cat.foldLeft(false)((b, a) => b || categoryBlacklist.contains(a))
            } then {
                exitWith(RuleMessage.CategoryBlacklist)
            }
        },

        rule(RuleMessage.NotFromMobileApp) let {
            val br = kindOf[BidRequest] having (_.app == null)
            then {
                exitWith(RuleMessage.NotFromMobileApp)
            }
        },

        rule(RuleMessage.AdDimensionFilter) let {
            val br = kindOf[BidRequest] having (_.imp != null)
            then {
                if(br.imp.size == 1) {
                    val d = Dimension(br.imp.head.banner.w, br.imp.head.banner.h)
                    if (adTypeFilter contains d)
                        exitWith(RuleMessage.AdDimensionFilter)
                } else if (br.imp.size > 1) {
                    exitWith(RuleMessage.TooManyImpressions)
                }
            }
        }
    )
}

case class Dimension(width: Int, height: Int)