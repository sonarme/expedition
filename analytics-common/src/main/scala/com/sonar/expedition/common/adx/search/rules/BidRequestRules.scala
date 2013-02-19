package com.sonar.expedition.common.adx.search.rules

import hammurabi.{FailedExecutionException, RuleEngine, WorkingMemory, Rule}
import Rule._
import com.sonar.expedition.common.adx.search.service.BidProcessingService
import org.openrtb.mobile.BidRequest
import collection.JavaConversions._

object BidRequestRules {

    //domain blacklist
    val DomainBlacklist = Set(
        "highlig.ht"
    )
    //publisher category blacklist
    val CategoryBlacklist = Set(
        "IAB2" //Automotive
    )
    //filter out these dimensions
    val AdTypeFilter = Set(
        Dimension(10, 10)
    )

    //todo: daily spend and hourly spend max
    object RuleMessage {
        val DomainBlacklist = "BidRequest is on domain blacklist"
        val CategoryBlacklist = "BidRequest is on category blacklist"
        val NotFromMobileApp = "BidRequest is not from a mobile app"
        val AdDimensionFilter = "BidRequest contains filtered Ad dimensions"
        val TooManyImpressions = "BidRequest contains too many Impressions"
        val MaxHourlySpentExhausted = "Max Hourly Spent Exhausted"
        val AuctionTypeNotSupported = "AuctionType not supported"
    }

    def execute(bidRequest: BidRequest) = {
        val workingMemory = WorkingMemory(bidRequest)
        RuleEngine(ruleSet) execOn workingMemory
    }

    val ruleSet = Set(
        rule(RuleMessage.DomainBlacklist) let {
            val br = kindOf[BidRequest] having (_.app != null)
            when {
                DomainBlacklist contains br.app.domain.toString
            } then {
                exitWith(RuleMessage.DomainBlacklist)
            }
        },

        rule(RuleMessage.CategoryBlacklist) let {
            val br = kindOf[BidRequest] having (br => br.app != null && br.app.cat != null)
            when {
                br.app.cat.foldLeft(false)((b, a) => b || CategoryBlacklist.contains(a))
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
                if (br.imp.size == 1) {
                    val d = Dimension(br.imp.head.w, br.imp.head.h)
                    if (AdTypeFilter contains d)
                        exitWith(RuleMessage.AdDimensionFilter)
                }
            }
        },
        rule(RuleMessage.TooManyImpressions) let {
            val br = kindOf[BidRequest] having (_.imp != null)
            then {
                if (br.imp.size > 1)
                    exitWith(RuleMessage.TooManyImpressions)
            }
        },
        rule(RuleMessage.MaxHourlySpentExhausted) let {
            val br = any(kindOf[BidRequest])
            when {
                BidProcessingService.CurrentHourAmountSpent >= BidProcessingService.MaxAmountSpentHourly
            } then {
                exitWith(RuleMessage.MaxHourlySpentExhausted)
            }
        },
        rule(RuleMessage.AuctionTypeNotSupported) let {
            val br = kindOf[BidRequest] having (_.at != 2)
            then {
                exitWith(RuleMessage.AuctionTypeNotSupported)
            }
        }
    )
}

case class Dimension(width: Int, height: Int)
