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
            val br = kindOf[BidRequest] having (_.getApp != null)
            when {
                DomainBlacklist contains br.getApp.getDomain
            } then {
                exitWith(RuleMessage.DomainBlacklist)
            }
        },

        rule(RuleMessage.CategoryBlacklist) let {
            val br = kindOf[BidRequest] having (br => br.getApp != null && br.getApp.getCatList != null)
            when {
                br.getApp.getCatList.foldLeft(false)((b, a) => b || CategoryBlacklist.contains(a))
            } then {
                exitWith(RuleMessage.CategoryBlacklist)
            }
        },

        rule(RuleMessage.NotFromMobileApp) let {
            val br = kindOf[BidRequest] having (_.getApp == null)
            then {
                exitWith(RuleMessage.NotFromMobileApp)
            }
        },

        rule(RuleMessage.AdDimensionFilter) let {
            val br = kindOf[BidRequest] having (_.getImpList != null)
            then {
                if (br.getImpList.size == 1) {
                    val d = Dimension(br.getImpList.head.getW, br.getImpList.head.getH)
                    if (AdTypeFilter contains d)
                        exitWith(RuleMessage.AdDimensionFilter)
                }
            }
        },
        rule(RuleMessage.TooManyImpressions) let {
            val br = kindOf[BidRequest] having (_.getImpList != null)
            then {
                if (br.getImpList.size > 1)
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
            val br = kindOf[BidRequest] having (_.getAt != 2)
            then {
                exitWith(RuleMessage.AuctionTypeNotSupported)
            }
        }
    )
}

case class Dimension(width: Int, height: Int)
