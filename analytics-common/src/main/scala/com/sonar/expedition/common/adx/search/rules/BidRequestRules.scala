package com.sonar.expedition.common.adx.search.rules

import hammurabi.{FailedExecutionException, RuleEngine, WorkingMemory, Rule}
import Rule._
import com.sonar.expedition.common.adx.search.model.{BidRequestWrapper, BidRequest}

object BidRequestRules {

    //domain blacklist
    val domainBlacklist = Set(
        "highlig.ht"
    )
    //publisher category blacklist
    val categoryBlacklist = Set(
        "IAB2" //Automotive
    )
    //only allow these types
    val adTypeFilter = Set(
        Dimension("10", "10")
    )
    //todo: daily spend and hourly spend max
    object RuleMessage {
        val DomainBlacklist = "BidRequest is on domain blacklist"
        val CategoryBlacklist = "BidRequest is on category blacklist"
        val NotFromMobileApp = "BidRequest is not from a mobile app"
    }

    def execute(bidRequestWrapper: BidRequestWrapper) = {
        val workingMemory = WorkingMemory(bidRequestWrapper)
        RuleEngine(ruleSet) execOn workingMemory
    }

    val add = new {
        def error(ruleMessage: String) = new {
            def to(brw: BidRequestWrapper) = {
                remove(brw)
                val failedRules = ruleMessage :: brw.failedRules
                produce(brw.copy(failedRules = failedRules))
            }
        }
    }

    val ruleSet = Set(
        rule(RuleMessage.DomainBlacklist) let {
            val br = kindOf[BidRequestWrapper] having (_.bidRequest.app != null)
            when {
                domainBlacklist contains br.bidRequest.app.domain
            } then {
                add error RuleMessage.DomainBlacklist to br
            }
        },

        rule(RuleMessage.CategoryBlacklist) let {
            val br = kindOf[BidRequestWrapper] having  (br => br.bidRequest.app != null && br.bidRequest.app.publisher != null && br.bidRequest.app.publisher.name != null)
            when {
                categoryBlacklist contains br.bidRequest.app.publisher.name
            } then {
                add error RuleMessage.CategoryBlacklist to br
            }
        },
        rule(RuleMessage.NotFromMobileApp) withSalience 1 let {
            val br = kindOf[BidRequestWrapper] having (_.bidRequest.app == null)
            then {
                add error RuleMessage.NotFromMobileApp to br
            }
        }
    )
}

case class Dimension(width: String, height: String)