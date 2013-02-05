package com.sonar.expedition.common.adx.search.model

case class BidRequestWrapper(bidRequest: BidRequest, var failedRules: List[String] = List[String]())