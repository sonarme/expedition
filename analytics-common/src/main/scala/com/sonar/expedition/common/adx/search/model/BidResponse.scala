package com.sonar.expedition.common.adx.search.model

case class BidResponse(id: CharSequence, bidid: CharSequence, nbr: Int, cur: CharSequence, units: Int, seatbid: List[SeatBid])

case class SeatBid(seat: CharSequence, group: Int, bid: List[Bid])

case class Bid(impid: CharSequence, price: CharSequence, adid: CharSequence, nurl: CharSequence, adm: CharSequence, adomain: CharSequence, iurl: CharSequence, cid: CharSequence, crid: CharSequence, attr: List[CharSequence])
