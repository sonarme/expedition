package com.sonar.expedition.common.adx.search.model

case class BidResponse(id: String,
                       seatbid: List[SeatBid],
                       bidid: String = null,
                       cur: String = null,
                       customdata: String = null,
                       ext: Any = null)

case class SeatBid(bid: List[Bid],
                   seat: String = null,
                   group: Int = 0,
                   ext: Any = null)

case class Bid(id: String,
               impid: String,
               price: Float,
               adid: String = null,
               nurl: String = null,
               adm: String = null,
               adomain: List[String] = null,
               iurl: String = null,
               cid: String = null,
               crid: String = null,
               attr: List[CharSequence] = null,
               ext: Any = null)
