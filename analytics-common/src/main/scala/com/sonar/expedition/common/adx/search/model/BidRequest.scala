package com.sonar.expedition.common.adx.search.model

case class BidRequest(id: String, at: Int, tmax: Int, imp: List[BidImpression], site: Site, app: App, device: Device, user: User, restrictions: Restrictions)

case class BidImpression(impid: CharSequence, wseat: List[CharSequence], h: Int, w: Int, pos: Int, instl: Int, btype: List[CharSequence], battr: List[CharSequence])

case class Site(sid: CharSequence, name: CharSequence, domain: CharSequence, pid: CharSequence, pub: CharSequence, pdomain: CharSequence, cat: List[CharSequence], keywords: CharSequence, page: CharSequence, ref: CharSequence, search: CharSequence)

case class App(aid: CharSequence, name: CharSequence, domain: CharSequence, pid: CharSequence, pub: CharSequence, pdomain: CharSequence, cat: List[CharSequence], keywords: CharSequence, ver: CharSequence, bundle: CharSequence, paid: Int)

case class Device(did: CharSequence, dpid: CharSequence, ip: CharSequence, country: CharSequence, carrier: CharSequence, ua: CharSequence, make: CharSequence, model: CharSequence, os: CharSequence, osv: CharSequence, js: Int, loc: CharSequence)

case class User(uid: CharSequence, yob: Int, gender: CharSequence, zip: CharSequence, country: CharSequence, keywords: CharSequence)

case class Restrictions(bcat: List[CharSequence], badv: List[CharSequence])

