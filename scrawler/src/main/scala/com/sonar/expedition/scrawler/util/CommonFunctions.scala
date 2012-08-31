package com.sonar.expedition.scrawler.util

import java.security.MessageDigest
import util.matching.Regex
import com.sonar.dossier.dto.ServiceType
import com.twitter.scalding.RichDate

object CommonFunctions {

    val CheckinExtractLine: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.Z]*)::([\-\d]*)::([\.\d\-E]+)::([\.\d\-E]+)""".r
    val CheckinExtractLineWithMessages: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.Z]*)::([\-\d]*)::([\.\d\-E]+)::([\.\d\-E]+)::(.*)""".r

    val CheckinExtractLineWithVenueId: Regex = """(twitter|facebook|foursquare|linkedin|sonar):([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.Z]*)::(.*?)::([\.\d\-E]+)::([\.\d\-E]+)::([a-zA-Z\w\d\-]+)?::(.*)""".r
    val CheckinExtractLineStageData: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.]*)::([\-\d]*)::([\.\d\-E]+)::([\.\d\-E]+)""".r
    val CheckinExtractLineWithMessagesStageData: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.]*)::([\-\d]*)::([\.\d\-E]+)::([\.\d\-E]+)::(.*)""".r

    //(RE for CheckinExtractLineProdData)
    // 16551972-d480-337d-969d-f398dca7c2c4::foursquare::9912007::500a4c8fe4b01d293b7a70ef::Bullingdon Road::::2012-07-21T06:30:39.000Z::gcpn7uhn9fdc::51.746735::-1.235883::4e31c3eba809189f7a21b67e::Zzzzzzzzz.
    val CheckinExtractLineProdData: Regex = """([a-zA-Z\d\-]*?)::(twitter|facebook|foursquare|linkedin|sonar)::([\w\d\-\.@]*)::([\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.Z]*)::([\w\d\-]+)::([\.\d\-E]+)::([\.\d\-E]+)::([\w\d]*?)::(.*?)""".r

    val ServiceProfileExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|4s)\s:\s(.*)""".r
    val Occupation: Regex = """([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)\t([a-zA-Z\d\- ]+)\t(.+)""".r
    val FriendExtractLine: Regex = """([a-zA-Z\d\-]+):(.*?)"id":"(.*?)","service_type":"(.*?)","name":"(.*?)","photo(.*)""".r
    val FriendProdExtractLine: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\d\-]+)::(.*)""".r
    val ServiceProfileExtractLineCheck: Regex = """^([a-zA-Z\d\-]+)_(fb|ln|tw|4s)\s:\s(.*)$""".r
    val pay: Regex = """(.*)([\d\,]+)(\s+)(per year)(.*)""".r
    val BuzzFromText: Regex = """(.*)\t([\d\.]+)\t([\d\.]+)\t(.*)""".r

    val College: Regex = """(A|B|O)""".r
    val NoCollege: Regex = """(H)""".r
    val Grad: Regex = """(D|M|MBA|J|P)""".r

    val DEFAULT_NO_DATE = RichDate(0L)
    val NONE_VALUE = "none"

    def md5SumString(bytes: Array[Byte]): String = {
        val md5 = MessageDigest.getInstance("MD5")
        md5.reset()
        md5.update(bytes)
        md5.digest().map(0xFF & _).map {
            "%02x".format(_)
        }.foldLeft("") {
            _ + _
        }
    }

    def hashed(str: String) = if (str.isEmpty) "" else md5SumString(str.getBytes("UTF-8"))

    def isNumeric(input: String): Boolean = !isNullOrEmpty(input) && input.forall(_.isDigit)

    def isNullOrEmpty(str: String) = str == null || str.isEmpty || str == "null"

    final val venueGoldenIdPriorities = List(ServiceType.foursquare, ServiceType.twitter, ServiceType.facebook).reverse.zipWithIndex.toMap

}
