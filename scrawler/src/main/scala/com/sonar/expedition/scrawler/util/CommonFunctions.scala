package com.sonar.expedition.scrawler.util

import java.security.MessageDigest
import util.matching.Regex

object CommonFunctions {

    val CheckinExtractLine: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.]*)::([\-\d]*)::([\.\d\-E]+)::([\.\d\-E]+)""".r
    val CheckinExtractLineWithMessages: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\w\d\-\.@]*)::([a-zA-Z\w\d\-]+)::(.*?)::(.*?)::([\d\-:T\+\.]*)::([\-\d]*)::([\.\d\-E]+)::([\.\d\-E]+)::(.*)""".r
    val ServiceProfileExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|4s) : (.*)""".r
    val Occupation: Regex = """([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)\t([a-zA-Z\d\- ]+)\t(.+)""".r
    val FriendExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo(.*)""".r
    val FriendProdExtractLine: Regex = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\d\-]+)::(.*)""".r

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

    def hashed(str: String): String = {
        if (str.equals(""))
            ""
        else
            md5SumString(str.getBytes("UTF-8"))
    }

}