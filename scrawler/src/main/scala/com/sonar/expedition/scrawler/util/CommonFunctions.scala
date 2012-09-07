package com.sonar.expedition.scrawler.util

import java.security.MessageDigest
import util.matching.Regex
import com.sonar.dossier.dto.ServiceType
import com.twitter.scalding.RichDate

object CommonFunctions {

    val Occupation: Regex = """([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)\t([a-zA-Z\d\- ]+)\t(.+)""".r
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
