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

    def iqrOutlier(data: Iterable[Double]) = {
        require(data.size > 0)
        val dataSorted = data.toSeq.sorted
        val half = dataSorted.length / 2
        val quarter = half / 2
        val (q1, q3) =
            if (half % 2 == 1)
                (dataSorted(quarter), dataSorted(dataSorted.length - quarter - 1))
            else ((dataSorted(quarter) + dataSorted(quarter + 1)) / 2,
                    (dataSorted(dataSorted.length - quarter - 1) + dataSorted(dataSorted.length - quarter)) / 2)
        val iqr = q3 - q1
        val outlierFactor = 3
        (q1 - outlierFactor * iqr, q3 + outlierFactor * iqr)
    }

    def mean[T](item: Traversable[T])(implicit n: Numeric[T]) = {
        n.toDouble(item.sum) / item.size.toDouble
    }

    def variance[T](items: Traversable[T])(implicit n: Numeric[T]): Double = {
        val itemMean = mean(items)
        val count = items.size
        val sumOfSquares = items.foldLeft(0.0d)((total, item) => {
            val itemDbl = n.toDouble(item)
            val square = math.pow(itemDbl - itemMean, 2)
            total + square
        })
        sumOfSquares / count.toDouble
    }

    def stddev[T](items: Traversable[T])(implicit n: Numeric[T]): Double = {
        math.sqrt(variance(items))
    }

}
