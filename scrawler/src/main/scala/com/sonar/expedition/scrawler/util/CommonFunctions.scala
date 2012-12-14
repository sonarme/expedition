package com.sonar.expedition.scrawler.util

import java.security.MessageDigest
import util.matching.Regex
import com.sonar.dossier.dto.ServiceType
import com.twitter.scalding.{Args, RichDate}
import org.apache.commons.beanutils.{PropertyUtils, BeanUtils}
import collection.JavaConversions._

object CommonFunctions {

    val Occupation: Regex = """([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)\t([a-zA-Z\d\- ]+)\t(.+)""".r
    val pay: Regex = """(.*)([\d\,]+)(\s+)(per year)(.*)""".r
    val BuzzFromText: Regex = """(.*)\t([\d\.]+)\t([\d\.]+)\t(.*)""".r

    val College: Regex = """(A|B|O)""".r
    val NoCollege: Regex = """(H)""".r
    val Grad: Regex = """(D|M|MBA|J|P)""".r

    val NONE_VALUE = "none"

    def md5SumString(bytes: Array[Byte]) = {
        val md5 = MessageDigest.getInstance("MD5")
        md5.reset()
        md5.update(bytes)
        md5.digest().map(0xFF & _).map {
            "%02x".format(_)
        }.mkString
    }

    def hashed(str: String) = if (str.isEmpty) "" else md5SumString(str.getBytes("UTF-8"))

    def isNumeric(input: String): Boolean = !isNullOrEmpty(input) && input.forall(_.isDigit)

    def isNullOrEmpty(str: String) = str == null || str.isEmpty || str == "null"

    final val venueGoldenIdPriorities = List(ServiceType.foursquare, ServiceType.twitter, ServiceType.facebook).reverse.zipWithIndex.toMap

    def iqrOutlier(data: Iterable[Double]) = {
        if (data.size == 1) (data.head, data.head)
        else {
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
    }

    def populateNonEmpty[T](agg: T, any: T) = {
        BeanUtils.populate(agg, PropertyUtils.describe(any).filterNot {
            _._2 match {
                case null => true
                case s: String => s.isEmpty
                case juMap: java.util.Map[_, _] => juMap.isEmpty
                case collection: java.util.Collection[_] => collection.isEmpty
                case iterable: Iterable[_] => iterable.isEmpty
                case _ => false
            }
        })
        agg
    }

    def ppmap(args: Args) = {
        args.optional("ppmap") map {
            _.split(" *, *").map {
                s =>
                    val Array(left, right) = s.split(':')
                    ("cassandra.node.map." + left) -> right
            }.toMap
        } getOrElse (Map.empty[String, String])

    }

    def createSegments[T <: Comparable[_], S](value: T, segments: Iterable[Segment[T, S]]) = segments.filter(_.contains(value))

    case class Segment[T <: Comparable[_], S](from: T, to: T, name: S) {
        def contains(value: T) = from.asInstanceOf[Comparable[T]].compareTo(value) <= 0 && value.asInstanceOf[Comparable[T]].compareTo(to) < 0
    }

}
