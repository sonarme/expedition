package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._

class ReachLoyaltyNormalization(args: Args) extends Job(args){

    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val sequenceInputStatic = args("sequenceInputStatic")
    val buzzTextFile = args("buzzScore")

    val buzz = TextLine(buzzTextFile).read
            .flatMapTo('line ->('venName, 'buzzCount, 'buzzScore, 'goldenId)) {
        line: String => {
            line match {
                case BuzzFromText(venName, buzzCount, buzzScore, goldenId) => {
                    Some((venName, buzzCount, buzzScore, goldenId))
                }
                case _ => {
                    println("Coudn't extract line using regex: " + line)
                    None
                }
            }
        }
    }

    val count = buzz.map(('buzzCount, 'goldenId) -> ('rowKey, 'columnName, 'columnValue)) {
        fields: (String, String) =>
            val (buzzcount, golden) = fields
            val row = golden + "_normalizedBuzz"
            val colName = "buzzCount"
            val colVal = buzzcount.toDouble
            (row, colName, colVal)
    }.project('rowKey, 'columnName, 'columnValue)

    val score = buzz.map(('buzzScore, 'goldenId) -> ('rowKey, 'columnName, 'columnValue)) {
        fields: (String, String) =>
            val (buzzscore, golden) = fields
            val row = golden + "_normalizedBuzz"
            val colName = "buzzScore"
            val colVal = buzzscore.toDouble
            (row, colName, colVal)
    }.project('rowKey, 'columnName, 'columnValue)


    val seqStatic = SequenceFile(sequenceInputStatic, Fields.ALL).read.mapTo((0, 1, 2) ->('rowKey, 'columnName, 'columnValue)) {
        fields: (String, String, Double) => fields
    }
            .map('rowKey ->('venue, 'category)) {
        rowKey: String => {
            val venuecat = rowKey.replaceFirst("_", " ").split(" ")
            val venue = venuecat.head
            val cat = venuecat.last
            (venue, cat)
        }
    }

    val loyalty = seqStatic
            .filter('category) {
        category: String => (category.equals("loyalty_visitCount") || category.equals("loyalty_customerCount"))
    }
            .groupBy('venue, 'category) {
        _.sum('columnValue -> 'columnSum)
    }
            .groupBy('venue) {
        _.toList[Double]('columnSum -> 'columnSumList)
    }
            .map('columnSumList -> 'loyaltyRawScore) {
        list: List[Double] => {
            val bigger = list.max
            val smaller = list.min
            bigger / smaller
        }
    }


    val loyaltyMin = loyalty.groupAll {
        _
                .min('loyaltyRawScore -> 'min)
    }.project('min)

    val loyaltyMax = loyalty.groupAll {
        _
                .max('loyaltyRawScore -> 'max)
    }.project('max)

    val normalizedLoyalty = loyalty
    .crossWithTiny(loyaltyMin)
    .crossWithTiny(loyaltyMax)
    .map(('loyaltyRawScore, 'min, 'max) -> ('normalizedLoyalty)) {
        fields: (Double, Double, Double) =>
            val (raw, min, max) = fields
            val normal = ((scala.math.log(raw) - scala.math.log(min)) / (scala.math.log(max) - scala.math.log(min))) * 98 + 1
            normal
    }.mapTo(('venue, 'normalizedLoyalty) -> ('rowKey, 'columnName, 'columnValue)) {
        fields: (String, Double) =>
            val (venue, normalLoyalty) = fields
            val rowkey = venue + "_loyalty"
            val colName = "normalizedLoyalty"
            (rowkey, colName, normalLoyalty)
    }


    val reach = seqStatic.filter('category) {
        category: String => category.equals("reach_distance")
    }
            .filter('columnName) {
        name: String => name.equals("meanDist")
    }
            .rename('columnValue -> 'reachRawScore)

    val reachMin = reach.groupAll {
        _
                .min('reachRawScore -> 'min)
    }.project('min)

    val reachMax = reach.groupAll {
        _
                .max('reachRawScore -> 'max)
    }.project('max)

    val normalizedReach = reach
            .crossWithTiny(reachMin)
            .crossWithTiny(reachMax)
            .map(('reachRawScore, 'min, 'max) -> ('normalizedReach)) {
        fields: (Double, Double, Double) =>
            val (raw, min, max) = fields
            val normal = ((scala.math.log(raw) - scala.math.log(min)) / (scala.math.log(max) - scala.math.log(min))) * 98 + 1
            normal
    }.mapTo(('venue, 'normalizedReach) -> ('rowKey, 'columnName, 'columnValue)) {
        fields: (String, Double) =>
            val (venue, normalReach) = fields
            val rowkey = venue + "_reach"
            val colName = "normalizedReach"
            (rowkey, colName, normalReach)
    }


    val normalizedScore = (normalizedReach ++ normalizedLoyalty ++ score)
            .map('rowKey ->('venue, 'category)) {
        rowKey: String => {
            val venuecat = rowKey.replaceFirst("_", " ").split(" ")
            val venue = venuecat.head
            val cat = venuecat.last
            (venue, cat)
        }
    }
            .groupBy('venue) {
        _.average('columnValue -> 'normalizedScore)
    }
    .mapTo(('venue, 'normalizedScore) -> ('rowKey, 'columnName, 'columnValue)) {
        fields: (String, Double) =>
            val (venue, normalScore) = fields
            val rowkey = venue + "_normalized"
            val colName = "normalizedScore"
            (rowkey, colName, normalScore)
    }




    val staticCassandra = (normalizedScore)
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueStatic",
            scheme = WideRowScheme(keyField = 'rowKey)
        )
    )

}
