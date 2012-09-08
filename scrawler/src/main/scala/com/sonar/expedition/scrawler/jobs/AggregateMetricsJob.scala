package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.{Tuple, Fields}
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.sonar.dossier.ScalaGoodies._
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import ch.hsr.geohash.GeoHash

class AggregateMetricsJob(args: Args) extends Job(args) {
    val placeClassification = args("placeClassification")
    val staticInputFiles = args("staticInputFiles").split(',')
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val yelpReviews = args("yelpReviews")
    val allPipes = staticInputFiles.map(file => SequenceFile(file, ('rowKey, 'columnName, 'columnValue)).read).reduce(_ ++ _)

    def metricGeosector(lat: Double, lng: Double) = GeoHash.withBitPrecision(lat, lng, 15).longValue()

    val geosectors = SequenceFile(placeClassification, PlaceClassification.PlaceClassificationOutputTuple).read.project('goldenId, 'venueLat, 'venueLng).map(('venueLat, 'venueLng) -> 'geosector) {
        in: (Double, Double) =>
            val (lat, lng) = in
            metricGeosector(lat, lng)
    }.discard('venueLat, 'venueLng)

    val metrics = allPipes.mapTo(('rowKey, 'columnName, 'columnValue) ->('venueId, 'metric, 'value)) {
        in: (String, String, java.lang.Double) =>
            val (rowKey, columnName, columnValue) = in
            val Array(venueId, metricPrefix) = rowKey.split("_", 2)
            (venueId, metricPrefix + "_" + columnName, optionDouble(columnValue).getOrElse(0.0))
    }.groupBy('venueId) {
        _.pivot(('metric, 'value) -> AggregateMetricsJob.MetricsFields)
    }
    val metricsWithGeosector = metrics.joinWithLarger('venueId -> 'goldenId, geosectors).discard('goldenId)
    val deals = Tsv(dealsOutput, DealAnalysis.DealsOutputTuple).read
    val dealGeosectors = deals.mapTo(('merchantLat, 'merchantLng) -> 'merchantGeosector) {
        in: (Double, Double) =>
            val (lat, lng) = in
            metricGeosector(lat, lng)
    }.unique('merchantGeosector)

    val relevantMetrics = metricsWithGeosector.joinWithSmaller('geosector -> 'merchantGeosector, dealGeosectors).discard('merchantGeosector)
    mapRelativeWinner(relevantMetrics, "gender_winner",
        "gender_male",
        "gender_female")
    val geosectorMetrics = relevantMetrics.groupBy('geosector) {
        _.sizeAveStdev('ageAve ->('_size1, 'sectorAgeAve, 'sectorAgeStdev))
                .sizeAveStdev('gender_male_rel ->('_size2, 'sectorGenderMaleRelAve, 'sectorGenderMaleRelStdev))
                .sizeAveStdev('numCheckins_all ->('_size3, 'sectorNumCheckins_allAve, 'sectorNumCheckins_allStdev))
                .sizeAveStdev('meanDist ->('_size4, 'sectorMeanDistAve, 'sectorMeanDistStdev))
    }.discard('_size1, '_size2, '_size3, '_size4)

    /*val geoSectorMetrics = metrics.groupBy('venueId) {
        _.sizeAveStdev('ageAve->('_size1,'sectorAve,'))
    }*/


    val results = deals
            .discard('goldenId, 'venName, 'venAddress, 'venuePhone, 'merchantName, 'merchantLat, 'merchantLng, 'merchantAddress, 'merchantPhone, 'distance, 'levenshtein)
            .map(('majorCategory, 'minorCategory) ->('majorCategory, 'minorCategory)) {
        in: (String, String) => ("'" + in._1 + "'", "'" + in._2 + "'")
    }
            .filter('enabled) {
        enabled: Boolean => enabled
    }.leftJoinWithLarger('goldenId -> 'venueId, relevantMetrics)
    results.map(('ageAve, 'gender_male_rel, 'numCheckins_all, 'meanDist) ->('ageAve_stdevGs, 'gender_male_rel_stdevGs, 'numCheckins_all_stdevGs, 'meanDist_stdevGs)) {
        in: (Double, Double, Int, Double) =>

    }
    mapRelativeWinner(results, "age_winner",
        "age_<18",
        "age_18-24",
        "age_25-45",
        "age_35-44",
        "age_45-54",
        "age_55-64",
        "age_65+")

    mapRelativeWinner(results, "education_winner",
        "education_College",
        "education_No College",
        "education_Grad School",
        "education_unknown")

    mapRelativeWinner(results, "loyalty_customerCount_winner",
        "loyalty_customerCount_Passers-By",
        "loyalty_customerCount_Regulars",
        "loyalty_customerCount_Addicts")
    mapRelativeWinner(results, "loyalty_visitCount_winner",
        "loyalty_visitCount_Passers-By",
        "loyalty_visitCount_Regulars",
        "loyalty_visitCount_Addicts")
    readCrawl(results, yelpReviews, "yelp")
    results
            .write(Csv(metricsOut, Fields.ALL))

    /* def stdev(in: RichPipe, fields: String*) = {
        val fieldsList = fields.toList
        val outputFields = fieldsList.map(_ + "_stdevGs")
        in.map(new Fields(fields: _*) -> new Fields(outputFields: _*)) {
            in: Tuple =>
                val doubles = in.iterator().map(x => x.asInstanceOf[java.lang.Double]: Double)
                val total = integers.sum
                val results = integers.map {
                    el => (if (total == 0) Double.NaN else el.toDouble / total): java.lang.Double
                }.toList
                val maxIndex = results.indexOf(results.max)
                val winner = if (maxIndex >= 0) fieldsList(maxIndex) else ""
                new Tuple((winner :: results): _*)
        }
    }*/

    def mapRelativeWinner(in: RichPipe, winner: String, fields: String*) = {
        val fieldsList = fields.toList
        val outputFields = winner :: fieldsList.map(_ + "_rel")
        in.map(new Fields(fields: _*) -> new Fields(outputFields: _*)) {
            in: Tuple =>
                val integers = in.iterator().map(x => x.asInstanceOf[Integer]: Int)
                val total = integers.sum
                val results = integers.map {
                    el => (if (total == 0) Double.NaN else el.toDouble / total): java.lang.Double
                }.toList
                val maxIndex = results.indexOf(results.max)
                val winner = if (maxIndex >= 0) fieldsList(maxIndex) else ""
                new Tuple((winner :: results): _*)
        }
    }

    def readCrawl(in: RichPipe, file: String, crawler: String) = {
        val fields = List("dealId", "_successfulDeal", "_merchantName", "_majorCategory", "_minorCategory", "_minPricepoint", "_address", "_city", "_state", "_zip", "_lat", "_lng", "Link", "businessName", "category", "rating", "latitude", "longitude", "address", "city", "state", "zip", "phone", "priceRange", "reviewCount", "reviews").map(crawler + "_" + _)
        val important = List("dealId", "rating", "priceRange", "reviewCount").map(crawler + "_" + _)

        val crawl = SequenceFile(file, new Fields(fields: _*)).read.project(new Fields(important: _*)).map(new Fields(crawler + "_priceRange") -> new Fields(crawler + "_ypriceRangeMetric")) {
            priceRange: String => priceRange.length
        }
        val crawlerDealId = new Fields(crawler + "_dealId")
        in.leftJoinWithTiny('dealId -> crawlerDealId, crawl).discard(crawlerDealId)
    }
}

object AggregateMetricsJob extends FieldConversions {
    val MetricsFields = new Fields(
        "loyalty_customerCount_Passers-By",
        "loyalty_customerCount_Regulars",
        "loyalty_customerCount_Addicts",
        "loyalty_visitCount_Passers-By",
        "loyalty_visitCount_Regulars",
        "loyalty_visitCount_Addicts",
        "numCheckins_all",
        "numCheckins_withProfile",
        "reach_distance_meanDist",
        "reach_distance_stdevDist",
        "reach_originCount_numHome",
        "reach_originCount_numWork",
        "ageAve",
        "ageStdev",
        "age_<18",
        "age_18-24",
        "age_25-45",
        "age_35-44",
        "age_45-54",
        "age_55-64",
        "age_65+",
        "gender_male",
        "gender_female",
        "education_College",
        "education_No College",
        "education_Grad School",
        "education_unknown",
        "income_$0-50k",
        "income_$50-100k",
        "income_$100-150k",
        "income_$150k+"
    )


}

case class Csv(p: String, f: Fields = Fields.ALL) extends FixedPathSource(p)
with DelimitedScheme {
    override val fields = f
    override val separator = ","
    override val writeHeader = true
}
