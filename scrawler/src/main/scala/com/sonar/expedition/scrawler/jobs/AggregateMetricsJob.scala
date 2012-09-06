package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.sonar.dossier.ScalaGoodies._

class AggregateMetricsJob(args: Args) extends Job(args) {
    val staticInputFiles = args("staticInputFiles").split(',')
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val yelpReviews = args("yelpReviews")
    val allPipes = staticInputFiles.map(file => SequenceFile(file, ('rowKey, 'columnName, 'columnValue)).read).reduce(_ ++ _)

    val yelp = SequenceFile(yelpReviews, AggregateMetricsJob.YelpReviews).read.mapTo(('dealId, 'yrating, 'ypriceRange, 'yreviewCount) ->('dealId, 'yrating, 'ypriceRangeMetric, 'yreviewCount)) {
        in: (String, String, String, java.lang.Integer) =>
            val (dealId, yrating, ypriceRange, yreviewCount) = in
            (dealId, if (yrating == null || yrating == "") 0.0 else yrating.toDouble, ypriceRange.length, optionInteger(yreviewCount).getOrElse(0))
    }
    val metrics = allPipes.mapTo(('rowKey, 'columnName, 'columnValue) ->('venueId, 'metric, 'value)) {
        in: (String, String, java.lang.Double) =>
            val (rowKey, columnName, columnValue) = in
            val Array(venueId, metricPrefix) = rowKey.split("_", 2)
            (venueId, metricPrefix + "_" + columnName, optionDouble(columnValue).getOrElse(0.0))
    }
    Tsv(dealsOutput, DealAnalysis.DealsOutputTuple).filter('enabled) {
        enabled: Boolean => enabled
    }.leftJoinWithLarger('goldenId -> 'venueId, metrics)
            .groupBy(DealAnalysis.DealsOutputTuple) {
        _.pivot(('metric, 'value) -> AggregateMetricsJob.MetricsFields)
    }.leftJoinWithTiny('dealId -> 'yelpDealId, yelp.rename('dealId -> 'yelpDealId)).write(Csv(metricsOut, AggregateMetricsJob.OutputFormat))

}

object AggregateMetricsJob extends FieldConversions {
    val MetricsFields = new Fields("loyalty_customerCount_Passers-By", "loyalty_customerCount_Regulars", "loyalty_customerCount_Addicts", "loyalty_visitCount_Passers-By", "loyalty_visitCount_Regulars", "loyalty_visitCount_Addicts", "numCheckins_count", "reach_distance_meanDist", "reach_distance_stdevDist", "reach_distance_latitude", "reach_distance_longitude", "reach_originCount_numHome", "reach_originCount_numWork", "age_<18", "age_18-24", "age_25-45", "age_35-44", "age_45-54", "age_55-64", "age_65+", "gender_male", "gender_female", "education_College", "education_No College", "education_Grad School", "education_unknown", "income_$0-50k", "income_$50-100k", "income_$100-150k", "income_$150k+")
    val OutputFormat = (('dealId, 'successfulDeal /*, 'goldenId, 'venName, 'venAddress, 'venuePhone, 'merchantName, 'merchantAddress, 'merchantPhone, 'distance, 'levenshtein*/ ): Fields).append(MetricsFields).append(('yrating, 'ypriceRangeMetric, 'yreviewCount))
    val YelpReviews =
        new Fields("dealId", "successfulDeal", "merchantName", "majorCategory", "minorCategory", "minPricepoint", "address", "city", "state", "zip", "lat", "lng", "yelpLink", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews")
}

case class Csv(p: String, f: Fields = Fields.ALL) extends FixedPathSource(p)
with DelimitedScheme {
    override val fields = f
    override val separator = ","
}
