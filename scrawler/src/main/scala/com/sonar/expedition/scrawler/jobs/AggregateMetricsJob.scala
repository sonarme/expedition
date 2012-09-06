package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv

class AggregateMetricsJob(args: Args) extends Job(args) {
    val staticInputFiles = args("staticInputFiles").split(',')
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val yelpReviews = args("yelpReviews")
    val allPipes = staticInputFiles.map(file => SequenceFile(file, ('rowKey, 'columnName, 'columnValue)).read).reduce(_ ++ _)

    val yelp = SequenceFile(yelpReviews, AggregateMetricsJob.YelpReviews).read.project('dealId, 'yrating, 'ypriceRange, 'yreviewCount).map('ypriceRange -> 'ypriceRangeMetric) {
        ypriceRange: String => ypriceRange.length
    }
    val metrics = allPipes.mapTo(('rowKey, 'columnName, 'columnValue) ->('venueId, 'metric, 'value)) {
        in: (String, String, Double) =>
            val (rowKey, columnName, columnValue) = in
            val Array(venueId, metricPrefix) = rowKey.split("_", 2)
            (venueId, metricPrefix + "_" + columnName, columnValue)
    }
    SequenceFile(dealsOutput, DealAnalysis.DealsOutputTuple).leftJoinWithLarger('goldenId -> 'venueId, metrics)
            .groupBy(DealAnalysis.DealsOutputTuple) {
        _.pivot(('metric, 'value) -> AggregateMetricsJob.MetricsFields)
    }.leftJoinWithTiny('dealId -> 'yelpDealId, yelp.rename('dealId -> 'yelpDealId)).write(Tsv(metricsOut, AggregateMetricsJob.OutputFormat))

}

object AggregateMetricsJob extends FieldConversions {
    val MetricsFields = new Fields("loyalty_customerCount_Passers-By", "loyalty_customerCount_Regulars", "loyalty_customerCount_Addicts", "loyalty_visitCount_Passers-By", "loyalty_visitCount_Regulars", "loyalty_visitCount_Addicts", "numCheckins_count", "reach_distance_meanDist", "reach_distance_stdevDist", "reach_distance_latitude", "reach_distance_longitude", "reach_originCount_numHome", "reach_originCount_numWork", "age_<18", "age_18-24", "age_25-45", "age_35-44", "age_45-54", "age_55-64", "age_65+", "gender_male", "gender_female", "education_College", "education_No College", "education_Grad School", "education_unknown", "income_$0-50k", "income_$50-100k", "income_$100-150k", "income_$150k+")
    val OutputFormat = (DealAnalysis.DealsOutputTuple: Fields).append(MetricsFields).append(('yrating, 'ypriceRangeMetric, 'yreviewCount))
    val YelpReviews =
        new Fields("dealId", "successfulDeal", "merchantName", "majorCategory", "minorCategory", "minPricepoint", "address", "city", "state", "zip", "lat", "lng", "yelpLink", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews")
}

// ((numCheckins_count,112.0), (loyalty_visitCount_Addicts,112.0), (loyalty_customerCount_Addicts,1.0), (reach_originCount_numWork,0.0), (reach_distance_latitude,40.813189281932864), (reach_distance_longitude,-73.94112270377491), (reach_distance_meanDist,12.2684546586255), (reach_distance_stdevDist,0.020099797206679218), (reach_originCount_numHome,112.0))
