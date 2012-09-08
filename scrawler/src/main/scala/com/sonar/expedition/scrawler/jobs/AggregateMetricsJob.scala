package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.Tsv
import com.sonar.dossier.ScalaGoodies._
import com.twitter.scalding.SequenceFile
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

class AggregateMetricsJob(args: Args) extends Job(args) {
    val staticInputFiles = args("staticInputFiles").split(',')
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val yelpReviews = args("yelpReviews")
    val allPipes = staticInputFiles.map(file => SequenceFile(file, ('rowKey, 'columnName, 'value)).read).reduce(_ ++ _)
    val yelp = SequenceFile(yelpReviews, AggregateMetricsJob.YelpReviews).read.mapTo(('dealId, 'yrating, 'ypriceRange, 'yreviewCount) ->('dealId, 'yrating, 'ypriceRangeMetric, 'yreviewCount)) {
        in: (String, String, String, java.lang.Integer) =>
            val (dealId, yrating, ypriceRange, yreviewCount) = in
            (dealId, if (yrating == null || yrating == "") 0.0 else yrating.toDouble, ypriceRange.length, optionInteger(yreviewCount).getOrElse(0))
    }
    val metrics = allPipes.map(('rowKey, 'columnName) ->('venueId, 'metric)) {
        in: (String, String) =>
            val (rowKey, columnName) = in
            val Array(venueId, metricPrefix) = rowKey.split("_", 2)
            (venueId, metricPrefix + "_" + columnName)
    }.discard('rowKey, 'columnName)
    val deals = Tsv(dealsOutput, DealAnalysis.DealsOutputTuple).read.project(AggregateMetricsJob.AggregateDealTuple).filter('enabled) {
        enabled: Boolean => enabled
    }.map(('majorCategory, 'minorCategory, 'venName, 'merchantName) ->('majorCategory, 'minorCategory, 'venName, 'merchantName)) {
        in: (String, String, String, String) => ("\"" + in._1 + "\"", "\"" + in._2 + "\"", "\"" + in._3 + "\"", "\"" + in._4 + "\"")
    }
    val results = deals.leftJoinWithLarger('goldenId -> 'venueId, metrics)
            .groupBy(AggregateMetricsJob.AggregateDealTuple) {
        _.pivot(('metric, 'value) -> AggregateMetricsJob.MetricsFields)
    }
    results
            .leftJoinWithTiny('dealId -> 'yelpDealId, yelp.rename('dealId -> 'yelpDealId)).write(Csv(metricsOut, Fields.ALL))

}

object AggregateMetricsJob extends FieldConversions {
    val AggregateDealTuple = ('enabled, 'dealId, 'successfulDeal, 'goldenId, 'venName, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint)
    val YelpReviews =
        new Fields("dealId", "successfulDeal", "merchantName", "majorCategory", "minorCategory", "minPricepoint", "address", "city", "state", "zip", "lat", "lng", "yelpLink", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews")
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
        "age_25-35",
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
    val OutputFormat = (('dealId, 'successfulDeal, 'goldenId, 'venName, 'merchantName /*'venAddress, 'venuePhone,  'merchantAddress, 'merchantPhone, 'distance, 'levenshtein*/ ): Fields).append(MetricsFields).append(('yrating, 'ypriceRangeMetric, 'yreviewCount))
}

import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}

case class Csv(p: String, f: Fields = Fields.ALL) extends FixedPathSource(p)
with DelimitedScheme {
    override val fields = f
    override val separator = ","
    override val writeHeader = true

    override def hdfsScheme = {
        val scheme = new CHSequenceFile(fields).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
        scheme.setNumSinkParts(1)
        scheme
    }

}
