package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.{Tuple, Fields}
import com.twitter.scalding.Tsv
import com.sonar.dossier.ScalaGoodies._
import com.twitter.scalding.SequenceFile
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import com.fasterxml.jackson.databind.{SerializationFeature, DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class NewAggregateMetricsJob(args: Args) extends Job(args) {
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val features = SequenceFile(args("features"), FeatureExtractions.OutputTuple).read

    val deals = Tsv(dealsOutput, DealAnalysis.DealsOutputTuple).read
            .leftJoinWithSmaller('dealId -> '_dealId, reviews("yelp", args("yelp"))).discard('_dealId)
    val fields = NewAggregateMetricsJob.NonFeatureTuple.append((List("yelp") map NewAggregateMetricsJob.reviewTuple) reduce (_ append _))
    val fieldnames = fields.iterator().toList.map(_.toString)
    val results = features.leftJoinWithSmaller('goldenId -> 'goldenId1, deals.rename('goldenId -> 'goldenId1)).mapTo(('featuresCount).append(fields) -> 'json) {
        in: Tuple =>
            val features = in.getObject(0).asInstanceOf[Map[String, Int]]
            val dealMetrics = fieldnames.zip(in.tail)
            import collection.JavaConversions._

            val result = NewAggregateMetricsJob.ObjectMapper.writeValueAsString(features ++ dealMetrics: java.util.Map[String, Any])
            result
    }
    results.write(TextLine(metricsOut))

    def reviews(name: String, file: String) = SequenceFile(file, NewAggregateMetricsJob.Reviews).read.project('dealId, 'rating, 'reviewCount).rename('dealId -> '_dealId).rename(('rating, 'reviewCount) -> NewAggregateMetricsJob.reviewTuple(name))
}

import collection.JavaConversions._

object NewAggregateMetricsJob extends FieldConversions {
    val AggregateDealTuple = ('enabled, 'dealId, 'successfulDeal, 'goldenId, 'venName, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint)
    val NonFeatureTuple = ('numCheckins, 'numCheckinsWithProfile).append(DealAnalysis.DealsOutputTuple)

    val ObjectMapper = new ObjectMapper
    ObjectMapper.disable(SerializationFeature.WRITE_NULL_MAP_VALUES)
    val DealMetrics = NonFeatureTuple.iterator().toIterable.map(_.toString)
    val Reviews =
        new Fields("dealId", "successfulDeal", "merchantName", "majorCategory", "minorCategory", "minPricepoint", "address", "city", "state", "zip", "lat", "lng", "link", "ybusinessName", "ycategory", "rating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "reviewCount", "yreviews")

    def reviewTuple(name: String) = new Fields(name + "_rating", name + "_reviewCount")

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
