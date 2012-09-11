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
    //val numCheckins = Tsv(args("numOutput"), ('goldenId, 'numCheckins, 'numPeople)).read
    val features = SequenceFile(args("rawoutput"), FeatureExtractions.RawTuple).read
            .groupBy('dealId, 'goldenId) {
        // count the features for the venue
        // using java map because of kryo problems
        _.foldLeft('features -> 'featuresCount)(Map.empty[String, Int]) {
            (agg: Map[String, Int], features: Set[String]) =>
                agg ++ features.map(feature => feature -> (agg.getOrElse(feature, 0) + 1))
        }

    } /*.joinWithSmaller('goldenId -> 'goldenId1, numCheckins.rename('goldenId -> 'goldenId1)).discard('goldenId1)*/

    val yelp = SequenceFile(args("yelp"), NewAggregateMetricsJob.Reviews).read.project('dealId, 'yrating, 'yreviewCount)
    val deals = SequenceFile(dealsOutput, DealAnalysis.DealsOutputTuple).read
            .leftJoinWithSmaller('dealId -> '_dealId, yelp.rename('dealId -> '_dealId)).discard('_dealId)
    val fields = NewAggregateMetricsJob.NonFeatureTuple.append(('yrating, 'yreviewCount))
    val fieldnames = fields.iterator().toList.map(_.toString)
    val results = features.leftJoinWithSmaller(('dealId, 'goldenId) ->('dealId1, 'goldenId1), deals.rename(('dealId, 'goldenId) ->('dealId1, 'goldenId1))).mapTo(('featuresCount).append(fields) -> 'json) {
        in: Tuple =>
            val features = in.getObject(0).asInstanceOf[Map[String, Int]]
            val dealMetrics = fieldnames.zip(in.tail) map {
                case (name, value) =>
                    name -> (if (name.startsWith("num") || NewAggregateMetricsJob.IntValues(name)) try {
                        value.toString.toInt
                    } catch {
                        case _: NumberFormatException => 0
                    } else value)
            }
            import collection.JavaConversions._

            val result = NewAggregateMetricsJob.ObjectMapper.writeValueAsString(features ++ dealMetrics: java.util.Map[String, Any])
            result
    }
    results.write(TextLine(metricsOut))

}

import collection.JavaConversions._

object NewAggregateMetricsJob extends FieldConversions {
    val IntValues = Set("reviewCount", "rating", "purchased", "yreviewCount", "yrating", "likes", "checkins")
    val AggregateDealTuple = ('enabled, 'dealId, 'successfulDeal, 'goldenId, 'venName, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint)
    val NonFeatureTuple = /*('numCheckins, 'numPeople).append(*/ DealAnalysis.DealsOutputTuple /*)*/

    val ObjectMapper = new ObjectMapper
    ObjectMapper.disable(SerializationFeature.WRITE_NULL_MAP_VALUES)
    val DealMetrics = NonFeatureTuple.iterator().toIterable.map(_.toString)
    val Reviews =
        new Fields("dealId", "successfulDeal", "goldenId", "venName", "venueLat", "venueLng", "merchantName", "merchantAddress", "merchantCity", "merchantState", "merchantZip", "merchantPhone", "majorCategory", "minorCategory", "minPricepoint", "rating", "priceRange", "reviewCount", "likes", "purchased", "savingsPercent", "venueSector", "yurl", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews")
    /*

            new Fields("dealId", "successfulDeal", "merchantName", "majorCategory", "minorCategory", "minPricepoint", "address", "city", "state", "zip", "lat", "lng", "link", "ybusinessName", "ycategory", "rating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "reviewCount", "yreviews")
    */

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
