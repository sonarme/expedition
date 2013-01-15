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

class NewAggregateMetricsJob(args: Args) extends DefaultJob(args) {
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val numCheckins = Tsv(args("numOutput"), ('goldenId, 'numCheckins, 'numPeople)).read
    val features = SequenceFile(args("rawoutput"), FeatureExtractions.RawTuple).read
            .groupBy('dealId, 'goldenId) {
        // count the features for the venue
        // using java map because of kryo problems
        _.foldLeft('features -> 'featuresCount)(Map.empty[String, Int]) {
            (agg: Map[String, Int], features: Set[String]) =>
                agg ++ features.map(feature => feature -> (agg.getOrElse(feature, 0) + 1))
        }

    }.joinWithSmaller('goldenId -> 'goldenId1, numCheckins.rename('goldenId -> 'goldenId1)).discard('goldenId1)

    val yelp = SequenceFile(args("yelp"), NewAggregateMetricsJob.Reviews).read.project('dealId, 'yrating, 'yreviewCount)
    val deals = SequenceFile(dealsOutput, DealAnalysis.DealsOutputTuple).read
            .leftJoinWithSmaller('dealId -> '_dealId, yelp.rename('dealId -> '_dealId)).discard('_dealId)
            .groupBy('dealId) {
        _.head(NewAggregateMetricsJob.DealDedupe -> NewAggregateMetricsJob.DealDedupe)
    }
    val fields = ('numCheckins, 'numPeople).append(NewAggregateMetricsJob.DealJoinFields)
    val fieldnames = fields.iterator().toList.map(_.toString)
    val results = features.discard('goldenId).leftJoinWithSmaller('dealId -> 'dealId1, deals.rename('dealId -> 'dealId1)).mapTo(('featuresCount).append(fields) -> 'json) {
        in: Tuple =>
            val features = in.getObject(0).asInstanceOf[Map[String, Int]]
            val dealMetrics = fieldnames.zip(in.tail) map {
                case (name, value) =>
                    name ->
                            (if (value == null || name == "rating") null
                            else if (name == "yrating") try {
                                value.toString.toDouble
                            } catch {
                                case _: NumberFormatException => null
                            }
                            else if (name.startsWith("num") || NewAggregateMetricsJob.IntValues(name)) try {
                                value.toString.toInt
                            } catch {
                                case _: NumberFormatException => null
                            } else value match {
                                case s: String => s.replaceAll("[\\s,`'\"/]", " ")
                                case x => x
                            })
            }
            import collection.JavaConversions._

            val result = NewAggregateMetricsJob.ObjectMapper.writeValueAsString(features ++ dealMetrics: java.util.Map[String, Any])
            result
    }
    results.write(TextLine(metricsOut))

}

import collection.JavaConversions._

object NewAggregateMetricsJob extends FieldConversions {
    val IntValues = Set("reviewCount", "purchased", "yreviewCount", "likes", "checkins")
    val AggregateDealTuple = ('enabled, 'dealId, 'successfulDeal, 'goldenId, 'venName, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint)
    val DealJoinFields = DealAnalysis.DealsOutputTuple.append(('yrating, 'yreviewCount))
    val DealDedupe = DealJoinFields.subtract('dealId)
    val ObjectMapper = new ObjectMapper
    ObjectMapper.disable(SerializationFeature.WRITE_NULL_MAP_VALUES)
    val Reviews =
        new Fields("dealId", "successfulDeal", "goldenId", "venName", "venueLat", "venueLng", "merchantName", "merchantAddress", "merchantCity", "merchantState", "merchantZip", "merchantPhone", "majorCategory", "minorCategory", "minPricepoint", "rating", "priceRange", "reviewCount", "likes", "purchased", "savingsPercent", "venueSector", "yurl", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews")


}
