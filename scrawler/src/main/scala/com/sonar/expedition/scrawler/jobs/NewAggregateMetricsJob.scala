package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.{Tuple, Fields}
import com.twitter.scalding.Tsv
import com.sonar.dossier.ScalaGoodies._
import com.twitter.scalding.SequenceFile
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

class NewAggregateMetricsJob(args: Args) extends Job(args) {
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    //val features = SequenceFile(args("features"), FeatureExtractions.OutputTuple).read

    val deals = Tsv(dealsOutput, DealAnalysis.DealsOutputTuple).read
    //reviews(args,deals)
    val results = deals /*.leftJoinWithLarger('goldenId -> 'venueId, features)*/ .mapTo(NewAggregateMetricsJob.JsonTuple -> 'json) {
        in: Tuple =>
            val features = in.getObject(0).asInstanceOf[Map[String, Int]]
            val dealMetrics = NewAggregateMetricsJob.DealMetrics.zip(in.tail)
            DealAnalysis.DealObjectMapper.writeValueAsString(features ++ dealMetrics)
    }
    results.write(TextLine(metricsOut))
    /*
 def reviews(args:Args, name:String, in:RichPipe) = {

        for (crawl <- args.getOrElse("yelp","").split(',') if crawl.nonEmpty) {
             val Array(name,file) = crawl.split('@')
            val reviews = SequenceFile(file, NewAggregateMetricsJob.Reviews).read.project('dealId, 'rating, 'reviewCount).rename(('dealId, 'rating, 'reviewCount) -> new Fields("dealId1",name + "_rating", name + "_reviewCount"))
            in.leftJoinWithSmaller('dealId->'dealId1,reviews).discard('dealId1)
        }
        in
    }*/
}

import collection.JavaConversions._

object NewAggregateMetricsJob extends FieldConversions {
    val AggregateDealTuple = ('enabled, 'dealId, 'successfulDeal, 'goldenId, 'venName, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint)
    val JsonTuple = /*('feature).append(*/ DealAnalysis.DealsOutputTuple
    /*)*/
    val DealMetrics = DealAnalysis.DealsOutputTuple.iterator().toIterable.map(_.toString)
    /*

        val Reviews =
            new Fields("dealId", "successfulDeal", "merchantName", "majorCategory", "minorCategory", "minPricepoint", "address", "city", "state", "zip", "lat", "lng", "link", "ybusinessName", "ycategory", "rating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "reviewCount", "yreviews")
    */
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
