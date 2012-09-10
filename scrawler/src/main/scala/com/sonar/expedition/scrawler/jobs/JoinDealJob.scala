package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile

class JoinDealJob(args: Args) extends Job(args) {
    val input = Csv(args("input"),
        new Fields("enabled", "dealId", "successfulDeal", "goldenId", "venName", "merchantName", "majorCategory", "minorCategory", "minPricepoint", "loyalty_customerCount_Passers-By", "loyalty_customerCount_Regulars", "loyalty_customerCount_Addicts", "loyalty_visitCount_Passers-By", "loyalty_visitCount_Regulars", "loyalty_visitCount_Addicts", "numCheckins_all", "numCheckins_withProfile", "reach_distance_meanDist", "reach_distance_stdevDist", "reach_originCount_numHome", "reach_originCount_numWork", "ageAve", "ageStdev", "age_<18", "age_18-24", "age_25-35", "age_35-44", "age_45-54", "age_55-64", "age_65+", "gender_male", "gender_female", "education_College", "education_No College", "education_Grad School", "education_unknown", "income_$0-50k", "income_$50-100k", "income_$100-150k", "income_$150k+", "yrating", "ypriceRangeMetric", "yreviewCount", "yelpDealId")).read
    val ls = SequenceFile(args("livingsocial"), ('url, 'timestamp, 'merchantName, 'majorCategory, 'rating, 'merchantLat, 'merchantLng, 'merchantAddress, 'city, 'state, 'zip, 'merchantPhone, 'priceRange, 'reviewCount, 'likes, 'dealDescription, 'dealImage, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent)).read.map('url -> 'dealId1) {
        url: String => url.split('/').last.split('-').head
    }.discard('dealDescription)
    /*.project('dealId1, 'reviewCount, 'likes, 'dealPrice, 'purchased/*, 'city, 'state, 'zip*/, 'merchantPhone, 'priceRange/*, 'dealDescription*/, 'dealImage, 'dealRegion, 'savingsPercent)/*.map('dealDescription -> 'editedDealDescription) {
        (descriptionField: String) => descriptionField match {
            case (description: String) if description != null => description.replaceAll( """\s""", "_")
            case _ => ""
        }
    }*/*/.map(('dealPrice, 'purchased) -> ('revenue, 'dealSuccess)) {
        in: (Double, Int) => {
        val (dealPrice, purchased) = in
            val revenue = dealPrice * purchased
            val dealSuccess = revenue > 1800
            (revenue, dealSuccess)
        }
    }
    val reveneAnalysisPipe = ls.groupAll(_.sizeAveStdev('revenue -> ('revenueCount, 'revenueAverage, 'revenueStdDev))
            /*.max('revenue -> 'maxRevenue)
            .min('revenue -> 'minRevenue)*/)

    val lsWithStats = ls.crossWithTiny(reveneAnalysisPipe).map(('revenue, 'revenueAverage, 'revenueStdDev) -> 'revenueBucket) {
        in: (Double, Double, Double) =>
            val (revenue, revenueAverage, revenueStdDev) = in
            val offset = math.round(revenueAverage / revenueStdDev)
            offset + math.round((revenue - revenueAverage) / revenueStdDev)
    }

    /*val lsSample = lsWithStats.limit(1000).groupAll(_.sortedTake[Int]('revenue -> 'sortedRevenue, 1000).mapStream('sortedRevenue -> 'revenueBuckets) {
        in: (Double) => {

        }
    })
*/

    input.leftJoinWithLarger('dealId -> 'dealId1, lsWithStats).write(Tsv(args("output"), Fields.ALL, true, true))
}

