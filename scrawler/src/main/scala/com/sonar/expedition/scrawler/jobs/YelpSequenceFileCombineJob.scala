package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}
import cascading.tuple.Fields

class YelpSequenceFileCombineJob(args: Args) extends Job(args) {
    val file1 = SequenceFile(args("input1"), NewAggregateMetricsJob.Reviews).read
    val file2 = SequenceFile(args("input2"), NewAggregateMetricsJob.Reviews).read
    val file3 = SequenceFile(args("input3"), NewAggregateMetricsJob.Reviews).read
    (file1 ++ file2 ++ file3).groupBy('dealId) {
        _.head(new Fields("successfulDeal", "goldenId", "venName", "venueLat", "venueLng", "merchantName", "merchantAddress", "merchantCity", "merchantState", "merchantZip", "merchantPhone", "majorCategory", "minorCategory", "minPricepoint", "rating", "priceRange", "reviewCount", "likes", "purchased", "savingsPercent", "venueSector", "yurl", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews")
                -> new Fields("successfulDeal", "goldenId", "venName", "venueLat", "venueLng", "merchantName", "merchantAddress", "merchantCity", "merchantState", "merchantZip", "merchantPhone", "majorCategory", "minorCategory", "minPricepoint", "rating", "priceRange", "reviewCount", "likes", "purchased", "savingsPercent", "venueSector", "yurl", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews"))
    }.write(SequenceFile(args("output"), NewAggregateMetricsJob.Reviews))
}

