package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, SequenceFile, Args, Job}
import cascading.tuple.Fields

class YelpSequenceFileCombineJob(args: Args) extends Job(args) {
    val merged = args("inputs").split(',').map {
        file => SequenceFile(file, NewAggregateMetricsJob.Reviews).read
    }.reduce(_ ++ _)
    merged.groupBy('dealId) {
        _.head(new Fields("successfulDeal", "goldenId", "venName", "venueLat", "venueLng", "merchantName", "merchantAddress", "merchantCity", "merchantState", "merchantZip", "merchantPhone", "majorCategory", "minorCategory", "minPricepoint", "rating", "priceRange", "reviewCount", "likes", "purchased", "savingsPercent", "venueSector", "yurl", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews")
                -> new Fields("successfulDeal", "goldenId", "venName", "venueLat", "venueLng", "merchantName", "merchantAddress", "merchantCity", "merchantState", "merchantZip", "merchantPhone", "majorCategory", "minorCategory", "minPricepoint", "rating", "priceRange", "reviewCount", "likes", "purchased", "savingsPercent", "venueSector", "yurl", "ybusinessName", "ycategory", "yrating", "ylatitude", "ylongitude", "yaddress", "ycity", "ystate", "yzip", "yphone", "ypriceRange", "yreviewCount", "yreviews"))
    }.write(SequenceFile(args("output"), NewAggregateMetricsJob.Reviews))
            .write(Tsv(args("output") + "_tsv", NewAggregateMetricsJob.Reviews))
}

