package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import CrawlAggregationJob._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}

class CrawlAggregationJob(args: Args) extends Job(args) {
    args("inputs").split(',').map {
        input =>
            val Array(serviceType, file) = input.split('@')
            SequenceFile(file, CrawlTuple).read.map('url -> 'venueId) {
                url: String => val id = url.stripSuffix("/").split('/').last.stripSuffix(".json")
                serviceType + ":" + id
            }
    }.reduce(_ ++ _).groupBy('venueId) {
        _.head(CrawlTuple)
    }.write(SequenceFile(args("output"), CrawlOutTuple))
}

object CrawlAggregationJob {
    val CrawlTuple = ('url, 'timestamp, 'business, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'reviews, 'peopleCount, 'checkins, 'wereHereCount, 'talkingAboutCount, 'likes)
    val CrawlOutTuple = ('venueId).append(CrawlTuple)
}

