package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Job, Args}
import CrawlAggregationJob._

class CrawlAggregationJob(args: Args) extends Job(args) {
    args("inputs").split(',').map {
        input =>
            val Array(serviceType, file) = input.split('@')
            SequenceFile(file, CrawlTuple).read.map('url -> 'venueId) {
                url: String => val id = url.stripSuffix("/").split('/').last.stripSuffix(".json")
                serviceType + ":" + id
            }
    }.reduce(_ ++ _).write(SequenceFile(args("output"), CrawlOutTuple))
}

object CrawlAggregationJob {
    val CrawlTuple = ('url, 'timestamp, 'business, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'reviews, 'peopleCount, 'checkins, 'wereHereCount, 'talkingAboutCount, 'likes)
    val CrawlOutTuple = ('venueId, 'url, 'timestamp, 'business, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'reviews, 'peopleCount, 'checkins, 'wereHereCount, 'talkingAboutCount, 'likes)
}
