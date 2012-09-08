package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import CrawlAggregationJob._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import cascading.scheme.hadoop.SequenceFile
import com.twitter.scalding.SequenceFile
import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

class CrawlAggregationJob(args: Args) extends Job(args) {
    args("inputs").split(',').map {
        input =>
            val Array(serviceType, file) = input.split('@')
            SequenceFile(file, CrawlTuple).read.map('url -> 'venueId) {
                url: String => val id = url.stripSuffix("/").split('/').last.stripSuffix(".json")
                serviceType + ":" + id
            }
    }.reduce(_ ++ _).write(OneSequenceFile(args("output"), CrawlOutTuple))
}

object CrawlAggregationJob {
    val CrawlTuple = ('url, 'timestamp, 'business, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'reviews, 'peopleCount, 'checkins, 'wereHereCount, 'talkingAboutCount, 'likes)
    val CrawlOutTuple = ('venueId, 'url, 'timestamp, 'business, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'reviews, 'peopleCount, 'checkins, 'wereHereCount, 'talkingAboutCount, 'likes)
}

import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}

case class OneSequenceFile(p: String, f: Fields = Fields.ALL) extends FixedPathSource(p) with SequenceFileScheme {
    override val fields = f

    override def hdfsScheme = {
        val scheme = new CHSequenceFile(fields).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
        scheme.setNumSinkParts(1)
        scheme
    }
}
