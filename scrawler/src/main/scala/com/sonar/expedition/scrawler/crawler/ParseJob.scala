package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.sonar.expedition.scrawler.util.Tuples

class ParseJob(args: Args) extends Job(args) with ParseFilter {

    val level: Int = args("level").toInt
    val srcDir = args("srcDir")

    val rawSequence = SequenceFile(srcDir + "/crawl_" + level + "/raw", Tuples.Crawler.Raw)

    val parsedTsv = Tsv(srcDir + "/crawl_" + level + "/parsedTsv", Tuples.Crawler.BaseVenue)
    val parsedSequence = SequenceFile(srcDir + "/crawl_" + level + "/parsedSequence", Tuples.Crawler.BaseVenue)

    val parsedTuples = rawSequence
                .filter('url) { url: String => url != null && isUrlIncluded(url)}
                .map(('url, 'content) -> ('businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone)) { in: (String, String) => {
                        val (url, content) = in
                        val extractor = ExtractorFactory.getExtractor(url, content)
                        val business = extractor.businessName()
                        val category = extractor.category()
                        val rating = extractor.rating()
                        val latitude = extractor.latitude()
                        val longitude = extractor.longitude()
                        val address = extractor.address()
                        val city = extractor.city()
                        val state = extractor.state()
                        val zip = extractor.zip()
                        val phone = extractor.phone()
                        val priceRange = extractor.priceRange()
                        val reviewCount = extractor.reviewCount()
                        val reviews = extractor.reviews()
                        val peopleCount = extractor.peopleCount()
                        val checkins = extractor.checkinCount()
                        val wereHereCount = extractor.wereHereCount()
                        val talkingAboutCount = extractor.talkingAboutCount()
                        val likes = extractor.likes()
                        val dealPrice = extractor.price()
                        val purchased = extractor.purchased()
                        val savingsPercent = extractor.savingsPercent()
                        val dealDescription = extractor.dealDescription()
                        val dealImage = extractor.dealImage()
                        val dealRegion = extractor.dealRegion()

                        (business, category, rating, latitude, longitude, address, city, state, zip, phone)
                    }
                }
                .discard('content, 'links, 'status)

        parsedTuples
            .write(parsedTsv)

        parsedTuples
            .write(parsedSequence)

}