package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import filter.ParseFilter
import com.sonar.expedition.scrawler.util.Tuples

class ParseJob(args: Args) extends Job(args) with ParseFilter {

    val level: Int = args("level").toInt
    val levelUp: Int = level + 1
    val outputDir = args("output")
    val domains = args("domains")

    val rawSequence = SequenceFile(outputDir + "/crawl_" + level + "/raw", Tuples.RawCrawl)

    val parsed = Tsv(outputDir + "/crawl_" + level + "/parsed.tsv", ('url, 'timestamp, 'businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage))
    val parsedSequence = SequenceFile(outputDir + "/crawl_" + level + "/parsed", ('url, 'timestamp, 'businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage))

    val parsed2 = Tsv(outputDir + "/crawl_" + level + "/parsed2.tsv")
    val parsedSequence2 = SequenceFile(outputDir + "/crawl_" + level + "/parsed2", Fields.ALL)


    val parsedTuples = rawSequence
                .filter('url) { url: String => url != null && isUrlIncluded(url)}
                .map(('url, 'content) -> ('businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage)) { in: (String, String) => {
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

                        (business, category, rating, latitude, longitude, address, city, state, zip, phone, priceRange, reviewCount, likes, dealRegion, dealPrice, purchased, savingsPercent, dealDescription, dealImage)
                    }
                }
                .discard('content, 'links, 'status)

        parsedTuples
            .write(parsed)

        parsedTuples
            .write(parsedSequence)

    val parsedAgain = parsed
            .map(('url, 'dealPrice, 'purchased, 'savingsPercent) ->('dealPriceNum, 'purchasedNum, 'savingsPercentNum)) {
        in: (String, String, String, String) =>
            val (url, price, purchased, savings) = in
            val priceNum = try {
                price.substring(1).replace(",", "").toInt
            } catch {
                case e: Exception => println("url: " + url + "\n" + e); 0
            }
            val purchaseNum = try {
                purchased.replace(",", "").toInt
            } catch {
                case e: Exception => println("url: " + url + "\n" + e); try {
                    purchased.replace(",", "").replace("?", "").toInt
                } catch {
                    case f: Exception => 0
                }
            }
            val savingsNum = try {
                savings.stripSuffix("%").toInt
            } catch {
                case e: Exception => println("url: " + url + "\n" + e); 0
            }
            (priceNum, purchaseNum, savingsNum)
    }
            .discard('dealPrice, 'purchased, 'savingsPercent)
            .filter('dealPriceNum) { price: Int => price > 0}

    parsedAgain
        .write(parsed2)

    parsedAgain
        .write(parsedSequence2)
}

object ParseJob extends FieldConversions {
    val BaseVenueTuple = ('url, 'timestamp, 'businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone) append ('priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage)
    val LivingSocialTuple = BaseVenueTuple append ('priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage)
    val FoursquareTuple = BaseVenueTuple append ('priceRange, 'reviewCount, 'reviews)
    val FacebookTuple = BaseVenueTuple append ('priceRange, 'reviewCount, 'likes, 'peopleCount, 'checkins, 'wereHereCount, 'talkingAboutCount)
    val TwitterTuple = BaseVenueTuple
    val YelpTuple = BaseVenueTuple append ('priceRange, 'reviewCount, 'reviews)
    val CitySearchTuple = BaseVenueTuple append ('priceRange, 'reviewCount)
}