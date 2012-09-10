package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import cascading.tuple.Fields

class ParseJob(args: Args) extends Job(args) {

    val level: Int = args("level").toInt
    val levelUp: Int = level + 1
    val outputDir = args("output")
    val domains = args("domains")

    val parsed = Tsv(outputDir + "/crawl_" + level + "/parsed.tsv", ('url, 'timestamp, 'businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage))
    val parsedSequence = SequenceFile(outputDir + "/crawl_" + level + "/parsed", ('url, 'timestamp, 'businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone, 'priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage))

    val parsed2 = Tsv(outputDir + "/crawl_" + level + "/parsed2.tsv")
    val parsedSequence2 = SequenceFile(outputDir + "/crawl_" + level + "/parsed2", Fields.ALL)


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