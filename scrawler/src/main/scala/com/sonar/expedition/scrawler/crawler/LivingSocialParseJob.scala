package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.util.Tuples

class LivingSocialParseJob(args: Args) extends Job(args) with ParseFilter {
    val level: Int = args("level").toInt
    val srcDir = args("srcDir")

    val rawSequence = SequenceFile(srcDir + "/crawl_" + level + "/raw", Tuples.Crawler.Raw)

    val parsedTsv = Tsv(srcDir + "/crawl_" + level + "/parsedTsv", Tuples.Crawler.LivingSocial)
    val parsedSequence = SequenceFile(srcDir + "/crawl_" + level + "/parsedSequence", Tuples.Crawler.LivingSocial)

    val parsedTuples = rawSequence
            .filter('url) { url: String => url != null && isUrlIncluded(url)}
            .mapTo(('url, 'timestamp, 'content) -> Tuples.Crawler.LivingSocial) { in: (String, String, String) => {
                    val (url, timestamp, content) = in
                    val extractor = ExtractorFactory.getExtractor(url, content)

                    (url, timestamp, extractor.businessName(), extractor.category(), extractor.rating(), extractor.latitude(), extractor.longitude(), extractor.address(), extractor.city(), extractor.state(), extractor.zip(), extractor.phone(), extractor.priceRange(), extractor.reviewCount(), extractor.likes(), extractor.dealRegion(), extractor.price(), extractor.purchased(), extractor.savingsPercent(), extractor.dealDescription(), extractor.dealImage())
                }
            }

        parsedTuples
            .write(parsedTsv)

        parsedTuples
            .write(parsedSequence)
}