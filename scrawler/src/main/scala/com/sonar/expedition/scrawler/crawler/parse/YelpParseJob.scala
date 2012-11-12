package com.sonar.expedition.scrawler.crawler.parse

import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.crawler.{ExtractorFactory, ParseFilter}
import com.sonar.expedition.scrawler.util.Tuples

class YelpParseJob(args: Args) extends Job(args) with ParseFilter {
    val level: Int = args("level").toInt
    val srcDir = args("srcDir")

    val rawSequence = SequenceFile(srcDir + "/crawl_" + level + "/raw", Tuples.Crawler.Raw)

    val parsedTsv = Tsv(srcDir + "/crawl_" + level + "/parsedTsv", Tuples.Crawler.Yelp)
    val parsedSequence = SequenceFile(srcDir + "/crawl_" + level + "/parsedSequence", Tuples.Crawler.Yelp)

    val parsedTuples = rawSequence
            .filter('url) { url: String => url != null && isUrlIncluded(url)}
            .mapTo(('url, 'timestamp, 'content) -> Tuples.Crawler.Yelp) { in: (String, String, String) => {
                    val (url, timestamp, content) = in
                    val extractor = ExtractorFactory.getExtractor(url, content)

                    (url, timestamp, extractor.businessName(), extractor.category(), extractor.rating(), extractor.latitude(), extractor.longitude(), extractor.address(), extractor.city(), extractor.state(), extractor.zip(), extractor.phone(), extractor.priceRange(), extractor.reviewCount(), extractor.reviews())
                }
            }

        parsedTuples
            .write(parsedTsv)

        parsedTuples
            .write(parsedSequence)
}