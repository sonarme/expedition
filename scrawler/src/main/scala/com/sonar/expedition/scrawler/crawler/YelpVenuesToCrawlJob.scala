package com.sonar.expedition.scrawler.crawler

import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}

class YelpVenuesToCrawlJob(args: Args) extends Job(args) {

    val input = args("input")
    val output = args("output")

    val rawSequence = SequenceFile(input, YelpCrawl.DealsOutputTuple append('url, 'status, 'content, 'links))
    val venuesToCrawl = Tsv(output, YelpCrawl.DealsOutputTuple append ('url))


    rawSequence
        .filter('content) {
            content: String => content.contains("Sorry, you're not allowed to access this page")
        }
        .discard('status, 'content, 'links)
        .write(venuesToCrawl)

}