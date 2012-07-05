package com.sonar.expedition.scrawler.test

import com.twitter.scalding._

class WordCountJob(args: Args) extends Job(args) {
    TextLine(args("input")).read.
            flatMap('line -> 'word) {
        line: String => line.split("\\s+")
    }.
            groupBy('word) {
        _.size
    }.
            write(Tsv(args("output")))
}
