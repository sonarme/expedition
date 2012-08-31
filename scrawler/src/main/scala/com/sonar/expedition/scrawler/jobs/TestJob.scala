package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import cascading.tuple.Fields

class TestJob(args: Args) extends Job(args) {
    Tsv("test.tsv", ('a, 'b)).read.groupBy('a) {
        _.sortedTake[String]('b -> 'grouped, 1).head('b)
    }.write(Tsv("test_out.tsv", Fields.ALL))
}
