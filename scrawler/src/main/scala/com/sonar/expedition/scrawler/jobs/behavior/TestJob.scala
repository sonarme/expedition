package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding.{Args, Tsv, Job}

class TestJob(args: Args) extends Job(args) {
    Tsv("scrawler/test.txt", ('a, 'b)).read.groupBy('a) {
        _.size.sortBy('b).take(2).toList[(Int, Int)](('a, 'b) -> ('c))
    }.write(Tsv("scrawler/testout"))
}
