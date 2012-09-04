package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import cascading.tuple.Fields

class TestJob(args: Args) extends Job(args) {
    Tsv("test.tsv", ('a, 'bStr)).read.map('bStr -> 'b) {
        in: Double => in
    }.groupAll {
        _.sortBy('b)
    }.write(Tsv("test_out.tsv", Fields.ALL))
}
