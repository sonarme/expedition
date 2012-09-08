package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import cascading.tuple.{Tuple, Fields}

class TestJob(args: Args) extends Job(args) {
    Tsv("test.tsv", ('a, 'b, 'c)).read.map('b -> 'b) {
        in: String => in + "_"
    }.write(Tsv("test_out.tsv", Fields.ALL, false, true))
}
