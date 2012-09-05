package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import cascading.tuple.Fields

class TestJob(args: Args) extends Job(args) {
    Tsv("test.tsv", ('key, 'col, 'val)).read.groupBy('key) {
        _.pivot(('col, 'val) ->('x, 'y, 'z))
    }.write(Tsv("test_out.tsv", Fields.ALL))
}
