package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import cascading.tuple.{Tuple, Fields}

class TestJob(args: Args) extends Job(args) {
    Tsv(args("input"), ('a, 'b, 'c)).read.groupBy('a) {
        _.foldLeft('b -> 'q)(Map.empty[String, String]) {
            (agg: Map[String, String], f: String) => agg + (f -> "x")
        }
    }.write(Tsv(args("output"), Fields.ALL, false, true))
}
