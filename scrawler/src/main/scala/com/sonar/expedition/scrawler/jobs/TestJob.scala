package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, TextLine, Job, Args}
import cascading.tuple.{Tuple, Fields}

class TestJob(args: Args) extends Job(args) with CheckinSource {
    val (checkins, _) = checkinSource(args, false, false)
    checkins.groupBy('venId) {
        _.foldLeft('keyid -> 'q)(Map.empty[String, Int]) {
            (agg: Map[String, Int], f: String) => agg + (f -> (agg.getOrElse(f, 0) + 1))
        }
    }.write(Tsv(args("output"), Fields.ALL, false, true))
}
