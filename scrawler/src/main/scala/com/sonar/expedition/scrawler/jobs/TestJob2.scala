package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.Tsv
import scala.Some

class TestJob2(args: Args) extends Job(args) with CheckinSource {
    SequenceFile(args("input"), 'q).read.map('q -> 'a) {
        in: Map[String, Int] =>
            in.toString
    }.write(Tsv(args("output"), Fields.ALL, false, true))
}
