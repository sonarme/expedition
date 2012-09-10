package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.{Tuple, Fields}
import com.twitter.scalding.Tsv
import scala.Some

class TestJob(args: Args) extends Job(args) with CheckinSource {
    val checkins = Tsv(args("input"), ('a, 'b, 'c)).read
    checkins.groupBy('a) {
        _.sortBy('b).head('c)
    }.write(Tsv(args("output"), ('a, 'c)))
}
