package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.{Tuple, Fields}
import com.twitter.scalding.Tsv
import scala.Some

class TestJob(args: Args) extends Job(args) with CheckinSource {
    val checkins = args.optional("input") match {
        case Some(input) => Tsv(input, ('venId, 'keyid, 'c)).read: RichPipe
        case _ => checkinSource(args, false, false)._1
    }
    checkins.map('venId -> 'bla) {
        x: String =>
            val set = ((1 to 10000) map (x + _)).toSet[String]
            set
    }.groupBy('venId) {
        _.foldLeft('keyid -> 'q)(Map.empty[String, Int]) {
            (agg: Map[String, Int], f: String) => agg + (f -> (agg.getOrElse(f, 0) + 1))
        }
    }.write(SequenceFile(args("output"), 'q))
}
