package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import cascading.tuple.{Tuple, Fields}
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.expedition.scrawler.source.{LuceneSource, BerkeleyDBSource}
import elephantdb.DomainSpec
import elephantdb.persistence.JavaBerkDB
import elephantdb.cascading.ElephantDBTap.{Args => EArgs}

class TestJob(args: Args) extends DefaultJob(args) {
    IterableSource(Seq(1 -> 11, 1 -> 12, 1 -> 13, 2 -> 21, 2 -> 22, 3 -> 33), ('a, 'b)).read.groupBy('a) {
        _.sum('b -> 'sum)
                .sortWithTake(Fields.ALL -> 'x, 2) {
            (in: Tuple, in2: Tuple) =>
                println("G " + in)
                true
        }
    }.write(LuceneSource("TestLucene", 'sum))
}
