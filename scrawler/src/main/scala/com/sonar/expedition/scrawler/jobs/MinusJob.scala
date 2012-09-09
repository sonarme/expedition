package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, Args, Job}

class MinusJob(args: Args) extends Job(args) {
    Tsv(args("a"), 'a).leftJoinWithLarger('a -> 'b, Tsv(args("b"), 'b)).filter(('a, 'b)) {
        in: (String, String) => in._2 == null
    }.write(Tsv(args("c"), 'a))
}
