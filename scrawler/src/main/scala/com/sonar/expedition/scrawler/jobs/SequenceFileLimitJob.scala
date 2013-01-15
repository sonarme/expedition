package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Args, Job}

class SequenceFileLimitJob(args: Args) extends DefaultJob(args) {
    val input = args("input")
    val output = args("output")
    val limit = args("limit").toInt
    SequenceFile(input).limit(limit).write(SequenceFile(output))
}

