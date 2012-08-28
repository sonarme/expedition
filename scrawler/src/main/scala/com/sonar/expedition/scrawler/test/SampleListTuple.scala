package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, RichPipe, Args}
import com.twitter.scalding.Job

class SampleListTuple(args: Args) extends Job(args) {

    var inputData = args("sample")
    var out = args("out")

    var inp = TextLine(inputData).read.project('line).map('line ->('id, 'joblist)) {
        fields: (String) =>
            val (line) = fields
            var id = getId(line)
            var joblist: List[String] = getJobs(line)
            (id, joblist)

    }.project('id, 'joblist)

    //.write(TextLine(out))


    def getId(input: String): String = {
        input.substring(0, input.indexOf(":")).trim
    }

    def getJobs(input: String): List[String] = {
        var names: List[String] = List("Arnold", "George", "Obama")
        names = names ::: List("mein")
        input.substring(input.indexOf(":") + 1).split(",").toList ::: names
    }
}
