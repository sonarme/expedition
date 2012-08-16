package com.sonar.expedition.scrawler.test

import com.twitter.scalding._
import util.matching.Regex

/**
 * Created with IntelliJ IDEA.
 * User: jyotirmoysundi
 * Date: 5/24/12
 * Time: 10:37 AM
 * To change this template use File | Settings | File Templates.
 */

/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

 */
class ReadSequenceFile(args: Args) extends Job(args) {
    //var output= "/tmp/output"
    //var inpfile="/tmp/stagedata.seq"
    //var inpfile="/tmp/hello"

    //var res = TextLine(inpfile).read.project('line).write(TextLine(output))
    //var srcScheme = WriterSeqFile(inpfile, ("key","value")).read.project(Fields.ALL).write(TextLine(output));
    //var srcScheme =  WriterSeqFile(inpfile,new Fields("key","value"))
    //var res= srcScheme.read.project(Fields.ALL).write((TextLine(output)))

    var inputData = args("sample")
    var out = args("out")


    var inp = TextLine(inputData).read.project('line).map('line ->('id, 'joblist)) {
        fields: (String) =>
            val (line) = fields
            var id = getId(line)
            var joblist: List[String] = getJobs(line)
            (id, joblist)

    }.project('id, 'joblist).write(TextLine(out))

    def getId(input: String): String = {
        input.substring(0, input.indexOf(":")).trim
    }

    def getJobs(input: String): List[String] = {
        var names: List[String] = List("Arnold", "George", "Obama")
        names = names ::: List("mein")
        input.substring(input.indexOf(":") + 1).split(",").toList ::: names
    }

}

object ReadSequenceFile {
    val Jobline: Regex = """\d*:*""".r
}

