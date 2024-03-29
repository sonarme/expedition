package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Tsv, TextLine, Args, Job}
import java.io.File
import com.sonar.expedition.scrawler.jobs.DefaultJob

class DeleteSlf4jJob(args: Args) extends DefaultJob(args) {

    val dummyFile = args.getOrElse("dummyFile", "s3n://scrawler/dummyFile.txt")

    val readPipe = TextLine(dummyFile)
            .flatMap('line -> 'word) {
        line: String => {
            val slf4jVersion = args.getOrElse("version", "1.4.3")

            val localLog4jFile = new File("/home/hadoop/lib/slf4j-log4j12-" + slf4jVersion + ".jar")
            val localApiFile = new File("/home/hadoop/lib/slf4j-api-" + slf4jVersion + ".jar")

            val successfulLog4jDelete: Boolean = if (localLog4jFile.exists()) {
                localLog4jFile.delete()
            } else {
                false
            }

            val successfulApiDelete: Boolean = if (localApiFile.exists()) {
                localApiFile.delete()
            } else {
                false
            }
            //println("deleted slf4j files locally: " + (if (successfulApiDelete) "yes" else "no") + " " + (if (successfulLog4jDelete) "yes" else "no"))
            line.split( """\s+""")

        }
    }
            .groupBy('word) {
        _.size
    }
            .write(Tsv(args("output")))


}
