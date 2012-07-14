package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Args, TextLine, RichPipe, Job}
import util.matching.Regex
import JobCodeReader._

class JobCodeReader(args: Args) extends Job(args) {
    def readJobTypes(incoming: RichPipe): RichPipe = {
        val pipe = incoming.project('line)
                .mapTo('line ->('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)) {
            line: String => {
                line match {
                    case Occupation(matrixocccode, matrixocctitle, cpscode, cpsocctite) => (matrixocccode, matrixocctitle, cpscode, cpsocctite)
                    case _ => ("None", "None", "None", "None")
                }
            }

        }
        .project('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)

        pipe
    }
}

object  JobCodeReader{
    val Occupation: Regex = """([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)\t([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)""".r

}
