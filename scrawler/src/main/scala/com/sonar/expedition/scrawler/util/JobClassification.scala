package com.sonar.expedition.scrawler.util

import com.sonar.expedition.scrawler.Jobs.StemAndMetaphoneEmployer
import com.sonar.expedition.scrawler.Jobs.{StemAndMetaphoneEmployer}
import com.sonar.expedition.scrawler.pipes.{EmployerCheckinMatch, DTOProfileInfoPipe}
import com.twitter.scalding.{RichPipe, TextLine, Job, Args}
import util.matching.Regex
import JobClassification._

class JobClassification(args: Args) extends Job(args) {

    def getJobTypePipes(path: RichPipe): RichPipe = {
        //val chkininputData1 = TextLine(path)
        var data1 = (path.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
            line: String => {
                line match {
                    case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                    case _ => List.empty
                }
            }
        }).project('id, 'serviceType, 'jsondata)

        val dtoProfileGetPipe1 = new DTOProfileInfoPipe(args)
        val joinedProfiles1 = dtoProfileGetPipe1.getWrkDescProfileTuples(data1)
        val filteredProfiles1 = joinedProfiles1.project('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
        filteredProfiles1

    }

    def getOccupationCodes(path: String): RichPipe = {
        val jobtypePipe = TextLine(path).read.project('line).mapTo('line ->('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)) { //cpsocc: CPS OCCUPATION { {
            line: String => {
                line match {
                    case Occupation(matrixocccode, matrixocctitle, cpscode, cpsocctite) => (matrixocccode, matrixocctitle, cpscode, cpsocctite)
                    case _ => ("None", "None", "None", "None")
                }
            }

        }.project('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)

        jobtypePipe
    }
}

object JobClassification {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
    val Occupation: Regex = """([a-zA-Z\d\-]+),([a-zA-Z\d\-]+),([a-zA-Z\d\-]+),([a-zA-Z\d\-]+)""".r
}
