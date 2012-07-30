package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import util.matching.Regex

import com.twitter.scalding._
import com.twitter.scalding.TextLine
import com.sonar.expedition.scrawler.util.LuceneIndex
import com.sonar.expedition.scrawler.util.CommonFunctions._

class CategoriseJobTypeMain(args: Args) extends Job(args) {

    //val types = getJobTypePipes(TextLine("/tmp/dataAnalyse.txt").read.project('line))
    val output1 = TextLine("/tmp/output1")
    val output2 = TextLine("/tmp/output2")
    val output3 = TextLine("/tmp/output3")

    //todo use LuceneTFIDFUtils in utils scala object after fixing the error
    var lucene = new LuceneIndex()
    lucene.initialise
    val codes = TextLine("/tmp/occupationCodetsv.txt").project('line)
            .mapTo('line ->('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)) {
        line: String => {
            line match {
                case Occupation(matrixocccode, matrixocctitle, cpscode, cpsocctite) => (matrixocccode, matrixocctitle, cpscode, cpsocctite)
                case _ => ("None", "None", "None", "None")
            }
        }

    }
            .project('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)
            .mapTo(('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite) ->('matrixocccode1, 'matrixocctitle1, 'cpscode1, 'cpsocctite1)) {
        fields: (String, String, String, String) =>
            val (matrixocccode, matrixocctitle, cpscode, cpsocctite) = fields
            lucene.addItems(cpsocctite, matrixocctitle)
            (matrixocccode, matrixocctitle, cpscode, cpsocctite)
    }
            .project('matrixocccode1, 'matrixocctitle1, 'cpscode1, 'cpsocctite1)
            .write(output1)

    val data1 = (TextLine("/tmp/serviceProfileDatasmall.txt").read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project('id, 'serviceType, 'jsondata)

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data1)
    val filteredProfiles = joinedProfiles.project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle).map('worktitle -> 'worktitle1) {
        fields: (String) =>
            val (job) = fields
            val jobtype = lucene.search(job.mkString.trim)
            jobtype
    }.project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'worktitle1).filter('worktitle1) {
        work: String => (!work.equalsIgnoreCase("unclassified"))
    }.project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'worktitle1).write(output3)
}

object CategoriseJobTypeMain {


}
