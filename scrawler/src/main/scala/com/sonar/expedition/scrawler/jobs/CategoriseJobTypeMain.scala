package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.pipes.{JobImplicits, DTOProfileInfoPipe}

import com.twitter.scalding.{TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.objs.serializable.LuceneIndex

/*  com.sonar.expedition.scrawler.jobs.CategoriseJobTypeMain --hdfs --occupationCodetsv "/tmp/occupationCodetsv.txt" --JobClassifiedOutput "/tmp/JobClassifiedOutput" --serviceProfileData "/tmp/serviceProfileData.txt"

*/
class CategoriseJobTypeMain(args: Args) extends Job(args) with DTOProfileInfoPipe {

    val output3 = TextLine(args("JobClassifiedOutput"))

    //todo use LuceneTFIDFUtils in utils scala object after fixing the error
    var lucene = new LuceneIndex()
    lucene.initialise
    val codes = TextLine(args("occupationCodetsv")).read.project('line)
            .flatMapTo('line ->('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)) {
        line: String => {
            line match {
                case Occupation(matrixocccode, matrixocctitle, cpscode, cpsocctite) => List((matrixocccode, matrixocctitle, cpscode, cpsocctite))
                case _ => List.empty
            }
        }

    }
            .project(('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite))
            .mapTo(('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite) ->('matrixocccode1, 'matrixocctitle1, 'cpscode1, 'cpsocctite1)) {
        fields: (String, String, String, String) =>
            val (matrixocccode, matrixocctitle, cpscode, cpsocctite) = fields
            lucene.addItems(cpsocctite, matrixocctitle)
            (matrixocccode, matrixocctitle, cpscode, cpsocctite)
    }
            .project(('matrixocccode1, 'matrixocctitle1, 'cpscode1, 'cpsocctite1))


    val data1 = (TextLine(args("serviceProfileData")).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val joinedProfiles = getDTOProfileInfoInTuples(data1)
    val filteredProfiles = joinedProfiles.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle)).map('worktitle -> 'worktitle1) {
        fields: (String) =>
            val (job) = fields
            val jobtype = lucene.search(job.mkString.trim)
            jobtype
    }.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'worktitle1)).filter('worktitle1) {
        work: String => (!work.equalsIgnoreCase("unclassified"))
    }.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'worktitle1)).write(output3)
}

object CategoriseJobTypeMain {

}

