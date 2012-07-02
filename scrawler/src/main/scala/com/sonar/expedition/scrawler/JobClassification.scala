import com.sonar.expedition.scrawler.{StemAndMetaphoneEmployer, EmployerCheckinMatch, DTOProfileInfoPipe}
import com.twitter.scalding.{TextLine, Job, Args}
import util.matching.Regex
import JobClassification._

class JobClassification(args: Args) extends Job(args) {

    val chkininputData1 = TextLine("/tmp/dataAnalyse.txt")
    val output1 = TextLine("/tmp/jobcla.txt")

    val metaphoner1 = new StemAndMetaphoneEmployer
    val data1 = (chkininputData1.read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project('id, 'serviceType, 'jsondata)

    val dtoProfileGetPipe1 = new DTOProfileInfoPipe(args)
    val joinedProfiles1 = dtoProfileGetPipe1.getDTOWrkDescInfoInTuples(data1)

    val filteredProfiles1 = joinedProfiles1.project('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc).map('worktitle -> 'mtpworktitle) {
        fields: String =>
            val (worktitle) = fields
            val mtpworktitle = metaphoner1.getStemmedMetaphone(worktitle)
            mtpworktitle
    }.project('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'mtpworktitle, 'workdesc)
            .write(output1)


}

object JobClassification {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
}
