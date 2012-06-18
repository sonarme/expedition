import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouper._
import util.matching.Regex
import grizzled.slf4j.Logging


class FriendGrouper(args: Args) extends Job(args) {

    val inputData = "/tmp/friendData.txt"
    val out = "/tmp/userGroupedFriends.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('id, 'frienddata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, friendData) => (userProfileId, friendData)
                case _ => ("1","2")
            }
        }
    }).groupBy('id) {

        group => group.toList[String]('frienddata,'iid)
    }.write(TextLine(out))

}


object FriendGrouper {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)""".r
}



