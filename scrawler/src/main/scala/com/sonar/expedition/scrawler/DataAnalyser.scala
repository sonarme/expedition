import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import DataAnalyser._
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
class DataAnalyser(args: Args) extends Job(args) {

    var inputData = "/tmp/dataAnalyse.txt"
    var out = "/tmp/results2.txt"
    var data = (TextLine(inputData).read.project('line).map(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => (userProfileId, serviceType, json)
                case _ => ("", "", "")
            }
        }
    })

    var groupBy = data.groupBy('id) {
        _.toList[String]('jsondata -> 'json)
    }.
            //map(('id,'json) -> ('fbid,'fbjson,'lnid,'lnjson)){
            map(('id) -> ('data)) {

        //:wqid: String =>  id.substring(0)
        //var mapper = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(value.getBytes)); // can reuse, share globally

    }.write(TextLine(out))


}

object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
}



