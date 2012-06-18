import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.CheckinObjects

//import com.sonar.expedition.scrawler.MeetupCrawler
import com.twitter.scalding._
import java.nio.ByteBuffer
import CheckinGrouper._
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import com.sonar.dossier.dto.{Checkin, ServiceProfileDTO}


class CheckinGrouper(args: Args) extends Job(args) {

    var inputData = "/tmp/checkinData.txt.small"
    var out = "/tmp/userGroupedCheckins.txt"
    //   logger.debug(checkin.getUserProfileId() + "::" + checkin.getServiceType() + "::" + checkin.getServiceProfileId() + "::" + checkin.getServiceCheckinId() + "::" + checkin.getVenueName() + "::" + checkin.getVenueAddress() + "::" + checkin.getCheckinTime() + "::" + checkin.getGeohash() + "::" + checkin.getLatitude() + "::" + checkin.getLongitude() + "::" + checkin.getMessage())
    var data = (TextLine(inputData).read.project('line).map(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message)) {
        line: String => {
            line match {
                case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message)
                case _ => ("None","None","None","None","None","None","None","None","None","None","None")
            }
        }
    }).pack[CheckinObjects](('serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message) -> 'checkin).groupBy('userProfileId) {
        //        var packedData = data.pack[Checkin](('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message) -> 'person)
        //        group => group.toList[Tuple8[String,String,String,String,String,String,String,String]](packedData,'iid)
        //_.sortBy('userProfileId)
        group => group.toList[CheckinObjects]('checkin,'checkindata)
    }.map(Fields.ALL -> 'ProfileId, 'lat){
        fields : (String,List[CheckinObjects]) =>
        val (userid,checkins)    = fields
        val lat = getLatitute(checkins)
        (userid,lat)
    }.write(TextLine(out))

//    var test = dataTuple(2)._5.write(TextLine(out))

    def getLatitute(checkins:List[CheckinObjects]) : String ={
        val latitude="";
        checkins foreach{
            venue =>
                latitude ++ venue.getLatitude.toString+",";
        }
        latitude
    }

}


object CheckinGrouper {
//    val ExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}



