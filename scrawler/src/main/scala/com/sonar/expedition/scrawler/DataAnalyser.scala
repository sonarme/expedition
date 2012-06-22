import cascading.tuple.Fields
import com.restfb.types.User.Education
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO}
import com.sonar.expedition.scrawler.{HttpClientRest, FriendObjects, CheckinObjects, uniqueCompanies}
import com.twitter.scalding._
import java.nio.ByteBuffer
import DataAnalyser._
import java.security.MessageDigest
import scala.{Some, Option}
import util.matching.Regex

import scala.collection.JavaConversions._
import util.parsing.json.{JSONArray, JSONObject}

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
    var out = "/tmp/results7.txt"
    //TextLine(inputData).read.project('line).write(TextLine(out))
    var data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project('id, 'serviceType, 'jsondata)


    val numProfiles = data.groupBy('id) {
        _.size
    }.rename('size -> 'numProfiles)

    //var out3 = TextLine("/tmp/data1234.txt")
    var out2 = TextLine("/tmp/data123.txt")

    /*var rest1 = data.groupBy('id) {group =>
        group.toList[String]('serviceType,'jsondata)
    }

    var rest2 = data.groupBy('id) {group =>
        group.toList[String]('jsondata,'serviceType)
    }.joinWithSmaller('id->'id,rest1).project('id,'jsondata,'serviceType).write(out2)
    */

    // Merge `serviceType` with `numProfiles`, by joining on their id fields.

    val profiles = data.joinWithSmaller('id -> 'id, numProfiles)

    //join profiles againts itself to get all corelated ids(fb and linked correlation i am talking about :) )


    val dupProfiles = profiles.rename(('id, 'serviceType, 'jsondata, 'numProfiles) ->('id2, 'serviceType2, 'jsondata2, 'numProfiles2))


    val joinedProfiles = profiles.joinWithSmaller('id -> 'id2, dupProfiles).project('id, 'serviceType, 'jsondata, 'serviceType2, 'jsondata2)
            .mapTo(Fields.ALL -> Fields.ALL) {
        fields: (String, String, String, String, String) =>
            val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
            var value2 = getFBId(serviceType, serviceType2)
            var value3 = getFBJson(serviceType, serviceType2, jsondata)
            var value4 = getLinkedInId(serviceType, serviceType2)
            var value5 = getLNKDINJson(serviceType, serviceType2, jsondata2)
            (fields._1, value2, value3, value4, value5)
    }
            .mapTo(Fields.ALL -> Fields.ALL) {
        fields: (String, String, Option[String], String, Option[String]) =>
            val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
            var fbid = getID(jsondata)
            var lnid = getID(jsondata2)
            (id, fbid, jsondata, lnid, jsondata2)
    } //.write(out2)
            .mapTo(Fields.ALL ->('rowkey, 'username, 'fbid, 'lnid, 'fbedu, 'lnedu, 'fbwork, 'lnwork, 'city)) {
        fields: (String, Option[String], Option[String], Option[String], Option[String]) =>
            val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
            var fbid = getID(jsondata)
            var lnid = getID(jsondata2)
            var fbedu = getEducation(jsondata)
            var lnedu = getEducation(jsondata2)
            var fbwork = getWork(jsondata)
            var lnwork = getWork(jsondata2)
            var fbusername = getUserName(jsondata)
            var fbcity = getCity(jsondata)
            var lncity = getCity(jsondata2)
            var city = formCityList(fbcity, lncity)
            //var currwork = getWork(jsondata,jsondata2)
            //var currcity = getCity(jsondata,jsondata2)
            //var name
            //var worktitle
            //var workhistory = getWorkHistory(jsondata,jsondata2)
            //var locationhistory
            (fields._1, fbusername, fbid, lnid, fbedu, lnedu, fbwork, lnwork, city)
    }.map(Fields.ALL ->('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity)) {
        fields: (String, Option[String], Option[String], Option[String], List[UserEducation], List[UserEducation], List[UserEmployment], List[UserEmployment], List[String]) =>
            val (rowkey, fbname, fbid, lnid, fbedu, lnedu, fbwork, lnwork, city) = fields
            val edulist = formEducationlist(fbedu, lnedu)
            val worklist = formWorkHistoryList(fbwork, lnwork)
            (rowkey, fbname, fbid, lnid, edulist, worklist, city)
    }
            .project(('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity))
            .mapTo(Fields.ALL ->('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)) {
        fields: (String, Option[String], Option[String], Option[String], scala.collection.immutable.List[UserEducation], scala.collection.immutable.List[UserEmployment], List[String]) =>
            val (rowkey, fbname, fbid, lnid, edu, work, city) = fields

            val educationschool = getFirstEdu(edu)
            val edudegree = getFirstEduDegree(edu)
            var eduyear = getFirstEduDegreeYear(edu)

            val workcomp = getFirstWork(work)
            val worktitle = getWorkTitle(work)
            val workdesc = getWorkSummary(work)

            val ccity = getcurrCity(city)

            //(rowkey, fbname.mkString, md5SumString(fbid.mkString.getBytes("UTF-8")), md5SumString(lnid.mkString.getBytes("UTF-8")), educationschool.mkString, workcomp.mkString, ccity.mkString, edudegree.mkString, eduyear.mkString, worktitle.mkString, workdesc.mkString)
            (rowkey, fbname.mkString, fbid.mkString, lnid.mkString, educationschool.mkString, workcomp.mkString, ccity.mkString, edudegree.mkString, eduyear.mkString, worktitle.mkString, workdesc.mkString)

        //}.project('key, 'name, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
    }

    val companies = joinedProfiles.project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'workdesc)
            .filter('worked) {
        name: String => !name.trim.toString.equals("")
    }.project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'workdesc)
    //.unique('worked)
    //.write(TextLine(out))
    val tmpcompanies = companies.project('worked, 'uname, 'city, 'worktitle, 'workdesc, 'fbid, 'lnid)


    //find companies with uqniue coname and city
    val unq_cmp_city = tmpcompanies.unique('worked, 'city, 'fbid, 'lnid)

    val city_latlong = unq_cmp_city.unique('worked, 'city).mapTo(Fields.ALL ->('work, 'cname, 'lat, 'long)) {
        fields: (String, String) =>
            val (work, city) = fields
            val location = getLatLongCity(city)
            val locationregex(lat, long) = location
            (work, city, lat, long)

    }.project('work, 'cname, 'lat, 'long).mapTo(Fields.ALL ->('workname, 'placename, 'lati, 'longi, 'street_address)) {
        fields: (String, String, String, String) =>
            val (work, city, lat, long) = fields
            if (city == null) {

                (work, city, "", "", "")
            } else {
                val locationworkplace = fourSquareCall(work, lat, long)
                val locationofficeregex(lati, longi, pincode) = locationworkplace
                (work, city, lati, longi, pincode)
            }

    }.project('workname, 'placename, 'lati, 'longi, 'street_address)

    val work_loc = city_latlong
            .filter('lati, 'longi, 'street_address) {
        fields: (String, String, String) =>
            val (lat, lng, addr) = fields
            (!lat.toString.equalsIgnoreCase("-1")) && (!lng.toString.equalsIgnoreCase("-1")) && (!addr.toString.equalsIgnoreCase("-1"))

    }.write(TextLine("/tmp/work_place.txt"))

    val joinedwork_location = tmpcompanies.joinWithSmaller('city -> 'placename, city_latlong)
            .project('worked, 'uname, 'placename, 'lati, 'longi, 'street_address, 'worktitle, 'workdesc, 'fbid, 'lnid)
            .unique('worked, 'uname, 'placename, 'lati, 'longi, 'street_address, 'worktitle, 'workdesc, 'fbid, 'lnid)
            .filter('lati, 'longi, 'street_address) {
        fields: (String, String, String) =>
            val (lat, lng, addr) = fields
            (!lat.toString.equalsIgnoreCase("-1")) && (!lng.toString.equalsIgnoreCase("-1")) && (!addr.toString.equalsIgnoreCase("-1"))

    }
            .write(out2)
    //val company_locand_users = joinedwork_location.mapTo(Fields.ALL ->())

    //code starts to find correlation between comapnies.
    /*val tmpuniquecompanies= tmpcompanies.unique('worked,'city,'worktitle,'workdesc).mapTo('worked -> ('works,'cpid)){
        worked:String =>
        (worked,0)
    }.project('works,'cpid)

    val dup_tmpcompanies = tmpuniquecompanies.rename(('works,'cpid), ('works2,'cpid2))

    val unique_cos = tmpuniquecompanies.joinWithLarger('cpid->'cpid2,dup_tmpcompanies).project('works,'works2).mapTo(Fields.ALL -> ('works1,'works2,'linkw,'corltn)){
        fields: (String, String) =>
        val (wrk1, wrk2) = fields
        if(wrk1.contains(wrk2) || wrk2.contains(wrk1)){
            val linkweight = uniqueCompanies.relevantScore(wrk1,wrk2)
            val companiesregex(linkweght,corltn)=linkweight
            (wrk1, wrk2, linkweght.mkString.toDouble,corltn.mkString.toDouble)

        }else{
            val linkweight = "0:0"
            val companiesregex(linkweght,corltn)=linkweight
            (wrk1, wrk2, linkweght.mkString.toDouble,corltn.mkString.toDouble)
        }
        //val companiesregex(linkweght,corltn)=linkweight
        //(wrk1, wrk2, linkweght.mkString.toDouble,corltn.mkString.toDouble)
        /*linkweight: String => {
            line match {
                case companiesregex(linkweght,corltn) => (wrk1, wrk2, linkweght.mkString.toDouble,corltn.mkString.toDouble)
                case _ => ("None", "None", "None", "None")
            }
        }*/

    }

    .filter('works1, 'works2) { works : (String, String) => works._1 >= works._2 }
    .project('works1,'works2,'linkw,'corltn)

     val tmpcmp_filter = unique_cos.filter('linkw,'corltn) {
        fields : (Double, Double) =>
        val (lnw, corl) = fields
        (lnw > 50 ) ||  (corl > 0.1)
    }
    /*
    a a      sonarme sonar c d
    sonar sonar  sonarme sonar c d
    sonarme sonarme sonarme sonar c d
    sonarme sonar  sonarme sonar   c d

    a a      path.to path2 c d
    sonar sonar path.to path2 c d
    sonarme sonarme path.to path2 c d
    sonarme sonar  path.to path2   c d
      */
    val tmpcp1 = tmpcmp_filter.filter('works1,'works2){
        works : (String, String) => works._1 != works._2
    }.rename(('works1,'works2,'linkw,'corltn)->('works3,'works4,'linkw1,'corltn1))
    /*
   sonarme sonar  a b
    */

    val unique_cos_tmp = tmpcmp_filter.joinWithSmaller('works2 -> 'works3 ,tmpcp1)
    .project('works2,'works4)
            /*

    sonar sonar sonarme sonar
    sonarme sonarme sonarme sonar
    sonarme sonar sonarme sonar

    .filter('works2,'works11){
        works : (String, String) => works._1 == works._2
    }        */
    /*.unique('works2)*/
    //.project('works11)
    .write(TextLine(out))
    //code end to find correlation between comapnies.
    */

    var finp = "/tmp/frienddata.txt"
    var frout = "/tmp/userGroupedFriends.txt"


    var friends = (TextLine(finp).read.project('line).map(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)) {
        line: String => {
            line match {
                case DataExtractLineFriend(id, other2, serviceID, serviceType, friendName) => (id, serviceType, serviceID, friendName)
                case NullNameExtractLine(id, other2, serviceID, serviceType, friendName) => (id, serviceType, serviceID, friendName)
                case _ => ("None", "None", "None", "None")
            }
        }
    })
            .project(Fields.ALL).discard(0).map(Fields.ALL ->('key, 'serType, 'serProfileId, 'friName)) {
        fields: (String, String, String, String) =>
            val (userid, serviceType, serviceProfileId, friendName) = fields
            //val hashedServiceProfileId = md5SumString(serviceProfileId.getBytes("UTF-8"))
            val hashedServiceProfileId = serviceProfileId
            (userid, serviceType, hashedServiceProfileId, friendName)
    }.project('key, 'serType, 'serProfileId, 'friName).write(TextLine(frout))


    var chkininputData = "/tmp/checkinDatatest.txt"
    var chkinout = "/tmp/hasheduserGroupedCheckins.txt"

    var chkindata = (TextLine(chkininputData).read.project('line).map(('line) ->('userProfileID, 'serviceType, 'serviceProfileID, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude)) {
        line: String => {
            line match {
                case chkinDataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
                case _ => ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None")
            }
        }
    })
            .project(Fields.ALL).discard(0).map(Fields.ALL ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng)) {
        fields: (String, String, String, String, String, String, String, String, String, String) =>
            val (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng) = fields
            //val hashedServiceID = md5SumString(serviceID.getBytes("UTF-8"))
            val hashedServiceID = serviceID
            (id, serviceType, hashedServiceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng)
    }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng)
    //.write(TextLine(chkinout))

    val frnd_chkinjoin = companies.joinWithLarger('key -> 'keyid, chkindata).project('key, 'uname, 'fbid, 'lnid, 'worked, 'serType, 'serProfileID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng)
            .write(TextLine(chkinout))


    /*.map(Fields.ALL -> ('ProfileId, 'serviceType, 'serviceProfileId, 'friendName)){
       fields : (String,List[FriendObjects]) =>
           val (userid, friends)    = fields
           val friendName = getFriendName(friends)
           (userid, serviceType, serviceProfileId, friendName)
   }.project('ProfileId, 'friendName).write(TextLine(frout))*/

    //.map(Fields.ALL -> ('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity) )


    /*val joinedProfileswork =   joinedProfiles
            .flatMapTo('skey,'work -> ('rkey, 'worked)){
        fields: (String, Option[UserEmployment]) =>
        val (rowkey, work) = fields
        val getfirstjob=getFirstWork(work)

        (rowkey,getfirstjob)

    }*/
    //.write(TextLine(out))
    //    .project('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity).write(TextLine(out))

    /*def getFirstWork(works: Option[UserEmployment]): String = {
        Option(works.get.getCompanyName()).toString
    } */
    //.pack[ProfileAnalysis](('sonarkey, 'height) -> 'person)
    /*.map(Fields.ALL -> ('skey,'fid,'lid,'edu)){
        fields: (String, String, String, Some[List[UserEducation]] , Some[List[UserEducation]]) =>
        val (rowkey, fbid, lnid, fbedu, lnedu) = fields
        var edulist=fbedu+lnedu
        (rowkey,fbid,lnid,edulist)

    }*/
    //.discard(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //map()

    //var writejoinedProfiles= joinedProfiles.project('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity).write(TextLine(out))

    /*var friendlist = (TextLine("/tmp/frienddatatest.txt").read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsonfrnddata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project('id, 'serviceType, 'jsonfrnddata).groupBy('id) {
        group => group.toList[String]('jsonfrnddata, 'frndlist)
    }

    //var writefriendlist= friendlist.write(TextLine("/tmp/friendout.txt"))

    val profile_wth_frnd_list = joinedProfiles.joinWithLarger('skey -> 'id, friendlist).project('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity, 'frndlist)
    /*.map(Fields.ALL ->('rkey, 'fbname, 'fbid, 'lnid, 'edulist, 'worklist, 'curcity, 'friendlist)) {
   fields: (String, Option[String], Option[String], Option[String], Option[List[UserEducation]], Option[List[UserEmployment]], List[String], List[String]) =>
       val (rowkey, fbname, fbid, lnid, edu, work, city, frndlist) = fields
       //var parsedfrndList = getFriendsList(frndlist);
       (rowkey, fbname, fbid, lnid, edu, work, city, frndlist)

}     */
    //.project('rkey, 'fbname, 'fbid, 'lnid, 'edulist, 'worklist, 'curcity, 'friendlist)
    // .write(TextLine("/tmp/profile_wth_frnd_list.txt"))

    val checkin_inputData = "/tmp/checkinData.txt.small"
    //val checkin_output = "/tmp/userGroupedCheckins.txt"
    //   logger.debug(checkin.getUserProfileId() + "::" + checkin.getServiceType() + "::" + checkin.getServiceProfileId() + "::" + checkin.getServiceCheckinId() + "::" + checkin.getVenueName() + "::" + checkin.getVenueAddress() + "::" + checkin.getCheckinTime() + "::" + checkin.getGeohash() + "::" + checkin.getLatitude() + "::" + checkin.getLongitude() + "::" + checkin.getMessage())
    var checkindata = (TextLine(checkin_inputData).read.project('line).map(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message)) {
        line: String => {
            line match {
                case DataExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message)
                case _ => ("None", "None", "None", "None", "None", "None", "None", "None", "None", "None", "None")
            }
        }
    }).pack[CheckinObjects](('serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message) -> 'checkin).groupBy('userProfileId) {
        //        var packedData = data.pack[Checkin](('userProfileId, 'serviceType, 'serviceProfileId, 'serviceCheckinID, 'venueName, 'venueAddress, 'checkinTime, 'geohash, 'latitude, 'longitude, 'message) -> 'person)
        //        group => group.toList[Tuple8[String,String,String,String,String,String,String,String]](packedData,'iid)
        //_.sortBy('userProfileId)
        group => group.toList[CheckinObjects]('checkin, 'checkindata)
    }.project('userProfileId, 'checkindata)

    /*.map(Fields.ALL -> ('ProfileId, 'lat)){
fields : (String,List[CheckinObjects]) =>
   val (userid ,checkins)    = fields
   val lat = getLatitute(checkins)
   (userid,lat)
}.project('ProfileId,'lat)
   .write(TextLine(checkin_output)) */

    val profile_wth_frnd_list_chkindata =
        profile_wth_frnd_list.joinWithLarger('skey -> 'userProfileId, checkindata).project('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity, 'checkindata, 'frndlist)

    var companies=profile_wth_frnd_list_chkindata.
    flatMapTo('work -> 'corp ){
        work : List[scala.collection.immutable.List[UserEmployment]] => Option(work).flatten
    }.filter('corp){
        name : String => !name.trim.toString.contains("None")
    }.filter('corp){
        name : String => !name.trim.toString.contains("Some([])")
    }
    .write(TextLine(out)) */

    def getcurrCity(city: List[String]): String = {
        city.headOption.mkString
    }

    def getFirstEdu(edu: scala.collection.immutable.List[UserEducation]): String = {
        // if(edu.length>0)
        //     println(edu)
        edu.headOption.map(_.getSchoolName()).mkString
        //"job"

    }


    def getFirstEduDegree(edu: scala.collection.immutable.List[UserEducation]): String = {
        // if(edu.length>0)
        //     println(edu)
        edu.headOption.map(_.getDegree()).mkString
        //"job"

    }


    def getFirstEduDegreeYear(edu: scala.collection.immutable.List[UserEducation]): String = {
        // if(edu.length>0)
        //     println(edu)
        edu.headOption.map(_.getYear()).mkString
        //"job"

    }

    def getFirstWork(work: scala.collection.immutable.List[UserEmployment]): String = {
        //println(work)
        work.headOption.map(_.getCompanyName()).mkString
        //"work"

    }

    def getWorkSummary(work: scala.collection.immutable.List[UserEmployment]): String = {
        //println(work)
        work.headOption.map(_.getSummary()).mkString
        //"work"

    }

    def getWorkTitle(work: scala.collection.immutable.List[UserEmployment]): String = {
        //println(work)
        work.headOption.map(_.getTitle()).mkString
        //"work"

    }

    def getLatitute(checkins: List[CheckinObjects]): String = {

        checkins.map(_.getLatitude).toString
    }

    def formCityList(fbcitydata: Option[String], lnkdcitydata: Option[String]): List[String] = {
        fbcitydata.toList ++ lnkdcitydata.toList
    }

    def formWorkHistoryList(fbworkdata: scala.collection.immutable.List[UserEmployment], lnkdworkdata: scala.collection.immutable.List[UserEmployment]): scala.collection.immutable.List[UserEmployment] = {
        // fbworkdata ++ lnkdworkdata
        // Option(fbworkdata ++ lnkdworkdata).toList
        fbworkdata.toList ++ lnkdworkdata.toList
    }

    def formEducationlist(fbedulist: scala.collection.immutable.List[UserEducation], lnedulist: scala.collection.immutable.List[UserEducation]): scala.collection.immutable.List[UserEducation] = {
        fbedulist ++ lnedulist
    }

    /*def formCityList(fbcitydata: Option[String], lnkdcitydata: Option[String]): String = {
        //fbcitydata.toList ++ lnkdcitydata.toList
        //Option(Option(Option(fbcitydata).head).head).mkString
        (Option(fbcitydata).head).mkString
    }

    def formWorkHistoryList(fbworkdata: Option[scala.collection.immutable.List[UserEmployment]], lnkdworkdata: Option[scala.collection.immutable.List[UserEmployment]]): List[scala.collection.immutable.List[UserEmployment]] = {
    //def formWorkHistoryList(fbworkdata: Option[scala.collection.immutable.List[UserEmployment]], lnkdworkdata: Option[scala.collection.immutable.List[UserEmployment]]): Option[UserEmployment] = {
        fbworkdata.toList ++ lnkdworkdata.toList
        //Option(Option(Option(lnkdworkdata).head).head)
    }

    def formEducationlist(fbedulist: Option[scala.collection.immutable.List[UserEducation]], lnedulist: Option[scala.collection.immutable.List[UserEducation]]): List[scala.collection.immutable.List[UserEducation]] = {
        fbedulist.toList ++ lnedulist.toList
        //Option(Option(Option(Option(Option(lnedulist).head).head).head).head).map(_.getSchoolName()).toString
        //Option(Option(Option(lnedulist).head).head).mkString
        //Option(Option(Option(lnedulist).head.foreach())


    }*/

    def getWork(workJson: Option[String]): List[UserEmployment] = {
        val resultantJson = workJson.mkString;
        val fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        Option(fbdata).map(_.getWork().toList).getOrElse(List[UserEmployment]())
    }

    def getEducation(educationJson: Option[String]): List[UserEducation] = {
        val resultantJson = educationJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        Option(fbdata).map(_.getEducation().toList).getOrElse(List[UserEducation]())

    }

    def getUserName(fbJson: Option[String]): Option[String] = {
        val resultantJson = fbJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var res4 = Option(fbdata)
        res4.map(_.getFullName())
    }

    def getCity(fbJson: Option[String]): Option[String] = {
        val resultantJson = fbJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var res4 = Option(fbdata)
        res4.map(_.getLocation())
    }

    def getFBId(serviceTypeFB: String, serviceTypeLn: String): Option[String] = {
        if (serviceTypeFB == null)
            None
        if ((serviceTypeFB == serviceTypeLn) && serviceTypeFB.trim == "ln") {
            None
        }
        else {
            Option(serviceTypeFB)
        }
    }

    def getLinkedInId(serviceTypeFB: String, serviceTypeLn: String): Option[String] = {
        if (serviceTypeFB == null)
            None
        if ((serviceTypeFB == serviceTypeLn) && serviceTypeFB.trim == "fb") {
            None
        }
        else {
            Option(serviceTypeLn)
        }
    }

    def getFBJson(serviceTypeFB: String, serviceTypeLn: String, jsonString: String): Option[String] = {
        if (serviceTypeFB == null)
            None
        if (serviceTypeFB == serviceTypeLn && serviceTypeFB.trim == "ln") {
            None
        }
        else {
            Option(jsonString)
        }
    }

    def getLNKDINJson(serviceTypeFB: String, serviceTypeLn: String, jsonString: String): Option[String] = {
        if (serviceTypeFB == null)
            None
        if (serviceTypeFB == serviceTypeLn && serviceTypeFB.trim == "fb") {
            None
        }
        else {
            Option(jsonString)
        }
    }

    def getID(jsonString: Option[String]): Option[String] = {
        val resultantJson = jsonString.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var res4 = Option(fbdata)
        res4.map(_.getUserId())
    }

    def prettyPrint(foo: Option[String]): String = foo match {
        case Some(x) => x
        case None => "None"
    }

    def md5SumString(bytes: Array[Byte]): String = {
        val md5 = MessageDigest.getInstance("MD5")
        md5.reset()
        md5.update(bytes)
        md5.digest().map(0xFF & _).map {
            "%02x".format(_)
        }.foldLeft("") {
            _ + _
        }
    }

    def getFriendName(friends: List[FriendObjects]): String = {

        friends.map(_.getFriendName).toString()
    }

    def getServiceType(friends: List[FriendObjects]): String = {

        friends.map(_.getServiceType).toString()
    }

    def getServiceProfileId(friends: List[FriendObjects]): String = {

        friends.map(_.getServiceProfileId).toString()
    }

    def getVenue(checkins: List[CheckinObjects]): String = {
        checkins.map(_.getVenueName).toString
    }

    def getLatLongCity(city: String): String = {
        val resp = new HttpClientRest()
        val location = resp.getLatLong(city);
        location

    }

    def fourSquareCall(workplace: String, locationCityLat: String, locationCityLong: String): String = {
        val resp = new HttpClientRest()
        val location = resp.getFSQWorkplaceLatLong(workplace, locationCityLat, locationCityLong);
        location
        /*val fsquery = query.replaceAll(" ","%20")
        val fslocation=location;
        val url= "https://api.foursquare.com/v2/venues/search?ll="+fslocation+"&query="+fsquery+"&client_id=NB45JIY4HBP3VY232KO12XGDAZGF4O3DKUOBRTGZ5REY50E1&client_secret=5NCZW0FWUCHCJ5VS35YDG20AYHGBC2H5Z1W2OIG13IUEDHNK";
        var responseBodyString = HttpClientRestApi.fetchresponse(url)
        responseBodyString
        */


    }

}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
    val DataExtractLineFriend: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":"(.*)","photo.*""".r
    val NullNameExtractLine: Regex = """([a-zA-Z\d\-]+):(.*)"id":"(.*)","service_type":"(.*)","name":(null),"photo.*""".r
    val chkinDataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
    val companiesregex: Regex = """(.*):(.*)""".r
    val locationregex: Regex = """(.*):(.*)""".r
    val locationofficeregex: Regex = """(.*):(.*):(.*)""".r
}