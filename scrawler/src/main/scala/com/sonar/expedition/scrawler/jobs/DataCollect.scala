package com.sonar.expedition.scrawler.jobs

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
/*
class DataCollect(args: Args) extends Job(args) {

    var inputData = "/tmp/dataAnalyse.txt"
    var out = "/tmp/results7.txt"
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

    var out3 = TextLine("/tmp/data1234.txt")
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
            var fbedu = Option(getEducation(jsondata))
            var lnedu = Option(getEducation(jsondata2))
            var fbwork = Option(getWork(jsondata))
            var lnwork = Option(getWork(jsondata2))
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
        fields: (String, Option[String], Option[String], Option[String], Option[List[UserEducation]], Option[List[UserEducation]], Option[List[UserEmployment]], Option[List[UserEmployment]], List[String]) =>
            val (rowkey, fbname, fbid, lnid, fbedu, lnedu, fbwork, lnwork, city) = fields
            val edulist = formEducationlist(fbedu, lnedu)
            val worklist = formWorkHistoryList(fbwork, lnwork)
            (rowkey, fbname, fbid, lnid, edulist, worklist, city)
    }.project('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity)

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

    var friendlist = (TextLine("/tmp/frienddatatest.txt").read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsonfrnddata)) {
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
                case CheckinExtractLine(id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message) => (id, serviceType, serviceID, serviceCheckinID, venueName, venueAddress, checkinTime, geoHash, lat, lng, message)
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
            .write(TextLine(out))

    def getLatitute(checkins: List[CheckinObjects]): String = {

        checkins.map(_.getLatitude).toString
    }

    def formCityList(fbcitydata: Option[String], lnkdcitydata: Option[String]): List[String] = {
        fbcitydata.toList ++ lnkdcitydata.toList
    }

    def formWorkHistoryList(fbworkdata: Option[scala.collection.immutable.List[UserEmployment]], lnkdworkdata: Option[scala.collection.immutable.List[UserEmployment]]): List[scala.collection.immutable.List[UserEmployment]] = {
        fbworkdata.toList ++ lnkdworkdata.toList
    }

    def formEducationlist(fbedulist: Option[scala.collection.immutable.List[UserEducation]], lnedulist: Option[scala.collection.immutable.List[UserEducation]]): List[scala.collection.immutable.List[UserEducation]] = {
        fbedulist.toList ++ lnedulist.toList
    }

    def getWork(workJson: Option[String]): Option[java.util.List[UserEmployment]] = {
        val resultantJson = workJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var result = Option(fbdata)
        result.map(_.getWork())
    }

    def getEducation(educationJson: Option[String]): Option[java.util.List[UserEducation]] = {
        val resultantJson = educationJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var res4 = Option(fbdata)
        res4.map(_.getEducation())
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
}

object DataCollect {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
}
*/