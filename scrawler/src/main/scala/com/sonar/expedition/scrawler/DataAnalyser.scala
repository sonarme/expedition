package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.restfb.types.User.Education
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.dossier.dto.UserEducation
import com.sonar.dossier.dto.UserEmployment
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO}
import com.sonar.expedition.scrawler._
import com.twitter.scalding._
import com.twitter.scalding.TextLine
import java.nio.ByteBuffer
import DataAnalyser._
import java.security.MessageDigest
import scala.Some
import scala.{Some, Option}
import util.matching.Regex
import APICalls._

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

    val inputData = "/tmp/dataAnalyseerrorwithtime.txt"
    val out = "/tmp/results7.txt"
    val out2 = TextLine("/tmp/data123.txt")
    val finp = "/tmp/friendData.txt"
    val frout = "/tmp/userGroupedFriends2.txt"
    val chkininputData = "/tmp/checkinDatatest.txt"
    val chkinout = "/tmp/hasheduserGroupedCheckins.txt"
    val sanitycheck = "/tmp/sanityCheck.txt"

    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project('id, 'serviceType, 'jsondata)

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args);
    val employerGroupedServiceProfilePipe = new DTOProfileInfoPipe(args);
    val friendInfoPipe = new FriendInfoPipe(args);
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)
    val apiCalls = new APICalls(args)
    val metaphoner = new StemAndMetaphoneEmployer
    val coworkerPipe = new CoworkerFinderFunction((args))
    val friendGrouper = new FriendGrouperFunction(args)

//    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
//    val friendsInput = "/tmp/userGroupedFriends.txt"
//    val serviceIdsInput = "/tmp/serviceIds.txt"

    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)
    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project('key, 'worked).groupBy('worked) {
        group => group.toList[String]('key,'employeeData)
    }.filter('worked){
        worked : String => !worked.trim.equals("")
    }.map('worked -> 'mtphnWorked) {
        fields: String =>
            val (worked) = fields
            val mtphnWorked = metaphoner.getStemmedMetaphone(worked)
            mtphnWorked
    }.project('mtphnWorked, 'employeeData)

    val filteredProfiles = joinedProfiles .project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle).map('worked -> 'mtphnWorked) {
        fields: String =>
            val (worked) = fields
            val mtphnWorked = metaphoner.getStemmedMetaphone(worked)
            mtphnWorked
    }
//            .filter('mtphnWorked) {
//        name: String => !name.trim.toString.equals("")
//    }
            .project('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle)

    /*
    if city is not filled up find city form chekcins and friends checkin
     */
    var friends = friendInfoPipe.friendsDataPipe(TextLine(finp).read)
    friends.write(TextLine(frout))

    val chkindata = checkinGrouperPipe.groupCheckins(TextLine(chkininputData).read)

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'serType, 'serProfileID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

    val findcityfromchkins = checkinInfoPipe.findCityofUserFromChkins(profilesAndCheckins)

    val writechkin = findcityfromchkins.write(TextLine("/tmp/chkindata.txt"))

    val serviceIds = joinedProfiles.project('key, 'fbid, 'lnid)

    val friendsForCoworker = friendGrouper.groupFriends(finp)
    val coworkerCheckins = coworkerPipe.findCoworkerCheckins(serviceProfilePipe, friendsForCoworker, serviceIds, TextLine(chkininputData).read)

    //companies with city names from checkin information if not present in profile
    val tmpcompanies = findcityfromchkins.project('mtphnWorked, 'uname, 'city, 'worktitle, 'fbid, 'lnid)

    //find companies with uqniue coname and city
    val unq_cmp_city = tmpcompanies.unique('mtphnWorked, 'city, 'fbid, 'lnid)

    val coandcity_latlong = apiCalls.fsqAPIFindLatLongFromCompAndCity(unq_cmp_city)

    val work_loc = coandcity_latlong
            .filter('lati, 'longi, 'street_address) {
        fields: (String, String, String) =>
            val (lat, lng, addr) = fields
            (!lat.toString.equalsIgnoreCase("-1")) && (!lng.toString.equalsIgnoreCase("-1")) && (!addr.toString.equalsIgnoreCase("-1"))

    }.write(TextLine("/tmp/work_place.txt"))

    val joinedwork_location = tmpcompanies.joinWithSmaller('city -> 'placename, coandcity_latlong)
            .project('mtphnWorked, 'uname, 'placename, 'lati, 'longi, 'street_address, 'worktitle, 'fbid, 'lnid)
            .unique('mtphnWorked, 'uname, 'placename, 'lati, 'longi, 'street_address, 'worktitle, 'fbid, 'lnid)
            .filter('lati, 'longi, 'street_address) {
        fields: (String, String, String) =>
            val (lat, lng, addr) = fields
            (!lat.toString.equalsIgnoreCase("-1")) && (!lng.toString.equalsIgnoreCase("-1")) && (!addr.toString.equalsIgnoreCase("-1"))

    }
            .write(out2)


}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs):(.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
    val companiesregex: Regex = """(.*):(.*)""".r

}