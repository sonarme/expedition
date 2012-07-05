package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.Jobs.StemAndMetaphoneEmployer
import com.twitter.scalding._
import DataAnalyser._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import com.twitter.scalding.TextLine


/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

 */


class DataAnalyser(args: Args) extends Job(args) {

    /* val inputData = args("serInput")
        val out2 = TextLine("/tmp/data123.txt")
        val finp = args("friInput")
        val frout = "/tmp/userGroupedFriends2.txt"
        val chkininputData = args("chkInput")
        val chkinout = "/tmp/hasheduserGroupedCheckins.txt"
        val sanitycheck = "/tmp/sanityCheck.txt"
    */
    val inputData = args("serviceProfileData")
    val finp = args("friendData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")

    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project('id, 'serviceType, 'jsondata)

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val employerGroupedServiceProfilePipe = new DTOProfileInfoPipe(args)
    val friendInfoPipe = new FriendInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)
    val apiCalls = new APICalls(args)
    val metaphoner = new StemAndMetaphoneEmployer()
    val coworkerPipe = new CoworkerFinderFunction((args))
    val friendGrouper = new FriendGrouperFunction(args)


    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)

    val filteredProfiles = joinedProfiles.project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle).map('worked -> 'mtphnWorked) {
        fields: String =>
            val (worked) = fields
            val mtphnWorked = metaphoner.getStemmedMetaphone(worked)
            mtphnWorked
    }.project('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle)

    //filteredProfiles.write(TextLine("/tmp/test5.txt"))
    //val tmpcompanies = filteredProfiles.project('mtphnWorked, 'uname, 'city, 'worktitle, 'fbid, 'lnid)

    //find companies with uqniue coname and city
    //val unq_cmp_city = tmpcompanies.unique('mtphnWorked, 'city, 'fbid, 'lnid)
    /*
    if city is not filled up find city form chekcins and friends checkin
     */
    var friends = friendInfoPipe.friendsDataPipe(TextLine(finp).read)
    //friends.write(TextLine(frout))

    val chkindata = checkinGrouperPipe.groupCheckins(TextLine(chkininputData).read)

    //chkindata.write(TextLine("/tmp/test4.txt"))
    //val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'serType, 'serProfileID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

    //profilesAndCheckins.write(TextLine("/tmp/test3.txt"))
    //    val writechkin = findcityfromchkins.write(TextLine("/tmp/chkindata.txt"))

    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project('key, 'worked)
    /*.groupBy('worked) {
group => group.toList[String]('key,'employeeData)
}.filter('worked){
worked : String => !worked.trim.equals("")
}.project('worked, 'employeeData)
    */
    val serviceIds = joinedProfiles.project('key, 'fbid, 'lnid).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))
    val friendData = TextLine(finp).read.project('line)

    val friendsForCoworker = friendGrouper.groupFriends(friendData)
    //val checkinData = TextLine(chkininputData).read.project('line)

    val coworkerCheckins = coworkerPipe.findCoworkerCheckins(employerGroupedServiceProfiles, friendData, serviceIds, chkindata)


    //coworkerCheckins.write(TextLine("/tmp/test2.txt"))
    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))

    //findcityfromchkins.write(TextLine("/tmp/test.txt"))
    //companies with city names from checkin information if not present in profile
    /*val tmpcompanies = findcityfromchkins.project('mtphnWorked, 'uname, 'city, 'worktitle, 'fbid, 'lnid)

    //find companies with uqniue coname and city
    val unq_cmp_city = tmpcompanies.unique('mtphnWorked, 'city, 'fbid, 'lnid)
    */

    //findcityfromchkins.write(TextLine("/tmp/data123.txt"))

    filteredProfiles.joinWithSmaller('key -> 'key1, findcityfromchkins).project('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'centroid)
            .write(TextLine(jobOutput))
    /*val coandcity_latlong = apiCalls.fsqAPIFindLatLongFromCompAndCity(unq_cmp_city)

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
            .write(out2)  */


}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs) : (.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
    val companiesregex: Regex = """(.*):(.*)""".r

}
