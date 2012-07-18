package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import DataAnalyser._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import com.twitter.scalding.TextLine
import com.lambdaworks.jacks._
import java.security.MessageDigest
import tools.nsc.io.Streamable.Bytes


/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

 */


class DataAnalyser(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val finp = args("friendData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")
    val placesData = args("placesData")

    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val employerGroupedServiceProfilePipe = new DTOProfileInfoPipe(args)
    val friendInfoPipe = new FriendInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)
    val apiCalls = new APICalls(args)
    val metaphoner = new StemAndMetaphoneEmployer()
    val levenshteiner = new Levenshtein()
    val checker = new EmployerCheckinMatch
    val haversiner = new Haversine
    val coworkerPipe = new CoworkerFinderFunction((args))
    val friendGrouper = new FriendGrouperFunction(args)
    val dtoPlacesInfoPipe = new DTOPlacesInfoPipe(args)
    val scorer = new LocationScorer


    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)

    val filteredProfiles = joinedProfiles.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle))
            .map('worked ->('stemmedWorked, 'mtphnWorked)) {
        fields: String =>
            val (worked) = fields
            val stemmedWorked = StemAndMetaphoneEmployer.getStemmed(worked)
            val mtphnWorked = StemAndMetaphoneEmployer.getStemmedMetaphone(worked)

            (stemmedWorked, mtphnWorked)
    }.project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'mtphnWorked, 'city, 'worktitle))

    val placesPipe = dtoPlacesInfoPipe.getPlacesInfo(TextLine(placesData).read)
            .project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
            .map('propertiesName ->('stemmedName, 'mtphnName)) {
        fields: String =>
            val (placeName) = fields
            val stemmedName = StemAndMetaphoneEmployer.getStemmed(placeName)
            val mtphnName = StemAndMetaphoneEmployer.getStemmedMetaphone(placeName)
            (stemmedName, mtphnName)
    }.project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'stemmedName, 'mtphnName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))

    //find companies with uqniue coname and city
    //val unq_cmp_city = tmpcompanies.unique('mtphnWorked, 'city, 'fbid, 'lnid)
    /*
    if city is not filled up find city form chekcins and friends checkin
     */
    var friends = friendInfoPipe.friendsDataPipe(TextLine(finp).read)
    val friendData = TextLine(finp).read.project('line)

    val chkindata = checkinGrouperPipe.groupCheckins(TextLine(chkininputData).read)

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project(('key, 'worked))

    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val friendsForCoworker = friendGrouper.groupFriends(friendData)

    val numberOfFriends = friendsForCoworker.groupBy('userProfileId) {
        _.size
    }.rename('size -> 'numberOfFriends).project('userProfileId, 'numberOfFriends)

    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))


    filteredProfiles.joinWithSmaller('key -> 'key1, findcityfromchkins).project(('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'centroid, 'stemmedWorked))
            .map('mtphnWorked -> 'worked) {
        fields: (String) =>
            var (mtphnWorked) = fields
            if (mtphnWorked == null || mtphnWorked == "") {
                mtphnWorked = " "
            }
            mtphnWorked
    }
            .joinWithSmaller('worked -> 'mtphnName, placesPipe, joiner = new LeftJoin).project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'centroid, 'geometryLatitude, 'geometryLongitude, 'stemmedName, 'stemmedWorked))
            .map('centroid ->('lat, 'long)) {
        fields: String =>
            val (centroid) = fields
            val latLongArray = centroid.split(":")
            val lat = latLongArray.head
            val long = latLongArray.last
            (lat, long)
    }
            .map(('stemmedWorked, 'lat, 'long, 'stemmedName, 'geometryLatitude, 'geometryLongitude) ->('score, 'certainty)) {
        fields: (String, String, String, String, String, String) =>
            val (work, workLatitude, workLongitude, place, placeLatitude, placeLongitude) = fields
            val score = scorer.getScore(work, workLatitude, workLongitude, place, placeLatitude, placeLongitude)
            val certainty = scorer.certaintyScore(score, work, place)
            (score, certainty)
    }
            .groupBy('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked) {
        _
                .toList[(Double, String, String)](('certainty, 'geometryLatitude, 'geometryLongitude) -> 'certaintyList)
    }
            .map(('certaintyList) ->('certaintyScore, 'geometryLatitude, 'geometryLongitude)) {
        fields: (List[(Double, String, String)]) =>
            val (certaintyList) = fields
            val certainty = certaintyList.max
            (certainty._1, certainty._2, certainty._3)
    }.project(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked, 'certaintyScore, 'geometryLatitude, 'geometryLongitude))

            .joinWithSmaller('key -> 'userProfileId, numberOfFriends, joiner = new LeftJoin)
            .project('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends)
            .filter(('lat, 'long)) {
        fields: (String, String) =>
            val (lat, lng) = fields
            (lat.toDouble > 40.7 && lat.toDouble < 40.9 && lng.toDouble > -74 && lng.toDouble < -73.8) ||
                    (lat.toDouble > 40.489 && lat.toDouble < 40.924 && lng.toDouble > -74.327 && lng.toDouble < -73.723) ||
                    (lat.toDouble > 33.708 && lat.toDouble < 34.303 && lng.toDouble > -118.620 && lng.toDouble < -117.780) ||
                    (lat.toDouble > 37.596 && lat.toDouble < 37.815 && lng.toDouble > -122.514 && lng.toDouble < -122.362) ||
                    (lat.toDouble > 30.139 && lat.toDouble < 30.521 && lng.toDouble > -97.941 && lng.toDouble < -97.568) ||
                    (lat.toDouble > 41.656 && lat.toDouble < 42.028 && lng.toDouble > -87.858 && lng.toDouble < -87.491) ||
                    (lat.toDouble > 29.603 && lat.toDouble < 29.917 && lng.toDouble > -95.721 && lng.toDouble < -95.200) ||
                    (lat.toDouble > 33.647 && lat.toDouble < 33.908 && lng.toDouble > -84.573 && lng.toDouble < -84.250) ||
                    (lat.toDouble > 38.864 && lat.toDouble < 39.358 && lng.toDouble > -94.760 && lng.toDouble < -94.371) ||
                    (lat.toDouble > 30.130 && lat.toDouble < 30.587 && lng.toDouble > -82.053 && lng.toDouble < -81.384)
    }.project('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends)
            //            .map('uname -> 'hasheduser) {
            //        fields: String =>
            //            val user = fields
            //            val hashed = md5SumString(user)
            //            hashed
            //    }.project('key, 'hasheduser, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends)
            .write(TextLine(jobOutput))

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


    //           add this to output json lines

    //          .map(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked, 'certaintyScore, 'geometryLatitude, 'geometryLongitude) -> ('json)) {
    //        fields: (String, String, String, String, String, String, String, String, String, String, String, String, String) =>
    //            val (key, uname, fbid, lnid, city, worktitle, lat, long, worked, stemmedWorked, certaintyScore, geometryLatitude, geometryLongitude) = fields
    //            val json = JacksMapper.writeValueAsString(Map("key" -> key, "uname" -> uname, "Ids" -> List("fbid" -> fbid, "lnid" -> lnid), "city" -> city, "work" -> List("worktitle" -> worktitle, "lat" -> lat, "long" -> long, "worked" -> worked, "stemmedWorked" -> stemmedWorked), "certaintyScore" -> certaintyScore, "place" -> List("geometryLatitude" -> geometryLatitude, "geometryLongitude" -> geometryLongitude)))
    //            json
    //    }.project('json).write(TextLine(jobOutput))
}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs) : (.*)""".r
    val companiesregex: Regex = """(.*):(.*)""".r

}
