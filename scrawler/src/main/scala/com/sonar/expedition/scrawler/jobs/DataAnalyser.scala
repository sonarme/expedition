package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import DataAnalyser._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import com.twitter.scalding.TextLine


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
            val stemmedName = metaphoner.getStemmed(placeName)
            val mtphnName = metaphoner.getStemmedMetaphone(placeName)
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
            .map(('stemmedWorked, 'lat, 'long, 'stemmedName, 'geometryLatitude, 'geometryLongitude) ->('work, 'workLatitude, 'workLongitude, 'place, 'placeLatitude, 'placeLongitude, 'score, 'certainty)) {
        fields: (String, String, String, String, String, String) =>
            val (work, workLatitude, workLongitude, place, placeLatitude, placeLongitude) = fields
            val score = scorer.getScore(work, workLatitude, workLongitude, place, placeLatitude, placeLongitude)
            val certainty = scorer.certaintyScore(score, work, place)
            (work, workLatitude, workLongitude, place, placeLatitude, placeLongitude, score, certainty)
    }
//            .groupBy('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked) {
//        _
//                .toList[Double]('certainty -> 'certaintyList)
//    }
//            .map(('certaintyList) -> ('certaintyScore)) {
//        fields: (List[Double]) =>
//            val (certaintyList) = fields
//            val certainty = certaintyList.max
//            certainty
//    }


                                    .project(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'geometryLatitude, 'geometryLongitude, 'worked, 'stemmedWorked, 'stemmedName, 'score, 'certainty))
          //  .project(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked, 'certaintyScore))
            .write(TextLine(jobOutput))

}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs) : (.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
    val companiesregex: Regex = """(.*):(.*)""".r

}
