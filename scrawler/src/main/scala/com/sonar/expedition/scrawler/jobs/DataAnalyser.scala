package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import com.twitter.scalding.TextLine
import cascading.tuple.Fields
import com.lambdaworks.jacks._
import tools.nsc.io.Streamable.Bytes


/*

      com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt"  --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt"
        --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --male "/tmp/male.txt" --female "/tmp/female.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe"
        --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"
         --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt

     --jobtype "3"
        1 for only no gender and job classification
        2 for only gender
        3 for only job classification
        4 for both

        com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt" --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt" --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe" --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"  --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt"

 */


class DataAnalyser(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val finp = args("friendData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")
    val jobOutputclasslabel = args("outputclassify")
    val placesData = args("placesData")
    val bayestrainingmodel = args("bayestrainingmodel")
    val genderoutput = args("genderoutput")
    val profileCount = args("profileCount")
    val serviceCount = args("serviceCount")
    val geoCount = args("geoCount")
    val jobtypeToRun = args("jobtype")
    var trainedseqmodel = args("trainedseqmodel")

    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata)).write(TextLine("/tmp/data"))


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
    val gendperpipe = new GenderInfoReadPipe(args)
    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)
    val certainityScore = new CertainityScorePipe(args)
    val jobTypeToRun = new JobTypeToRun(args)
    val internalAnalysisJob = new InternalAnalysisJob(args)


    val filteredProfiles = joinedProfiles.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle))
            .map('worked ->('stemmedWorked, 'mtphnWorked)) {
        fields: String =>
            val (worked) = fields
            val stemmedWorked = StemAndMetaphoneEmployer.getStemmed(worked)
            val mtphnWorked = StemAndMetaphoneEmployer.getStemmedMetaphone(worked)

            (stemmedWorked, mtphnWorked)
    }.project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'mtphnWorked, 'city, 'worktitle)).write(TextLine("/tmp/output107"))

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


    val chkindata = checkinGrouperPipe.groupCheckins(TextLine(chkininputData).read).write(TextLine("/tmp/output106"))

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)).write(TextLine("/tmp/output105"))

    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project(('key, 'worked))

    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val friendsForCoworker = friendGrouper.groupFriends(friendData).write(TextLine("/tmp/output101"))

    val numberOfFriends = friendsForCoworker.groupBy('userProfileId) {
        _.size
    }.rename('size -> 'numberOfFriends).project(('userProfileId, 'numberOfFriends)).write(TextLine("/tmp/output102"))

    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata).write(TextLine("/tmp/output103"))

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins)).write(TextLine("/tmp/output104"))


    val filteredProfilesWithScore = certainityScore.stemmingAndScore(filteredProfiles, findcityfromchkins, placesPipe, numberOfFriends)
            .write(TextLine(jobOutput))


    val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
        fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

    }

    jobTypeToRun.jobTypeToRun(jobtypeToRun, filteredProfilesWithScore, seqModel, trainedseqmodel).write(TextLine(jobOutputclasslabel))

    internalAnalysisJob.internalAnalysisGroupByServiceType(data).write(TextLine(serviceCount))
    internalAnalysisJob.internalAnalysisUniqueProfiles(data).write(TextLine(profileCount))
    internalAnalysisJob.internalAnalysisGroupByCity(joinedProfiles).write(TextLine(geoCount))


}


object DataAnalyser {

}