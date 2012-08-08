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
import ch.hsr.geohash.GeoHash


/*

      com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt"  --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt"
        --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --male "/tmp/male.txt" --female "/tmp/female.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe"
        --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"
         --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt"
         --debug1 "/tmp/debug1" --debug2 "/tmp/debug2"  --debug3 "/tmp/debug3" --debug4 "/tmp/debug4" --debug5 "/tmp/debug5" --debug6 "/tmp/debug6"  --debug7 "/tmp/debug7" --debug8 "/tmp/debug8" --debug8 "/tmp/debug9"
         --geohashsectorsize "20"


     --jobtype "3"
        1 for only no gender and job classification
        2 for only gender
        3 for only job classification
        4 for both

        com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt" --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt" --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe" --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "3"  --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt" --realsocialgraph "/tmp/realSocialGraph.txt"

        com.sonar.expedition.scrawler.jobs.DataAnalyser --hdfs --serviceProfileData "/tmp/serviceProfileData.txt" --friendData "/tmp/friendData.txt" --checkinData "/tmp/checkinDatatest.txt" --placesData "/tmp/places_dump_US.geojson.txt" --output "/tmp/dataAnalyseroutput.txt" --occupationCodetsv "/tmp/occupationCodetsv.txt" --occupationCodeTsvOutpipe "/tmp/occupationCodeTsvOutpipe" --genderdataOutpipe "/tmp/genderdataOutpipe" --bayestrainingmodel "/tmp/bayestrainingmodel" --outputclassify "/tmp/jobclassified" --genderoutput "/tmp/genderoutput"  --outputjobtypes "/tmp/outputjobtypes"  --jobtype "4"  --profileCount "/tmp/profileCount.txt" --serviceCount "/tmp/serviceCount.txt" --geoCount "/tmp/geoCount.txt" --realsocialgraph "/tmp/realSocialGraph.txt" --trainedseqmodel  "/tmp/trainedseqmodel" --debug1 "/tmp/debug1" --debug2 "/tmp/debug2"  --debug3 "/tmp/debug3" --debug4 "/tmp/debug4" --debug5 "/tmp/debug5" --debug6 "/tmp/debug6"  --debug7 "/tmp/debug7" --debug8 "/tmp/debug8" --debug9 "/tmp/debug9" --geoHash "/tmp/geoHash" --geohashsectorsize "1000" --groupcountry "/tmp/groupcountry" --groupworktitle "/tmp/groupworktitle"  --groupcity "/tmp/groupcity"
 */


class DataAnalyser(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val twitterInputData = args("twitterServiceProfileData")
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
    val graphOutput = args("realsocialgraph")
    val jobtypeTorun = args("jobtype")
    val trainedseqmodel = args("trainedseqmodel")
    val geohashsectorsize = args.getOrElse("geohashsectorsize", "20").toInt
    val geoHash = args("geoHash")
    val groupworktitle = args("groupworktitle")
    val groupcountry = args("groupcountry")
    val groupcity = args("groupcity")


    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val twitterdata = (TextLine(twitterInputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
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
    val gendperpipe = new GenderInfoReadPipe(args)
    val joinedProfiles = dtoProfileGetPipe.getTotalProfileTuples(data, twitterdata)
    val serviceIdsGraph = joinedProfiles.rename('key ->'friendkey).project(('friendkey, 'uname, 'fbid, 'lnid, 'twid, 'fsid))
    val certainityScore = new CertainityScorePipe(args)
    val jobTypeToRun = new JobTypeToRun(args)
    val internalAnalysisJob = new InternalAnalysisJob(args)
    val realGraph = new RealSocialGraph(args)


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


    val friendData = TextLine(finp).read.project('line)


    val checkinData = TextLine(chkininputData).read
    val filteredCheckins = checkinGrouperPipe.groupCheckins(checkinData)
    val unfilteredCheckins = checkinGrouperPipe.unfilteredCheckins(checkinData)

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, filteredCheckins).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project(('key, 'worked))

    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val friendPipe = friendGrouper.groupFriends(friendData)

    val numberOfFriends = friendPipe.groupBy('userProfileId) {
        _.size
    }.rename('size -> 'numberOfFriends).project(('userProfileId, 'numberOfFriends))

    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendPipe, serviceIds, filteredCheckins)

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))

    val filteredProfilesWithScore = certainityScore.stemmingAndScore(filteredProfiles, findcityfromchkins, placesPipe, numberOfFriends)

    val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
        fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

    }

    val jobRunPipeResults = jobTypeToRun.jobTypeToRun(jobtypeTorun, filteredProfilesWithScore, seqModel, trainedseqmodel)
    val groupByServiceType = internalAnalysisJob.internalAnalysisGroupByServiceType(data)
    val uniqueProfiles = internalAnalysisJob.internalAnalysisUniqueProfiles(data)
    val groupByCity = internalAnalysisJob.internalAnalysisGroupByCity(joinedProfiles)
    val (returnpipecity, returnpipecountry, returnpipework) = internalAnalysisJob.internalAnalysisGroupByCityCountryWorktitle(filteredProfilesWithScore, placesPipe, jobRunPipeResults, geohashsectorsize) //'key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends
    val realSocialGraph = realGraph.friendsNearbyByFriends(friendPipe, unfilteredCheckins, serviceIdsGraph)

    val PipeToText = Map(returnpipework -> groupworktitle,
        returnpipecountry -> groupcountry,
        returnpipecity -> groupcity,
        jobRunPipeResults -> jobOutputclasslabel,
        groupByServiceType -> serviceCount,
        uniqueProfiles -> profileCount,
        groupByCity -> geoCount,
        filteredProfilesWithScore -> jobOutput,
        realSocialGraph -> graphOutput)

    PipeToText foreach {
        case (pipe, fileName) => pipe.write(TextLine(fileName))
        case _ =>
    }

}


object DataAnalyser {

}