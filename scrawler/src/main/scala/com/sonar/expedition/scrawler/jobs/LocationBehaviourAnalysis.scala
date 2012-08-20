package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.TextLine

/*
 BEFORE RUNING THIS MAKE SURE TO build the bayes model by running this
 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysisBayesModel --hdfs --placesData "/tmp/places_dump_US.geojson.txt" --bayestrainingmodelforlocationtype "/tmp/bayestrainingmodelforlocationtype"


 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysis --hdfs --timedifference "6" --checkindata "/tmp/checkin_nomsg.txt" --



  "/tmp/output.txt" --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox" --bayestrainingmodel "/tmp/bayestrainingmodel" --training "/tmp/training" --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"  --placesData "/tmp/places_dump_US.geojson.txt --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis""

 // need to integrate the data from cloudmade and cross join with the output of dataanalyser to get the male and user profile info

 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysis --hdfs --checkindata "/tmp/checkinDatatest.txt" --output "/tmp/output.txt"
 --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox"
 --bayestrainingmodelforlocationtype "/tmp/bayestrainingmodelforplacetype" --training "/tmp/training"
 --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"
 --placesData "/tmp/places_dump_US.geojson.txt" --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis"
 --timedifference "24" --geohashsectorsize "20"   --serviceProfile "/tmp/serviceProfileData.txt" --locationAnalyis "/tmp/locationAnalyis"
*/
class LocationBehaviourAnalysis(args: Args) extends LocationBehaviourAnalysePipe(args) {

    val checkinInfoPipe = new CheckinGrouperFunction(args)
    val chkindata = TextLine(args("checkindata"))
    val chkindataoutput = TextLine(args("output"))
    val bayestrainingmodel = args("bayestrainingmodelforlocationtype")
    val training = args("training")
    val trainingclassified = args("trainingclassified")
    val trainingclassifiedfinal = args("trainingclassifiedfinal")
    val locationBehaviourAnalysis = args("locationBehaviourAnalysis")
    val timedifference = args("timedifference")
    val geohashsectorsize = args("geohashsectorsize")
    val prodtest = args.getOrElse("prodtest", "0").toInt
    val placesData = args("placesData")
    val inputData = args("serviceProfile")
    val locationAnalyis = args("locationAnalyis")

    val chkinpipe = checkinInfoPipe.unfilteredCheckinsLatLon(chkindata).filter('venName) {
        fields: (String) =>
            val (venname) = fields
            (venname != None || venname != null || venname.trim != "")
    }.project(Fields.ALL)
    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime,'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour

    val chkinpipe1 = chkinpipe.project(('keyid, 'venName, 'chknTime, 'lat, 'lng)).rename(('lat, 'lng) ->('latitude, 'longitude))
    val chkinpipe2 = chkinpipe.project(('venName, 'keyid, 'chknTime, 'lat, 'lng)).rename(('venName, 'keyid, 'chknTime, 'lat, 'lng) ->('venName1, 'keyid1, 'chknTime1, 'latitude1, 'longitude1))

    var chkinpipefileterdtime = filterTime(chkinpipe1, chkinpipe2, timedifference, geohashsectorsize, prodtest)
            .project(Fields.ALL)
            .write(chkindataoutput)

    // do also for ghash2 and output none for no matching and do classify later
    val classificationByBayesModel = classifyTFIDF(bayestrainingmodel, chkinpipefileterdtime)
            /*val placesPipe = getLocationInfo(placesData, geohashsectorsize)

                val placeFromClassification = placesPipe.joinWithSmaller('geohash -> 'ghashFrom, classificationByBayesModel)
                        .project(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
                        .mapTo(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo) ->
                        ('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)) {
                        fields: (String, Int, String, String, Long, Long, String, String, String, String, String, Double, String, Long, String, Double) =>
                        val (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo) = fields
                        val placeFromType = getType(placetypeFrom, venNameFROM, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory)
                        (keyidS, countTIMES, venNameFROM, placeFromType, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo)

                }.unique(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            */

            /*val placesToClassification = placesPipe.joinWithSmaller('geohash -> 'ghashTo, placeFromClassification)
                    .project(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
                    .mapTo(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo) ->
                    ('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)) {
                fields: (String, Int, String, String, Long, Long, String, String, String, String, String, Double, String, Long, String, Double) =>
                    val (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo) = fields
                    val placeToType = getType(placetypeTo, venNameTO, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory)
                    (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placeToType, weightTo)
            }.unique(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            */ .write(TextLine(locationBehaviourAnalysis))

    val profilePipe = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(profilePipe) //'key, 'uname, 'fbid, 'lnid, 'fsid, 'twalias, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc

    val analysis = joinedProfiles.joinWithSmaller('key -> 'keyidS, classificationByBayesModel).project(('uname, 'fbid, 'lnid, 'fsid, 'twalias, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'countTIMES, 'venNameFROM, 'ghashFrom, 'keyFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)).write(TextLine(locationAnalyis))


}
