package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import java.util.{Calendar, Date}
import java.util
import com.sonar.expedition.scrawler.util.{Levenshtein, EmployerCheckinMatch, StemAndMetaphoneEmployer, LocationScorer}
import scala.Some
import scala.Some
import com.sonar.expedition.scrawler.objs.serializable.LuceneIndex
import scala.Some
import com.twitter.scalding.TextLine
import com.sonar.dossier.service.PrecomputationSettings
import ch.hsr.geohash.GeoHash
import java.text.DecimalFormat

/*
 BEFORE RUNING THIS MAKE SURE TO build the bayes model by running this
 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysisBayesModel --hdfs --placesData "/tmp/places_dump_US.geojson.txt" --bayestrainingmodelforlocationtype "/tmp/bayestrainingmodelforlocationtype"


 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysis --hdfs --timedifference "6" --checkindata "/tmp/checkin_nomsg.txt" --output "/tmp/output.txt" --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox" --bayestrainingmodel "/tmp/bayestrainingmodel" --training "/tmp/training" --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"  --placesData "/tmp/places_dump_US.geojson.txt --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis""

 // need to integrate the data from cloudmade and cross join with the output of dataanalyser to get the male and user profile info
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

    val chkinpipe = checkinInfoPipe.unfilteredCheckinsLatLon(chkindata).project(Fields.ALL)
    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime,'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour

    val chkinpipe1 = chkinpipe.project(('keyid, 'venName, 'chknTime, 'lat, 'lng)).rename(('lat, 'lng) ->('latitude, 'longitude))
    val chkinpipe2 = chkinpipe.project(('venName, 'keyid, 'chknTime, 'lat, 'lng)).rename(('venName, 'keyid, 'chknTime, 'lat, 'lng) ->('venName1, 'keyid1, 'chknTime1, 'latitude1, 'longitude1))
    var chkinpipefileterdtime = filterTime(chkinpipe1, chkinpipe2, timedifference, geohashsectorsize)
            .project(('venNameFROM, 'ghashTo, 'venNameTO, 'ghashFrom, 'countTIMES, 'keyidS))
            .write(chkindataoutput)

    // do also for ghash2 and output none for no matching and do classify later
    val classificationByBayesModel = classifyBayes(bayestrainingmodel, chkinpipefileterdtime, training, trainingclassified, trainingclassifiedfinal)
    val placesData = args("placesData")
    val placesPipe = getLocationInfo(placesData, geohashsectorsize)

    val placeFromClassification = placesPipe.joinWithSmaller('geohash -> 'ghashFrom, classificationByBayesModel)
            .project(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            .mapTo(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo) ->
            ('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)) {
        fields: (String, Int, String, String, Long, Long, String, String, String, String, String, Double, String, Long, String, Double) =>
            val (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo) = fields

            val placeFromType = getType(placetypeFrom, venNameFROM, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory)

            (keyidS, countTIMES, venNameFROM, placeFromType, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo)

    }.unique(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))


    val placesToClassification = placesPipe.joinWithSmaller('geohash -> 'ghashTo, placeFromClassification)
            .project(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            .mapTo(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo) ->
            ('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)) {
        fields: (String, Int, String, String, Long, Long, String, String, String, String, String, Double, String, Long, String, Double) =>
            val (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo) = fields
            val placeToType = getType(placetypeTo, venNameTO, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory)
            (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placeToType, weightTo)
    }.unique(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            .write(TextLine(locationBehaviourAnalysis))


}
