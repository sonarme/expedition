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
 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalyseBayesModel --hdfs --placesData "/tmp/places_dump_US.geojson.txt" --bayestrainingmodelforlocationtype "/tmp/bayestrainingmodelforlocationtype"


 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalyse --hdfs --checkindata "/tmp/checkin_nomsg.txt" --output "/tmp/output.txt" --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox" --bayestrainingmodel "/tmp/bayestrainingmodel" --training "/tmp/training" --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"  --placesData "/tmp/places_dump_US.geojson.txt --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis""
*/
class LocationBehaviourAnalyse(args: Args) extends LocationBehaviourAnalysePipe(args) {

    val checkinInfoPipe = new CheckinGrouperFunction(args)
    val chkindata = TextLine(args("checkindata"))
    val chkindataoutput = TextLine(args("output"))
    val bayestrainingmodel = args("bayestrainingmodelforlocationtype")
    val training = args("training")
    val trainingclassified = args("trainingclassified")
    val trainingclassifiedfinal = args("trainingclassifiedfinal")
    val locationBehaviourAnalysis = args("locationBehaviourAnalysis")

    val chkinpipe = checkinInfoPipe.unfilteredCheckinsLatLon(chkindata).project(Fields.ALL)
    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime,'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour

    val chkinpipe1 = chkinpipe.project(('keyid, 'venName, 'chknTime, 'latitude, 'longitude))
    val chkinpipe2 = chkinpipe.project(('venName, 'keyid, 'chknTime, 'latitude, 'longitude)).rename(('venName, 'keyid, 'chknTime, 'latitude, 'longitude) ->('venName1, 'keyid1, 'chknTime1, 'latitude1, 'longitude1))
    var chkinpipefileterdtime = filterTime(chkinpipe1)
            .project(('venNameFROM, 'ghashTo, 'venNameTO, 'ghashFrom, 'countTIMES, 'keyidS))
            .write(chkindataoutput)

    // do also for ghash2 and output none for no matching and do classify later
    val classificationByBayesModel = classifyBayes(bayestrainingmodel, chkinpipefileterdtime)
    val placesData = args("placesData")
    val placesPipe = getLocationInfo(TextLine(placesData).read)
            .project(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'geometryLatitude, 'geometryLongitude))
            .flatMapTo(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'geometryLatitude, 'geometryLongitude) ->
            ('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'geohash)) {
        fields: (String, String, String, String, String, Double, Double) =>
            val (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, latitude, longitude) = fields
            val sectorGeohash = GeoHash.withBitPrecision(latitude, longitude, PrecomputationSettings.SECTOR_BIT_LENGTH)
            val neighbouringSectors = sectorGeohash.getAdjacent().map(_.longValue())
            neighbouringSectors map (
                    sector => (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, sector)
                    )
    }

    val placeFromjoin = placesPipe.joinWithSmaller('geohash -> 'ghashFrom, classificationByBayesModel)
            .project(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            .mapTo(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo) ->
            ('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)) {
        fields: (String, Int, String, String, Long, Long, String, String, String, String, String, Double, String, Long, String, Double) =>
            val (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo) = fields

            val placeFromType = getType(placetypeFrom, venNameFROM, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory)

            (keyidS, countTIMES, venNameFROM, placeFromType, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo)

    }.unique(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))


    val placesToJoin = placesPipe.joinWithSmaller('geohash -> 'ghashTo, placeFromjoin)
            .project(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            .mapTo(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo) ->
            ('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'geohash, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)) {
        fields: (String, Int, String, String, Long, Long, String, String, String, String, String, Double, String, Long, String, Double) =>
            val (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placetypeTo, weightTo) = fields
            val placeToType = getType(placetypeTo, venNameTO, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory)
            (keyidS, countTIMES, venNameFROM, placetypeFrom, ghashFrom, geohash, propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, weightFrom, venNameTO, ghashTo, placeToType, weightTo)
    }.unique(('keyidS, 'countTIMES, 'venNameFROM, 'keyFrom, 'ghashFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
            .write(TextLine(locationBehaviourAnalysis))

    def getType(placetypeFrom: String, venNameFROM: String, propertiesName: String, propertiesTags: String, classifiersCategory: String, classifiersType: String, classifiersSubcategory: String): String = {
        if (Option(propertiesName).getOrElse("").contains(venNameFROM)) {
            classifiersCategory
        }
        val placename = StemAndMetaphoneEmployer.removeStopWords(venNameFROM)
        val chkifnamecontains = StemAndMetaphoneEmployer.removeStopWords(" " + propertiesName + " " + propertiesTags + " " + classifiersCategory + " " + classifiersType + " " + classifiersSubcategory)
        if (Option(chkifnamecontains).getOrElse("").contains(placename)) {
            classifiersSubcategory
        } else {
            placetypeFrom
        }
    }

    def filterTime(checkin: RichPipe): RichPipe = {
        val pipe = chkinpipe1.joinWithSmaller('keyid -> 'keyid1, chkinpipe2).project(('keyid, 'venName1, 'chknTime1, 'latitude, 'longitude, 'venName, 'chknTime, 'latitude1, 'longitude1))

                .filter(('venName1, 'venName)) {
            fields: (String, String) =>
                val (venname1, venname2) = fields
                (!venname1.equals(venname2))
        }.filter(('chknTime1, 'chknTime)) {
            fields: (String, String) =>
                val (chknTime1, chknTime2) = fields
                (deltatime(chknTime1, chknTime2))
        }
                .mapTo(('keyid, 'venName1, 'latitude, 'longitude, 'venName, 'latitude1, 'longitude1) ->('venNameFROM, 'latitudeFrom1, 'longitudeFrom1, 'venNameTO, 'latitudeTo1, 'longitudeTo1, 'countTIMES, 'keyidS)) {
            fields: (String, String, String, String, String, String, String) =>
                val (keyid, venname1, latitude, longitude, venname2, latitude2, longitude2) = fields
                (venname1, latitude, longitude, venname2, latitude2, longitude2, 1, keyid) //add one to do a sum('countTIMES) inside the pipe to find out total count of users moving from  'venNameFROM to 'venNameTO
        }
        val geohashedpipe = convertlatlongToGeohash(pipe)
        geohashedpipe
    }

    def convertlatlongToGeohash(inpipe: RichPipe): RichPipe = {
        val outpipe = inpipe.mapTo(('venNameFROM, 'latitudeFrom1, 'longitudeFrom1, 'venNameTO, 'latitudeTo1, 'longitudeTo1, 'countTIMES, 'keyidS) ->('venNameFROM, 'ghashFrom, 'venNameTO, 'ghashTo, 'countTIMES, 'keyidS)) {
            fields: (String, Double, Double, String, Double, Double, Int, String) =>
                val (venname1, latitude, longitude, venname2, latitude2, longitude2, cnt, keys) = fields
                val ghash1 = GeoHash.withBitPrecision(latitude, longitude, PrecomputationSettings.SECTOR_BIT_LENGTH).longValue()
                val ghash2 = GeoHash.withBitPrecision(latitude2, longitude2, PrecomputationSettings.SECTOR_BIT_LENGTH).longValue()

                (StemAndMetaphoneEmployer.getStemmed(venname1), ghash1, StemAndMetaphoneEmployer.getStemmed(venname2), ghash2, cnt, keys)
        }
        outpipe
    }


    def classifyBayes(bayestrainingmodel: String, chkinpipefileterdtime: RichPipe): RichPipe = {
        val trainer = new BayesModelPipe(args)
        val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
            fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields
        }
        val chkinpipe4 = chkinpipefileterdtime.project('venNameTO).rename('venNameTO -> 'data).write(TextLine(training))
        val trainedto = trainer.calcProb(seqModel, chkinpipe4).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('dataTo, 'keyTo, 'weightTo)).write(TextLine(trainingclassified)) //project('data, 'key, 'weight)
        val classifiedplacesto = chkinpipefileterdtime.joinWithSmaller('venNameTO -> 'dataTo, trainedto).project(('keyidS, 'venNameFROM, 'ghashFrom, 'venNameTO, 'countTIMES, 'ghashTo, 'keyTo, 'weightTo)) /*.write(TextLine(trainingclassifiedfinal))*/
        val chkinpipe5 = classifiedplacesto.project('venNameFROM).rename('venNameFROM -> 'data)
        val trainedfrom = trainer.calcProb(seqModel, chkinpipe5).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('dataFrom, 'keyFrom, 'weightFrom)) //project('data, 'key, 'weight)
        val classifiedjobs = classifiedplacesto.joinWithSmaller('venNameFROM -> 'dataFrom, trainedfrom).project(('keyidS, 'countTIMES, 'venNameFROM, 'ghashFrom, 'keyFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo)).write(TextLine(trainingclassifiedfinal))
        classifiedjobs.project(('keyidS, 'countTIMES, 'venNameFROM, 'ghashFrom, 'keyFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))

    }
}
