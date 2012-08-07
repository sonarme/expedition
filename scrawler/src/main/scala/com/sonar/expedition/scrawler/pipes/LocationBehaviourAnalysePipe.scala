package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import java.util.Calendar
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.twitter.scalding.TextLine
import cascading.tuple.Fields

/*
com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysis --hdfs --checkindata "/tmp/checkinDatatest.txt" --output "/tmp/output.txt" --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox" --bayestrainingmodelforlocationtype "/tmp/bayestrainingmodelforlocationtype" --training "/tmp/training" --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"  --placesData "/tmp/places_dump_US.geojson.txt" --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis"  --timedifference "24" --geohashsectorsize "20"
 */
class LocationBehaviourAnalysePipe(args: Args) extends DTOPlacesInfoPipe(args) {

    def getLocationInfo(placesData: RichPipe): RichPipe = {

        val parsedPlaces = placesPipe(placesData).project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode, 'linenum))

        parsedPlaces
    }

    def deltatime(chkintime1: String, chkintime2: String, timediff: String): Boolean = {

        val timeFilter1 = Calendar.getInstance()
        val checkinDate1 = CheckinTimeFilter.parseDateTime(chkintime1)
        timeFilter1.setTime(checkinDate1)
        val date1 = timeFilter1.get(Calendar.DAY_OF_YEAR)
        val time1 = timeFilter1.get(Calendar.HOUR_OF_DAY) + timeFilter1.get(Calendar.MINUTE) / 60.0
        val timeFilter2 = Calendar.getInstance()
        val checkinDate2 = CheckinTimeFilter.parseDateTime(chkintime2)
        timeFilter2.setTime(checkinDate2)
        val date2 = timeFilter2.get(Calendar.DAY_OF_YEAR)
        val time2 = timeFilter2.get(Calendar.HOUR_OF_DAY) + timeFilter2.get(Calendar.MINUTE) / 60.0
        if (date1.equals(date2)) {
            // need to include the timing too, which simple, if same date, check diff in time, normally we dont want checkins in border timings like 12 am.
            if ((time2.toDouble - time1.toDouble) < timediff.toInt)
                true
            else
                false
        }
        else
            false

    }

    def getLocationInfo(placesData: String, geoHashSectorSize: String): RichPipe = {
        val returnpipe = getLocationInfo(TextLine(placesData).read).project(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'geometryLatitude, 'geometryLongitude))
                .flatMapTo(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'geometryLatitude, 'geometryLongitude) ->
                ('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'geohash)) {
            fields: (String, String, String, String, String, Double, Double) =>
                val (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, latitude, longitude) = fields
                val sectorGeohash = GeoHash.withBitPrecision(latitude, longitude, geoHashSectorSize.toInt)
                val neighbouringSectors = sectorGeohash.getAdjacent().map(_.longValue())
                neighbouringSectors map (
                        sector => (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, sector)
                        )
        }
        returnpipe
    }


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

    def filterTime(chkinpipe1: RichPipe, chkinpipe2: RichPipe, timediff: String, geoHashSectorSize: String): RichPipe = {
        val pipe = chkinpipe1.joinWithSmaller('keyid -> 'keyid1, chkinpipe2).project(('keyid, 'venName1, 'chknTime1, 'latitude, 'longitude, 'venName, 'chknTime, 'latitude1, 'longitude1))

                .filter(('venName1, 'venName)) {
            fields: (String, String) =>
                val (venname1, venname2) = fields
                (!venname1.equals(venname2))
        }.filter(('chknTime1, 'chknTime)) {
            fields: (String, String) =>
                val (chknTime1, chknTime2) = fields
                (deltatime(chknTime1, chknTime2, timediff))
        }
                .mapTo(('keyid, 'venName1, 'latitude, 'longitude, 'venName, 'latitude1, 'longitude1) ->('venNameFROM, 'latitudeFrom1, 'longitudeFrom1, 'venNameTO, 'latitudeTo1, 'longitudeTo1, 'countTIMES, 'keyidS)) {
            fields: (String, String, String, String, String, String, String) =>
                val (keyid, venname1, latitude, longitude, venname2, latitude2, longitude2) = fields
                (venname1, latitude, longitude, venname2, latitude2, longitude2, 1, keyid) //add one to do a sum('countTIMES) inside the pipe to find out total count of users moving from  'venNameFROM to 'venNameTO
        }
        val geohashedpipe = convertlatlongToGeohash(pipe, geoHashSectorSize)
        geohashedpipe
    }

    def convertlatlongToGeohash(inpipe: RichPipe, geoHashSectorSize: String
                                       ): RichPipe = {
        val outpipe = inpipe.mapTo(('venNameFROM, 'latitudeFrom1, 'longitudeFrom1, 'venNameTO, 'latitudeTo1, 'longitudeTo1, 'countTIMES, 'keyidS) ->('venNameFROM, 'ghashFrom, 'venNameTO, 'ghashTo, 'countTIMES, 'keyidS)) {
            fields: (String, Double, Double, String, Double, Double, Int, String) =>
                val (venname1, latitude, longitude, venname2, latitude2, longitude2, cnt, keys) = fields
                val ghash1 = GeoHash.withBitPrecision(latitude, longitude, geoHashSectorSize.toInt).longValue()
                val ghash2 = GeoHash.withBitPrecision(latitude2, longitude2, geoHashSectorSize.toInt).longValue()

                (StemAndMetaphoneEmployer.getStemmed(venname1), ghash1, StemAndMetaphoneEmployer.getStemmed(venname2), ghash2, cnt, keys)
        }
        outpipe
    }


    def classifyBayes(bayestrainingmodel: String, chkinpipefileterdtime: RichPipe, training: String, trainingclassified: String, trainingclassifiedfinal: String): RichPipe = {
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
