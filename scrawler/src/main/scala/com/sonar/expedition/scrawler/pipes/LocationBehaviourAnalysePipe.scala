package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import java.util.Calendar
import ch.hsr.geohash.GeoHash
import com.sonar.dossier.service.PrecomputationSettings
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import cascading.tuple.Fields
import java.text.{DecimalFormat, NumberFormat}
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine


/*
com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysis --hdfs --checkindata "/tmp/checkinDatatest.txt" --output "/tmp/output.txt" --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox" --bayesmodelforvenuetype "/tmp/bayesmodelforvenuetype" --training "/tmp/training" --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"  --placesData "/tmp/places_dump_US.geojson.txt" --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis"  --timedifference "24" --geohashsectorsize "20"
*/
trait LocationBehaviourAnalysePipe extends DTOPlacesInfoPipe with BayesModelPipe {

    def deltatime(chkintime1: String, chkintime2: String, timediff: String, prodtest: Int): Boolean = {
        val timeFilter1 = Calendar.getInstance()
        val checkinDate1 = null //
        timeFilter1.setTime(checkinDate1)
        val date1 = timeFilter1.get(Calendar.DAY_OF_YEAR)
        val time1 = timeFilter1.get(Calendar.HOUR_OF_DAY) + timeFilter1.get(Calendar.MINUTE) / 60.0
        val timeFilter2 = Calendar.getInstance()
        val checkinDate2 = null //
        timeFilter2.setTime(checkinDate2)
        val date2 = timeFilter2.get(Calendar.DAY_OF_YEAR)
        val time2 = timeFilter2.get(Calendar.HOUR_OF_DAY) + timeFilter2.get(Calendar.MINUTE) / 60.0
        if (prodtest == 1) {
            if ((time2.toDouble - time1.toDouble) > 0) {
                true
            }
            else {
                false
            }
        } else {
            if (date1.equals(date2)) {
                // need to include the timing too, which simple, if same date, check diff in time, normally we dont want checkins in border timings like 12 am.
                if ((time2.toDouble - time1.toDouble) < timediff.toDouble)
                    true
                else
                    false
            }
            else
                false
        }

    }

    def getLocationInfo(placesData: String, geoHashSectorSize: String): RichPipe = {
        val returnpipe = placesPipe(TextLine(placesData).read)
                .project(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'geometryLatitude, 'geometryLongitude))
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

    def filterTime(chkinpipe: RichPipe, chkinpipe2: RichPipe, timediff: String, geoHashSectorSize: String, prodtest: Int): RichPipe = {
        val pipe = chkinpipe.joinWithSmaller('keyid -> 'keyid1, chkinpipe2).project(('keyid, 'venName1, 'chknTime1, 'latitude, 'longitude, 'venName, 'chknTime, 'latitude1, 'longitude1))
                .unique(('keyid, 'venName1, 'chknTime1, 'latitude, 'longitude, 'venName, 'chknTime, 'latitude1, 'longitude1))
                .filter(('venName1, 'venName)) {
            fields: (String, String) =>
                val (venname1, venname2) = fields
                (!venname1.equals(venname2))
        }.filter(('chknTime1, 'chknTime)) {
            fields: (String, String) =>
                val (chknTime1, chknTime2) = fields
                (deltatime(chknTime1, chknTime2, timediff, prodtest))
        }
                .mapTo(('keyid, 'venName1, 'chknTime1, 'latitude, 'longitude, 'venName, 'chknTime, 'latitude1, 'longitude1) ->('venNameFROM, 'chknTime1, 'latitudeFrom1, 'longitudeFrom1, 'venNameTO, 'chknTime, 'latitudeTo1, 'longitudeTo1, 'countTIMES, 'keyidS)) {
            fields: (String, String, String, String, String, String, String, String, String) =>
                val (keyid, venname1, chknTime1, latitude, longitude, venname2, chknTime, latitude2, longitude2) = fields
                (venname1, chknTime1, latitude, longitude, venname2, chknTime, latitude2, longitude2, 1, keyid) //add one to do a sum('countTIMES) inside the pipe to find out total count of users moving from  'venNameFROM to 'venNameTO
        }
                .groupBy(('venNameFROM, 'venNameTO, 'keyidS)) {
            _.size
        }.rename('size -> 'countTIMES)
        val geohashedpipe = convertlatlongToGeohash(pipe, geoHashSectorSize)
        geohashedpipe

        /* val output = chkinpipe.groupAll {
    _.sortBy('chknTime)
}
.groupBy('keyid) {
    _.reduce(('chknTime, 'venName, 'latitude, 'longitude) ->('chknTimeList, 'venNameList, 'latitude, 'longitude)) {

        (left: (String, String, String, String), right: (String, String, String, String)) =>


        (left._1 + "::" + right._1, left._2 + "::" + right._2, left._3 + "::" + right._3, left._4 + "::" + right._4)
    }

    //_.mapStream(('chknTime, 'venName, 'latitude, 'longitude, 'serCheckinID) ->('chknTimeList, 'venNameList, 'latitude, 'longitude, 'serCheckinID)) {
    //_.mapStream(('chknTime, 'venName, 'latitude, 'longitude, 'serCheckinID) ->('chknTimeList, 'venNameList, 'latitude, 'longitude, 'serCheckinID)) {

     /*   _.mapStream(('chknTime) ->('chknTimeList)) {

            checkins: Iterator[Fields] =>
            checkins.sliding(2)
            /*checkins: Iterator[Fields] => {
            checkins.sliding(2).filter {
                checkinPair:List[Fields] => {
                    case (checkin1, checkin2) if (DateRange(checkin1._1 + Hours(4)).contains(checkin2._1)) => true
                    case _ => false
                }
            }*//*.flatMap {
                pairAsList => {
                    val checkinEntry1 = pairAsList(0)
                    val checkinEntry2 = pairAsList(1)
                    (checkinEntry1._5, checkinEntry2._5)
                }

            }*/
     }*/

}
.mapTo(('keyid,'chknTimeList, 'venNameList, 'latitude, 'longitude)->('keyid,'chknTimeList, 'venNameList, 'latitude, 'longitude)){
    fields: (String, String, String, String,String)=>
    val (keyid,chknTimeList, venNameList, latitude, longitude) = fields

    val checkinfiltered= filterCheckin(chknTimeList)
    (keyid,checkinfiltered, venNameList, latitude, longitude)
}
        .write(TextLine("/tmp/testlist"))*/


    }


    /*def filterCheckin(checkinDates:String):Iterator[List[String]]={


          // todo
           val checkinPairs = checkinDates.split("::").toList.sliding(2)


    }*/
    def convertlatlongToGeohash(inpipe: RichPipe, geoHashSectorSize: String
                                       ): RichPipe = {
        val outpipe = inpipe.mapTo(('venNameFROM, 'chknTime1, 'latitudeFrom1, 'longitudeFrom1, 'venNameTO, 'chknTime, 'latitudeTo1, 'longitudeTo1, 'countTIMES, 'keyidS) ->
                ('venNameFROM, 'chknTimeFrom, 'chknDateFrom, 'ghashFrom, 'venNameTO, 'chknTimeTo, 'chknDateToDay, 'ghashTo, 'countTIMES, 'keyidS)) {
            fields: (String, String, Double, Double, String, String, Double, Double, Int, String) =>
                val formatter = new DecimalFormat("#0.00");
                val (venname1, chknTimeFrom, latitude, longitude, venname2, chknTimeTo, latitude2, longitude2, cnt, keys) = fields
                val ghash1 = GeoHash.withBitPrecision(latitude, longitude, geoHashSectorSize.toInt).longValue()
                val ghash2 = GeoHash.withBitPrecision(latitude2, longitude2, geoHashSectorSize.toInt).longValue()
                val timeFilter1 = Calendar.getInstance()
                val checkinDate1 = null //TODO:
                timeFilter1.setTime(checkinDate1)
                val timefrom = formatter.format((timeFilter1.get(Calendar.HOUR_OF_DAY) + timeFilter1.get(Calendar.MINUTE) / 60.0).toDouble) + " " + timeformat(timeFilter1.get(Calendar.AM_PM));
                val timeFilter2 = Calendar.getInstance()
                val checkinDate2 = null //TODO:
                timeFilter2.setTime(checkinDate2)
                val timeto = formatter.format((timeFilter2.get(Calendar.HOUR_OF_DAY) + timeFilter1.get(Calendar.MINUTE) / 60.0).toDouble) + " " + timeformat(timeFilter2.get(Calendar.AM_PM));
                val datefrom = timeFilter1.get(Calendar.YEAR) + "/" + timeFilter1.get(Calendar.MONTH) + "/" + timeFilter1.get(Calendar.DAY_OF_MONTH) + " " + getWeekDay(timeFilter1.get(Calendar.DAY_OF_WEEK))
                val dateto = timeFilter2.get(Calendar.YEAR) + "/" + timeFilter2.get(Calendar.MONTH) + "/" + timeFilter2.get(Calendar.DAY_OF_MONTH) + " " + getWeekDay(timeFilter2.get(Calendar.DAY_OF_WEEK))

                (venname1, timefrom, datefrom, ghash1, venname2, timeto, dateto, ghash2, cnt, keys)
        }
        outpipe
    }

    def getWeekDay(day: Int): String = {

        day match {
            case 1 => "Sun"
            case 2 => "Mon"
            case 3 => "Tue"
            case 4 => "Wed"
            case 5 => "Thur"
            case 6 => "Fri"
            case 7 => "Sat"
            case _ => ""

        }
    }

    def timeformat(timetype: Int): String = {

        timetype match {
            case 1 => "PM"
            case 0 => "AM"
            case _ => ""

        }
    }

    def classifyTFIDF(bayesmodel: String, chkinpipefileterdtime: RichPipe): RichPipe = {
        val seqModel = SequenceFile(bayesmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
            fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields
        }
        val chkinpipe4 = chkinpipefileterdtime.project('venNameTO).rename('venNameTO -> 'data)
        val trainedto = calcProb(seqModel, chkinpipe4).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('dataTo, 'keyTo, 'weightTo)) //project('data, 'key, 'weight)
        val classifiedplacesto = chkinpipefileterdtime.joinWithSmaller('venNameTO -> 'dataTo, trainedto).project(('keyidS, 'venNameFROM, 'ghashFrom, 'venNameTO, 'countTIMES, 'ghashTo, 'keyTo, 'weightTo)) /*.write(TextLine(trainingclassifiedfinal))*/

        val chkinpipe5 = classifiedplacesto.project('venNameFROM).rename('venNameFROM -> 'data)
        val trainedfrom = calcProb(seqModel, chkinpipe5).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('dataFrom, 'keyFrom, 'weightFrom)) //project('data, 'key, 'weight)

        val classifiedjobs = classifiedplacesto.joinWithSmaller('venNameFROM -> 'dataFrom, trainedfrom).project(('keyidS, 'countTIMES, 'venNameFROM, 'ghashFrom, 'keyFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))
        classifiedjobs.project(('keyidS, 'countTIMES, 'venNameFROM, 'ghashFrom, 'keyFrom, 'weightFrom, 'venNameTO, 'ghashTo, 'keyTo, 'weightTo))

    }


    def classifyPlaceType(bayesmodel: String, chkinpipefileterdtime: RichPipe): RichPipe = {
        val seqModel = SequenceFile(bayesmodel, ('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)).read
        val chkinpipe4 = chkinpipefileterdtime.project('venName).rename('venName -> 'data)
        val trainedto = calcProb(seqModel, chkinpipe4).project(('data, 'key, 'weight)) //project('data, 'key, 'weight)
        val classifiedplaces = chkinpipefileterdtime
                    .joinWithSmaller('venName -> 'data, trainedto)
                    .discard('data, 'weight)
                    .rename('key -> 'venueType)
        classifiedplaces
    }


}
