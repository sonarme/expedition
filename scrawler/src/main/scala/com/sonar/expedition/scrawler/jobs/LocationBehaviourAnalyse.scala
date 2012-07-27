package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{CheckinTimeFilter, CheckinGrouperFunction, CheckinInfoPipe}
import cascading.tuple.Fields
import java.util.{Calendar, Date}

/*

checkin_nomsg.txt
checkinanalyser.txt
*/
class LocationBehaviourAnalyse(args: Args) extends Job(args) {

    val checkinInfoPipe = new CheckinGrouperFunction(args)
    val chkindata = TextLine(args("checkindata"))
    val chkindataoutput = TextLine("output")
    val chkindataoutput1 = TextLine("output1")

    val chkindataoutput2 = TextLine("output2")

    val chkinpipe = checkinInfoPipe.unfilteredCheckinsLatLon(chkindata).project(Fields.ALL)
    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime,'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour

    val peopleanalysis = chkinpipe.groupAll {
        _.sortBy('chknTime)
    }
            .groupBy('keyid) {
        _
                .toList[String]('venName, 'venName1)
                .toList[String]('venAddress, 'venAddress1)
                .toList[Date]('chknTime, 'chknTime1)
                .toList[String]('ghash, 'ghash1)
                .toList[String]('latitude, 'lt1)
                .toList[String]('longitude, 'ln1)
                .toList[String]('dayOfYear, 'dayOfYear1)
                .toList[String]('hour, 'hour1)


    }
            .write(chkindataoutput1)


    val placeanalysis = chkinpipe.groupBy('venName) {
        _
                .toList[String]('keyid, 'keyid1)
                .toList[String]('venAddress, 'venAddress1)
                .toList[Date]('chknTime, 'chknTime1)
                .toList[String]('ghash, 'ghash1)
                .toList[String]('latitude, 'lt1)
                .toList[String]('longitude, 'ln1)
                .toList[String]('dayOfYear, 'dayOfYear1)
                .toList[String]('hour, 'hour1)

    }
            .filter('keyid1) {
        fields: (List[String]) =>
            val (profiles) = fields
            (profiles.size > 1)
    }
            .write(chkindataoutput2)


    val chkinpipe1 = chkinpipe.project(('keyid, 'venName, 'chknTime))

    val chkinpipe2 = chkinpipe.project(('venName, 'keyid, 'chknTime)).rename(('venName, 'keyid, 'chknTime) ->('venName1, 'keyid1, 'chknTime1))

    var chkinpipe3 = chkinpipe1.joinWithSmaller('keyid -> 'keyid1, chkinpipe2).project(('venName1, 'chknTime1, 'venName, 'chknTime))

            .filter(('venName1, 'chknTime1, 'venName, 'chknTime)) {
        fields: (String, String, String, String) =>
            val (venname1, chknTime1, venname2, chknTime2) = fields
            (!venname1.equals(venname2))
    }.filter(('venName1, 'chknTime1, 'venName, 'chknTime)) {
        fields: (String, String, String, String) =>
            val (venname1, chknTime1, venname2, chknTime2) = fields
            (deltatime(chknTime1, chknTime2))
    }
            .mapTo(('venName1, 'venName) ->('venName1, 'venName, 'count)) {
        fields: (String, String) =>
            val (venname1, venname2) = fields
            (venname1, venname2, 1)
    }
            .groupBy(('venName1, 'venName)) {
        _
                .toList[String]('count, 'count1)
    }.mapTo(('venName1, 'venName, 'count1) ->('venName1, 'venName, 'count2)) {
        fields: (String, String, List[String]) =>
            val (venname1, venname2, cnt) = fields
            (venname1, venname2, cnt.size)
    }.unique(('venName1, 'venName, 'count2))
            .groupBy('venName1) {
        _
                .toList[String]('venName -> 'venName2)
                .toList[String]('count2 -> 'count3)


    }.write(chkindataoutput)


    def deltatime(chkintime1: String, chkintime2: String): Boolean = {

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
            true
        }
        else
            false

    }

}
