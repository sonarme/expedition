package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{CheckinGrouperFunction, CheckinInfoPipe}
import cascading.tuple.Fields
import java.util.Date

class LocationBehaviourAnalyse(args: Args) extends Job(args) {

    val checkinInfoPipe = new CheckinGrouperFunction(args)

    val chkinpipe = checkinInfoPipe.unfilteredCheckinsLatLon(TextLine("/tmp/checkin_nomsg.txt")).project(Fields.ALL)

    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour
    val peopleanalysis= chkinpipe.groupAll { _.sortBy('chknTime) }
    .groupBy('keyid){
        _
            .toList[String]('venName,'venName1)
            .toList[String]('venAddress,'venAddress1)
            .toList[Date]('chknTime,'chknTime1)
            .toList[String]('ghash,'ghash1)
            .toList[String]('latitude,'lt1)
            .toList[String]('longitude,'ln1)
            .toList[String]('dayOfYear,'dayOfYear1)
            .toList[String]('hour,'hour1)


    }
    .write(TextLine("/tmp/checkinDatatest.txt"))


    val placeanalysis=chkinpipe.groupBy('venName){
        _
                .toList[String]('keyid,'keyid1)
                .toList[String]('venAddress,'venAddress1)
                .toList[Date]('chknTime,'chknTime1)
                .toList[String]('ghash,'ghash1)
                .toList[String]('latitude,'lt1)
                .toList[String]('longitude,'ln1)
                .toList[String]('dayOfYear,'dayOfYear1)
                .toList[String]('hour,'hour1)

    }
    .filter('keyid1){
        fields: (List[String]) =>
        val (profiles) = fields
        (profiles.size > 1)
    }
    .write(TextLine("/tmp/checkinDatatest2.txt"))



    val chkinpipe1 = chkinpipe.project('keyid,'venName,'chknTime)

    val chkinpipe2 = chkinpipe.project('venName,'keyid,'chknTime).rename(('venName,'keyid,'chknTime) -> ('venName1,'keyid1,'chknTime1))

    var chkinpipe3= chkinpipe1.joinWithSmaller('keyid -> 'keyid1,chkinpipe2).project('venName1,'venName)
            .filter('venName1,'venName){
            fields: (String,String) =>
            val (venname1,venname2) = fields
            (!venname1.equals(venname2))
    }
    .mapTo(('venName1,'venName) -> ('venName1,'venName,'count)){
        fields: (String,String) =>
        val (venname1,venname2) = fields
        (venname1,venname2,1)
    }
    .groupBy(('venName1,'venName)){
        _
                .toList[String]('count,'count1)
     }.mapTo(('venName1,'venName,'count1) -> ('venName1,'venName,'count2)){
        fields: (String,String,List[String]) =>
            val (venname1,venname2,cnt) = fields
            (venname1,venname2,cnt.size)
    }.unique('venName1,'venName,'count2)
     .groupBy('venName1){
        _
            .toList[String]('venName->'venName2)
            .toList[String]('count2->'count3)


    }.write(TextLine("/tmp/checkinDatatest3.txt"))




}
