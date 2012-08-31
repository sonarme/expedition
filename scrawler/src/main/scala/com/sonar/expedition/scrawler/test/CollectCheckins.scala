package com.sonar.expedition.scrawler.test

import cascading.tuple.Fields
import com.sonar.expedition.scrawler.pipes.{CheckinTimeFilter, CheckinInfoPipe}
import com.twitter.scalding._
import util.matching.Regex
import com.twitter.scalding.TextLine

// JUST FOR TESTING
class CollectCheckins(args: Args) extends Job(args) with CheckinInfoPipe {
    val chkininputData = TextLine(args("input"))
    val output = TextLine(args("output"))
    val output1 = TextLine(args("output1"))
    val output2 = TextLine(args("output2"))

    //rowkey,fbname, fbid,  lnid, work_company, curr_city, jobtype, checkin lat,  checkin long,  venue name,  time_chkin


    val chkres1 = getCheckinsDataPipeCollectinLatLon(chkininputData.read).project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln))
            .groupBy(Fields.ALL) {
        _.sortBy(('keyid, 'chknTime))
    }
            .filter('venName) {
        venue: String => (venue.startsWith("Ippudo") || venue.startsWith("Totto") || venue.startsWith("momofuku") || venue.startsWith("Bobby Van"))
    }
            .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln))


    val userschkinsforplaces = chkres1.project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln))
            .rename('keyid -> 'keyid2)
            .project('keyid2)
    //.write(TextLine("/tmp/ci.txt"))

    val chkres2 = getCheckinsDataPipeCollectinLatLon(chkininputData.read) //('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)
    //.project('keyid)
    //.write(TextLine("/tmp/co.txt"))


    val userchkdInToPlaces = chkres2.joinWithSmaller('keyid -> 'keyid2, userschkinsforplaces)
            .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lt, 'ln))
            .write(output1)

    chkres1.write(output)
    /*
    find time separted chkins
    val chkres1= getCheckinsDataPipe(TextLine(chkininputData).read).project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)
            .groupBy(Fields.ALL){ _.sortBy('keyid,'chknTime) }
            .filter('venName)
            {
                venue:String => (!venue.equalsIgnoreCase(""))
            }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

    val chkres= chkres1.project('keyid,'venName,'loc,'chknTime)
   .groupBy('keyid){
       group:GroupBuilder =>
       group.toList[String]('venName,'venue)
       group.toList[String]('chknTime,'chkin)
       group.toList[String]('loc,'location)
   }.project('keyid,'venue,'chkin,'location)
   .mapTo(('keyid,'venue,'chkin,'location) -> ('keyid1,'venue1,'chkin1,'location1)){
       fields: (String, List[String], List[String],List[String]) =>
       val (key, venue, chkin,location) = fields

       filterList(chkin)

    }*/

    val chkres3 = getCheckinsDataPipe(chkininputData.read).project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))
            .groupBy(Fields.ALL) {
        _.sortBy(('keyid, 'chknTime))
    }
            .filter('venName) {
        venue: String => (!venue.equalsIgnoreCase(""))
    }.project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))
            .project(('keyid, 'venName, 'loc, 'chknTime))

    val chkres4 = chkres3.rename(('keyid, 'venName, 'loc, 'chknTime) ->('keyid2, 'venName2, 'loc2, 'chknTime2))

    val join = chkres4.joinWithSmaller('keyid2 -> 'keyid, chkres3).filter(('venName, 'venName2)) {
        venue: (String, String) => venue._1 < venue._2
    }
            .project(('keyid, 'venName, 'loc, 'chknTime, 'venName2, 'loc2, 'chknTime2))
            .filter(('chknTime, 'chknTime2)) {
        fields: (String, String) =>
            val (chkintime1, chkintime2) = fields
            val parsedCheckinTime1 = CheckinTimeFilter.removeTrailingTimezoneColon(chkintime1)
            val parsedCheckinTime2 = CheckinTimeFilter.removeTrailingTimezoneColon(chkintime2)
            val checkinDate1 = CheckinTimeFilter.parseDateTime(parsedCheckinTime1)
            val checkinDate2 = CheckinTimeFilter.parseDateTime(parsedCheckinTime2)
            //println("d1 : " + checkinDate1.getTime())
            //println("d2 : " + (checkinDate2.getTime() - checkinDate1.getTime()))
            (((checkinDate2.getTime() - checkinDate1.getTime()) < 14400000) && ((checkinDate2.getTime() - checkinDate1.getTime()) > 0))
    }

            .groupBy(Fields.ALL) {
        _.sortBy('keyid)
    }

            .write(output2)


}

// rowkey,fbname, fbid,  lnid, work_company, curr_city, jobtype, checkin lat,  checkin long,  venue name,  time_chkin

object CollectCheckins {

}
