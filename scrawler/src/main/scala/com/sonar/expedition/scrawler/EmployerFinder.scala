package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import util.matching.Regex
import org.apache.commons.codec.language._
import EmployerFinder._
import com.twitter.scalding.{Job, Args, TextLine}

// currently checks employerGroupedServiceProfiles and userGroupedCheckins to find matches for work location names, and prints out sonar id, location name, lat, and long

class EmployerFinder(args: Args) extends Job(args) {

    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val checkinInput = "/tmp/userGroupedCheckins.txt"
    val out = "/tmp/locationMatch.txt"

    val employerGroupedEmployeeUserIDs = (TextLine(serviceProfileInput).read.project('line).map(('line) ->('employer, 'workers)) {
        line: String => {
            line match {
                case ExtractFromList(employer, workers) => (employer, workers)
                case _ => ("None","None")
            }
        }
    }).flatMap(('workers) -> ('listofworkers)) {
        fields : String =>
            val workerString = fields
            val employees = workerString.split(", ")
            (employees)
    }.project(('employer, 'listofworkers))


    val userIDGroupedCheckins = (TextLine(checkinInput).read.project('line).map(('line) ->('userID, 'venueName, 'latitude, 'longitude)) {
        line: String => {
            line match {
                case ExtractCheckin(userID, venue, lat, lng) => (userID, venue, lat, lng)
                case _ => ("None","None","None","None")
            }
        }
    })
//            .map(('userID, 'checkins) -> ('id, 'listofcheckins)) {
//        fields : (String, String) =>
//            val (userIDString, checkinString) = fields
//            val checkinVenues = checkinString.split(", ").toList
//            (userIDString, checkinVenues)
//    }



    val joined = userIDGroupedCheckins.joinWithSmaller('userID -> 'listofworkers, employerGroupedEmployeeUserIDs)
            .mapTo(('userID, 'venueName, 'employer, 'latitude, 'longitude) -> ('numberID, 'venueName, 'employer, 'latitude, 'longitude)){
        fields : (String, String, String, String, String) =>
            val (userID, venue, workplace, lat, lng) = fields
            val matcher = new EmployerCheckinMatch
            val matchedName = matcher.checkMetaphone(workplace, venue)
            var result = ("","","","","")
            if(matchedName ==  true){
                result = (userID, workplace, venue, lat, lng)}
            result
    }
            .filter('numberID){id : String => !id.matches("")}
            .write(TextLine(out))



}


//    def isCoWorker(employer: String, friend: FriendObjects): Boolean = {
//        val timeHour = getTimeFromString(checkin.getCheckinTime)
//        (timeHour < 18 && timeHour > 8)
//
//    }
//
//    def getTimeFromString(timeString : String): Int = {
//        val timeHour = {
//            timeString match {
//                case ExtractTime(other,hour) => (hour.toInt)
//                case _ => -1
//            }
//        }
//        timeHour
//    }
//


object EmployerFinder{
    val ExtractFromList: Regex = """(.*)\tList\((.*)\)""".r
//    val ExtractFromCheckinList: Regex = """(.*)\tList\((.*)\)\tList\((.*)\)\tList\((.*)\)""".r
    val ExtractCheckin: Regex = """(.*)\t(.*)\t(.*)\t(.*)""".r

}
