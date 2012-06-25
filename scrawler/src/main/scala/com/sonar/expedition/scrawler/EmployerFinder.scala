package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import util.matching.Regex
import org.apache.commons.codec.language._
import EmployerFinder._
import com.twitter.scalding.{Job, Args, TextLine}

// currently checks employerGroupedServiceProfiles and userGroupedCheckins to find matches for work location names, and prints out sonar id and location name

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


    val userIDGroupedCheckins = (TextLine(checkinInput).read.project('line).map(('line) ->('userID, 'checkins)) {
        line: String => {
            line match {
                case ExtractFromList(userID, checkins) => (userID, checkins)
                case _ => ("None","None")
            }
        }
    }).map(('userID, 'checkins) -> ('id, 'listofcheckins)) {
        fields : (String, String) =>
            val (userIDString, checkinString) = fields
            val checkinVenues = checkinString.split(", ").toList
            (userIDString, checkinVenues)
    }



    val joined = userIDGroupedCheckins.joinWithSmaller('id -> 'listofworkers, employerGroupedEmployeeUserIDs)
            .map(('id, 'listofcheckins, 'employer) -> ('numberID, 'venueName)){
        fields : (String, List[String], String) =>
            val (userID, listcheckins, workplace) = fields
            val matcher = new EmployerCheckinMatch
            var result = ("","")
            for (checkin <- listcheckins){
            val matchedName = matcher.checkStemMatch(workplace, checkin)
            if(matchedName)
                result = (userID, workplace)
            }
            result
    }
            .filter('numberID){id : String => !id.matches("")}
            .project(('numberID, 'venueName))
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

}
