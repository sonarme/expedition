package com.sonar.expedition.scrawler.jobs

import util.matching.Regex
import EmployerFinder._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.EmployerCheckinMatch

// currently checks employerGroupedServiceProfiles and userGroupedCheckins to find matches for work location names, and prints out sonar id, location name, lat, and long

class EmployerFinder(args: Args) extends Job(args) {

    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val checkinInput = "/tmp/userGroupedCheckins.txt"
    val out = "/tmp/locationMatch.txt"

    val employerGroupedEmployeeUserIds = (TextLine(serviceProfileInput).read.project('line).map(('line) ->('employer, 'workers)) {
        line: String => {
            line match {
                case ExtractFromList(employer, workers) => (employer, workers)
                case _ => ("None", "None")
            }
        }
    }).flatMap(('workers) -> ('listofworkers)) {
        fields: String =>
            val workerString = fields
            val employees = workerString.split(", ")
            (employees)
    }.project(('employer, 'listofworkers))


    val userIdGroupedCheckins = (TextLine(checkinInput).read.project('line).map(('line) ->('userId, 'venueName, 'latitude, 'longitude)) {
        line: String => {
            line match {
                case ExtractCheckin(userId, venue, lat, lng) => (userId, venue, lat, lng)
                case _ => ("None", "None", "None", "None")
            }
        }
    })
    //            .map(('userId, 'checkins) -> ('id, 'listofcheckins)) {
    //        fields : (String, String) =>
    //            val (userIdString, checkinString) = fields
    //            val checkinVenues = checkinString.split(", ").toList
    //            (userIdString, checkinVenues)
    //    }


    val joined = userIdGroupedCheckins.joinWithSmaller('userId -> 'listofworkers, employerGroupedEmployeeUserIds)
            .mapTo(('userId, 'venueName, 'employer, 'latitude, 'longitude) ->('numberId, 'venueName, 'employer, 'latitude, 'longitude)) {
        fields: (String, String, String, String, String) =>
            val (userId, venue, workplace, lat, lng) = fields
            val matcher = new EmployerCheckinMatch
            val matchedName = matcher.checkMetaphone(workplace, venue)
            var result = ("", "", "", "", "")
            if (matchedName == true) {
                result = (userId, workplace, venue, lat, lng)
            }
            result
    }
            .filter('numberId) {
        id: String => !id.matches("")
    }
            .write(TextLine(out))


}


object EmployerFinder {
    val ExtractFromList: Regex = """(.*)\tList\((.*)\)""".r
    //    val ExtractFromCheckinList: Regex = """(.*)\tList\((.*)\)\tList\((.*)\)\tList\((.*)\)""".r
    val ExtractCheckin: Regex = """(.*)\t(.*)\t(.*)\t(.*)""".r

}
