package com.sonar.expedition.scrawler.jobs

import util.matching.Regex
import EmployerFinder._
import com.twitter.scalding.{Job, TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.util.EmployerCheckinMatch
import EmployerCheckinMatch._

// currently checks employerGroupedServiceProfiles and userGroupedCheckins to find matches for work location names, and prints out sonar id, location name, lat, and long
// JUST FOR TESTING
class EmployerFinder(args: Args) extends Job(args) {

    val serviceProfileInput = args("employerGroupedServiceProfiles")
    val checkinInput = args("userGroupedCheckins")
    val out = args("locationMatch")

    val employerGroupedEmployeeUserIds = (TextLine(serviceProfileInput).read.project('line).flatMap(('line) ->('employer, 'listofworkers)) {
        line: String => {
            line match {
                case ExtractFromList(employer, workers) => Some((employer, workers.split(",")))
                case _ => None
            }
        }
    })

    val userIdGroupedCheckins = (TextLine(checkinInput).read.project('line).flatMap(('line) ->('userId, 'venueName, 'latitude, 'longitude)) {
        line: String => {
            line match {
                case ExtractCheckin(userId, venue, lat, lng) => Some((userId, venue, lat, lng))
                case _ => None
            }
        }
    })


    val joined = userIdGroupedCheckins.joinWithSmaller('userId -> 'listofworkers, employerGroupedEmployeeUserIds)
            .mapTo(('userId, 'venueName, 'employer, 'latitude, 'longitude) ->('numberId, 'venueName, 'employer, 'latitude, 'longitude)) {
        fields: (String, String, String, String, String) =>
            val (userId, venue, workplace, lat, lng) = fields
            val matchedName = checkMetaphone(workplace, venue)
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
