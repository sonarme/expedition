package com.sonar.expedition.scrawler.pipes

import util.matching.Regex
import EmployerFinderFunction._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.EmployerCheckinMatch

// currently checks employerGroupedServiceProfiles and userGroupedCheckins to find matches for work location names, and prints out sonar id, location name, lat, and long

class EmployerFinderFunction(args: Args) extends Job(args) {

    def findEmployeesFromText(serviceProfileInput: RichPipe, checkinInput: RichPipe): RichPipe = {


        val employerGroupedEmployeeUserIds = (serviceProfileInput.map(('line) ->('employer, 'workers)) {
            line: String => {
                line match {
                    case ExtractFromList(employer, workers) => Some((employer, workers))
                    case _ => None
                }
            }
        }).flatMap(('workers) -> ('listofworkers)) {
            fields: String =>
                val workerString = fields
                val employees = workerString.split(", ")
                (employees)
        }.project(('employer, 'listofworkers))


        val userIdGroupedCheckins = (checkinInput.map(('line) ->('userId, 'venueName, 'latitude, 'longitude)) {
            line: String => {
                line match {
                    case ExtractCheckin(userId, venue, lat, lng) => Some((userId, venue, lat, lng))
                    case _ => None
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

        joined


    }

    def findEmployees(serviceProfileInput: RichPipe, checkinInput: RichPipe): RichPipe = {


        val employerGroupedEmployeeUserIds = (serviceProfileInput.map(('line) ->('employer, 'workers)) {
            line: String => {
                line match {
                    case ExtractFromList(employer, workers) => Some((employer, workers))
                    case _ => None
                }
            }
        }).flatMap(('workers) -> ('listofworkers)) {
            fields: String =>
                val workerString = fields
                val employees = workerString.split(", ")
                (employees)
        }.project(('employer, 'listofworkers))


        ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)
        val userIdGroupedCheckins = checkinInput.rename('keyid -> 'userId).rename('venName -> 'venueName)



        val joined = userIdGroupedCheckins.joinWithSmaller('userId -> 'listofworkers, employerGroupedEmployeeUserIds)
                .map(('userId, 'venueName, 'employer, 'loc) ->('numberId, 'venueName1, 'employer1, 'loc1)) {
            fields: (String, String, String, String) =>
                val (userId, venue, workplace, loc) = fields
                val matcher = new EmployerCheckinMatch
                val matchedName = matcher.checkMetaphone(workplace, venue)
                var result = ("", "", "", "")
                if (matchedName == true) {
                    result = (userId, workplace, venue, loc)
                }
                result
        }
                .filter('numberId) {
            id: String => !id.matches("")
        }.project(('numberId, 'venueName1, 'employer1, 'loc1))

        joined


    }
}


object EmployerFinderFunction {
    val ExtractFromList: Regex = """(.*)\tList\((.*)\)""".r
    //    val ExtractFromCheckinList: Regex = """(.*)\tList\((.*)\)\tList\((.*)\)\tList\((.*)\)""".r
    val ExtractCheckin: Regex = """(.*)\t(.*)\t(.*)\t(.*)""".r

}
