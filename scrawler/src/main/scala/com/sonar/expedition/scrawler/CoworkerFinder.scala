package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import util.matching.Regex
import com.sonar.expedition.scrawler.CoworkerFinder._
import org.apache.commons.codec.language._
import CheckinTimeFilter._
import com.twitter.scalding.{Job, Args, TextLine}

class CoworkerFinder(args: Args) extends Job(args) {

    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val friendsInput = "/tmp/userGroupedFriends.txt"
    val serviceIDs = "/tmp/serviceIDs.txt"
    val out = "/tmp/coworkerGroups.txt"

    val employerGroupedEmployeeUserIDs = (TextLine(serviceProfileInput).read.project('line).map(('line) ->('employer, 'workers)) {
        line: String => {
            line match {
                case ExtractFromList(employer, workers) => (employer, workers)
                case _ => ("None","None")
            }
        }
    }).project('workers).map('workers -> ('listofworkers)) {
        fields : (String) =>
            val (workerString) = fields
            val employees = workerString.split(", ").toList
            employees
    }

    val userIDGroupedFriends = (TextLine(friendsInput).read.project('line).map(('line) ->('userID, 'friends)) {
        line: String => {
            line match {
                case ExtractFromList(userID, friends) => (userID, friends)
                case _ => ("None","None")
            }
        }
    }).project('friends).map('friends -> ('listoffriends)) {
        fields : (String) =>
            val (friendsString) = fields
            val friendServiceProfileIDs = friendsString.split(", ").toList
            friendServiceProfileIDs
    }

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


object CoworkerFinder{
    val ExtractFromList: Regex = """(.*)List\((.*)\)""".r

}
