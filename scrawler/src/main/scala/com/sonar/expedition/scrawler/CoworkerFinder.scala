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
    val out = "/tmp/coworkerGroups.txt"
    val temp = "/tmp/tempcoworkerGroups.txt"
    val EandF = "/tmp/employerAndFriends.txt"

    val employerGroupedEmployeeUserIDs = (TextLine(serviceProfileInput).read.project('line).map(('line) ->('employer, 'workers)) {
        line: String => {
            line match {
                case ExtractFromList(employer, workers) => (employer, workers)
                case _ => ("None","None")
            }
        }
    }).project('employer, 'workers).flatMap('workers -> ('listofworkers)) {
        fields : (String) =>
            val (workerString) = fields
            val employees = workerString.trim.split(", ")
            employees
    }.project('employer, 'listofworkers)


    val userIDGroupedFriends = (TextLine(friendsInput).read.project('line).map(('line) ->('userID, 'friends)) {
        line: String => {
            line match {
                case ExtractFromList(userID, friends) => (userID, friends)
                case _ => ("None","None")
            }
        }
    }).project('userID, 'friends).map('friends -> ('listoffriends)) {
        fields : (String) =>
            val (friendsString) = fields
            val friendServiceProfileIDs = friendsString.split(", ").toList
            friendServiceProfileIDs
    }.project('userID, 'listoffriends).map('userID, 'uID) {
        fields : (String) =>
            val (userIDString) = fields
            val uIDString = userIDString.trim
            uIDString
    }.project('uID, 'listoffriends)

val employerAndFriends = userIDGroupedFriends.joinWithLarger('uID -> 'listofworkers, employerGroupedEmployeeUserIDs).project('uID, 'employer, 'listoffriends).write(TextLine(EandF))
//
//val findFriendSonarID =


//     .groupBy('userProfileID){
//        _
//                .toList[String]('serviceType -> 'serviceTypelist)
//                .toList[String]('json -> 'jsonList)
//    }.project('userProfileID, 'serviceTypelist, 'jsonList)



}


object CoworkerFinder{
    val ExtractFromList: Regex = """(.*)List\((.*)\)""".r
}
