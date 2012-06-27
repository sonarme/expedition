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

    val employerGroupedEmployeeUserIds = (TextLine(serviceProfileInput).read.project('line).map(('line) ->('employer, 'workers)) {
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


    val userIdGroupedFriends = (TextLine(friendsInput).read.project('line).map(('line) ->('userId, 'friends)) {
        line: String => {
            line match {
                case ExtractFromList(userId, friends) => (userId, friends)
                case _ => ("None","None")
            }
        }
    }).project('userId, 'friends).map('friends -> ('listoffriends)) {
        fields : (String) =>
            val (friendsString) = fields
            val friendServiceProfileIds = friendsString.split(", ").toList
            friendServiceProfileIds
    }.project('userId, 'listoffriends).map('userId, 'uId) {
        fields : (String) =>
            val (userIdString) = fields
            val uIdString = userIdString.trim
            uIdString
    }.project('uId, 'listoffriends)

val employerAndFriends = userIdGroupedFriends.joinWithLarger('uId -> 'listofworkers, employerGroupedEmployeeUserIds).project('uId, 'employer, 'listoffriends).write(TextLine(EandF))
//
//val findFriendSonarId =


//     .groupBy('userProfileId){
//        _
//                .toList[String]('serviceType -> 'serviceTypelist)
//                .toList[String]('json -> 'jsonList)
//    }.project('userProfileId, 'serviceTypelist, 'jsonList)



}


object CoworkerFinder{
    val ExtractFromList: Regex = """(.*)List\((.*)\)""".r
}
