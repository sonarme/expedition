/*
package com.sonar.expedition.scrawler.jobs

import util.matching.Regex
import com.twitter.scalding.{Args, TextLine}
import cascading.pipe.joiner.OuterJoin
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.sonar.expedition.scrawler.jobs
import com.sonar.expedition.scrawler.jobs.CoworkerFinder._

class CoworkerFinder(args: Args) extends Job(args) {

    val out = "/tmp/coworkerGroups.txt"
    val temp = "/tmp/tempcoworkerGroups.txt"
    val EandF = "/tmp/employerAndFriends.txt"
    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val friendsInput = "/tmp/userGroupedFriends.txt"
    val serviceIdsInput = "/tmp/serviceIds.txt"
    val pipedcoworkers = "/tmp/pipedcoworkers.txt"

    val employerGroupedEmployeeUserIds = (TextLine(serviceProfileInput).read.project('line).flatMap(('line) ->('employer, 'workers)) {
        line: String => {
            line match {
                case ExtractFromList(employer, workers) => Some((employer, workers))
                case _ => None
            }
        }
    }).project(('employer, 'workers)).map('employer, 'emp) {
        fields: (String) =>
            val (employer) = fields
            val emp = employer.trim
            val fuzzyemp = StemAndMetaphoneEmployer.getStemmedMetaphone(emp)
            fuzzyemp
    }.flatMap('workers -> ('listofworkers)) {
        fields: (String) =>
            val (workerString) = fields
            val employees = workerString.trim.split(", ")
            employees
    }.project(('emp, 'listofworkers))


    val userIdGroupedFriends = (TextLine(friendsInput).read.project('line).map(('line) ->('userId, 'friends)) {
        line: String => {
            line match {
                case ExtractFromList(userId, friends) => Some((userId, friends))
                case _ => None
            }
        }
    }).project(('userId, 'friends)).flatMap('friends -> ('listoffriends)) {
        fields: (String) =>
            val (friendsString) = fields
            val friendServiceProfileIds = friendsString.split(", ").toList
            friendServiceProfileIds
    }.project(('userId, 'listoffriends)).map('userId, 'uId) {
        fields: (String) =>
            val (userIdString) = fields
            val uIdString = userIdString.trim
            uIdString
    }.project(('uId, 'listoffriends))

    //val employerAndFriends = userIdGroupedFriends.joinWithLarger('uId -> 'listofworkers, employerGroupedEmployeeUserIds).project('uId, 'employer, 'listoffriends).write(TextLine(EandF))

    val findFriendSonarId = (TextLine(serviceIdsInput).read.project('line).flatMap(('line) ->('friendUserId, 'fbId, 'lnId)) {
        line: String => {
            line match {
                case ExtractIds(userId, fbId, lnId) => Some((userId, fbId, lnId))
                case _ => None
            }
        }
    }).project(('friendUserId, 'fbId, 'lnId))

    val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'listoffriends, userIdGroupedFriends).mapTo(('uId, 'friendUserId, 'fbId, 'lnId) ->('originalUserId, 'friendUId, 'fbookId, 'linkedinId)) {
        fields: (String, String, String, String) =>
            val (originalUserId, friendUId, fbookId, linkedinId) = fields
            val originalUId = originalUserId
            val frId = friendUId
            val faceId = fbookId
            val linkedId = linkedinId
            (originalUId, frId, faceId, linkedId)
    }

    val facebookFriendEmployers = facebookFriends.joinWithLarger('friendUId -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'originalUserId) ->('emplyer, 'fboriginalUId)) {
        fields: (String, String) =>
            val (emplyer, fbUserId) = fields
            val emplyr = emplyer
            val facebookUserId = fbUserId
            (emplyr, facebookUserId)
    }.project(('fboriginalUId, 'friendUId, 'emplyer)).unique(('fboriginalUId, 'friendUId, 'emplyer))


    val facebookCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'fboriginalUId, facebookFriendEmployers).filter(('emp, 'emplyer)) {
        fields: (String, String) => {
            val (originalEmployer, friendsEmployer) = fields
            originalEmployer.equalsIgnoreCase(friendsEmployer)
        }
    }.project(('fboriginalUId, 'friendUId, 'emplyer)).unique(('fboriginalUId, 'friendUId, 'emplyer))

    val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'listoffriends, userIdGroupedFriends).project(('uId, 'friendUserId, 'fbId, 'lnId))

    val linkedinFriendEmployers = linkedinFriends.joinWithLarger('friendUserId -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'uId) ->('emplyer, 'lnoriginalUId)) {
        fields: (String, String) =>
            val (emplyer, lnUserId) = fields
            val emplyr = emplyer
            val linkedUserId = lnUserId
            (emplyr, linkedUserId)
    }.project(('lnoriginalUId, 'friendUserId, 'emplyer)).unique(('lnoriginalUId, 'friendUserId, 'emplyer))


    val linkedinCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'lnoriginalUId, linkedinFriendEmployers).filter(('emp, 'emplyer)) {
        fields: (String, String) => {
            val (originalEmployer, friendsEmployer) = fields
            originalEmployer.equalsIgnoreCase(friendsEmployer)
        }
    }.project(('lnoriginalUId, 'friendUserId, 'emp)).unique(('lnoriginalUId, 'friendUserId, 'emp))

    val mergedCoWorkers = linkedinCoworkers.joinWithSmaller(('lnoriginalUId, 'friendUserId, 'emp) ->('fboriginalUId, 'friendUId, 'emplyer), facebookCoworkers, joiner = new OuterJoin).project(('lnoriginalUId, 'friendUserId, 'emp, 'fboriginalUId, 'friendUId, 'emplyer)).write(TextLine(out))


}


object CoworkerFinder {
    val ExtractFromList: Regex = """(.*)List\((.*)\)""".r
    val ExtractIds: Regex = """(.*)\t(.*)\t(.*)""".r
}
*/
