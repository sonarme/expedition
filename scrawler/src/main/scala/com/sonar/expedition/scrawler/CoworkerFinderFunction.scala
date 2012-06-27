package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import util.matching.Regex
import com.sonar.expedition.scrawler.CoworkerFinderFunction._
import org.apache.commons.codec.language._
import CheckinTimeFilter._
import com.twitter.scalding.{RichPipe, Job, Args, TextLine}
import cascading.pipe.joiner._

class CoworkerFinderFunction(args: Args) extends Job(args) {

    def findCoworkers(serviceProfileInput : RichPipe, friendsInput : RichPipe, serviceIdsInput : RichPipe): RichPipe = {

        val employerGroupedEmployeeUserIds = (serviceProfileInput.map(('line) ->('employer, 'workers)) {
            line: String => {
                line match {
                    case ExtractFromList(employer, workers) => (employer, workers)
                    case _ => ("None","None")
                }
            }
        }).project('employer, 'workers).map('employer, 'emp) {
            fields : (String) =>
                val (employer) = fields
                val emp = employer.trim
                val empMetaphone = new StemAndMetaphoneEmployer
                val fuzzyemp = empMetaphone.getStemmedMetaphone(emp)
                fuzzyemp
        }.flatMap('workers -> ('listofworkers)) {
            fields : (String) =>
                val (workerString) = fields
                val employees = workerString.trim.split(", ")
                employees
        }.project('emp, 'listofworkers)


        val userIdGroupedFriends = (friendsInput.map(('line) ->('userId, 'friends)) {
            line: String => {
                line match {
                    case ExtractFromList(userId, friends) => (userId, friends)
                    case _ => ("None","None")
                }
            }
        }).project('userId, 'friends).flatMap('friends -> ('listoffriends)) {
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

//        val employerAndFriends = userIdGroupedFriends.joinWithLarger('uId -> 'listofworkers, employerGroupedEmployeeUserIds).project('uId, 'emp, 'listoffriends).write(TextLine(EandF))

        val findFriendSonarId = (serviceIdsInput.map(('line) ->('friendUserId, 'fbId, 'lnId)) {
            line: String => {
                line match {
                    case ExtractIds(userId, fbId, lnId) => (userId, fbId, lnId)
                    case _ => ("None","None", "None")
                }
            }
        }).project('friendUserId, 'fbId, 'lnId)

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'listoffriends, userIdGroupedFriends).mapTo(('uId, 'friendUserId, 'fbId, 'lnId)  -> ('originalUserId, 'friendUId, 'fbookId, 'linkedinId)) {
            fields : (String, String, String, String) =>
                val (originalUserId, friendUId, fbookId, linkedinId ) = fields
                val originalUId = originalUserId
                val frId = friendUId
                val faceId = fbookId
                val linkedId = linkedinId
                (originalUId, frId, faceId, linkedId)
        }

        val facebookFriendEmployers = facebookFriends.joinWithLarger('friendUId -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'originalUserId) -> ('emplyer, 'fboriginalUId)) {
            fields : (String, String) =>
                val (emplyer, fbUserId) = fields
                val emplyr = emplyer
                val facebookUserId = fbUserId
                (emplyr, facebookUserId)
        }.project('fboriginalUId, 'friendUId, 'emplyer).unique('fboriginalUId, 'friendUId, 'emplyer)


        val facebookCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'fboriginalUId, facebookFriendEmployers).filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.project('fboriginalUId, 'friendUId, 'emplyer).unique('fboriginalUId, 'friendUId, 'emplyer)

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'listoffriends, userIdGroupedFriends).project('uId, 'friendUserId, 'fbId, 'lnId)

        val linkedinFriendEmployers = linkedinFriends.joinWithLarger('friendUserId -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'uId) -> ('emplyer, 'lnoriginalUId)) {
            fields : (String, String) =>
                val (emplyer, lnUserId) = fields
                val emplyr = emplyer
                val linkedUserId = lnUserId
                (emplyr, linkedUserId)
        }.project('lnoriginalUId, 'friendUserId, 'emplyer).unique('lnoriginalUId, 'friendUserId, 'emplyer)


        val linkedinCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'lnoriginalUId, linkedinFriendEmployers).filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.project('lnoriginalUId, 'friendUserId, 'emp).unique('lnoriginalUId, 'friendUserId, 'emp)

        val mergedCoWorkers = linkedinCoworkers.joinWithSmaller('lnoriginalUId -> 'emplyer, facebookCoworkers, joiner = new OuterJoin).project('lnoriginalUId, 'friendUserId, 'emp, 'fboriginalUId, 'friendUId, 'emplyer)



        //    val allFriends = linkedinFriends.joinWithLarger('uId -> 'originalUserId, facebookFriends, joiner = new RightJoin).project('uId, 'friendUserId, 'fbId, 'lnId, 'originalUserId, 'friendUId, 'fbookId, 'linkedinId).unique('uId, 'friendUserId, 'fbId, 'lnId, 'originalUserId, 'friendUId, 'fbookId, 'linkedinId).write(TextLine(allfriends))
        //    val friendsEmploymer = linkedinFriends.
        //        .flatMap('workers -> ('listofworkers)) {
        //        fields : (String) =>
        //            val (workerString) = fields
        //            val employees = workerString.trim.split(", ")
        //            employees
        //    }.project('employer, 'listofworkers)


        //     .groupBy('userProfileId){
        //        _
        //                .toList[String]('serviceType -> 'serviceTypelist)
        //                .toList[String]('json -> 'jsonList)
        //    }.project('userProfileId, 'serviceTypelist, 'jsonList)

        mergedCoWorkers

    }

}


object CoworkerFinderFunction{
    val ExtractFromList: Regex = """(.*)List\((.*)\)""".r
    val ExtractIds: Regex = """(.*)\t(.*)\t(.*)""".r
}
