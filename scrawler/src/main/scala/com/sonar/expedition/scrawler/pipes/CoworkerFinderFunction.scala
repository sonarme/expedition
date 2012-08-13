package com.sonar.expedition.scrawler.pipes

import util.matching.Regex
import com.sonar.expedition.scrawler.pipes.CoworkerFinderFunction._
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.twitter.scalding.{RichPipe, Job, Args}

class CoworkerFinderFunction(args: Args) extends Job(args) {

    def findCoworkers(serviceProfileInput: RichPipe, friendsInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val employerGroupedEmployeeUserIds = (serviceProfileInput.flatMap(('line) ->('employer, 'workers)) {
            line: String => {
                line match {
                    case ExtractFromList(employer, workers) => Some((employer, workers))
                    case _ => None
                }
            }
        }).project('employer, 'workers).map('employer -> 'emp) {
            fields: (String) =>
                val (employer) = fields
                val emp = employer
                val fuzzyemp = StemAndMetaphoneEmployer.getStemmedMetaphone(emp)
                fuzzyemp

        }.flatMap('workers -> ('listofworkers)) {
            fields: (String) =>
                val (workerString) = fields
                val employees = workerString.split(", ")
                employees
        }.project('emp, 'listofworkers)

        //        val userIdGroupedCheckins = (checkinsInput.map(('line) ->('userkey, 'checkin)) {
        //            line: String => {
        //                line match {
        //                    case ExtractFromCheckin(userkey, checkin) => (userkey, checkin)
        //                    case _ => ("None","None")
        //                }
        //            }
        //        }).project('userkey, 'checkin)

        //        val checkinGrouper = new CheckinGrouperFunction(args)


        val userIdGroupedFriends = (friendsInput.map(('line) ->('userId, 'friends)) {
            line: String => {
                line match {
                    case ExtractFromList(userId, friends) => Some((userId, friends))
                    case _ => None
                }
            }
        }).project('userId, 'friends).flatMap('friends -> ('listoffriends)) {
            fields: (String) =>
                val (friendsString) = fields
                val friendServiceProfileIds = friendsString.split(", ").toList
                friendServiceProfileIds
        }.project('userId, 'listoffriends).map('userId, 'uId) {
            fields: (String) =>
                val (userIdString) = fields
                val uIdString = userIdString
                uIdString
        }.project('uId, 'listoffriends)

        //        val employerAndFriends = userIdGroupedFriends.joinWithLarger('uId -> 'listofworkers, employerGroupedEmployeeUserIds).project('uId, 'emp, 'listoffriends).write(TextLine(EandF))

        val findFriendSonarId = (serviceIdsInput.map(('line) ->('row_keyfrnd, 'fbId, 'lnId)) {
            line: String => {
                line match {
                    case ExtractIds(userId, fbId, lnId) => Some((userId, fbId, lnId))
                    case _ => None
                }
            }
        }).project('row_keyfrnd, 'fbId, 'lnId)

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'listoffriends, userIdGroupedFriends)
                .rename(('uId, 'row_keyfrnd, 'fbId, 'lnId) ->('originalUserId, 'friendUId, 'fbookId, 'linkedinId))

        val facebookFriendEmployers = facebookFriends.joinWithLarger('friendUId -> 'listofworkers, employerGroupedEmployeeUserIds)
                .rename(('emp, 'originalUserId) ->('emplyer, 'row_keyfbuser))
                .project('row_keyfbuser, 'friendUId, 'emplyer).unique('row_keyfbuser, 'friendUId, 'emplyer)


        val facebookCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keyfbuser, facebookFriendEmployers)
                .filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.rename(('row_keyfbuser, 'emplyer) ->('originalUId, 'employer)).project('originalUId, 'friendUId, 'employer).unique('originalUId, 'friendUId, 'employer)

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'listoffriends, userIdGroupedFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val linkedinFriendEmployers = linkedinFriends.joinWithLarger('row_keyfrnd -> 'listofworkers, employerGroupedEmployeeUserIds)
                .rename(('emp, 'uId) ->('emplyer, 'row_keylnuser))
                .project('row_keylnuser, 'row_keyfrnd, 'emplyer).unique('row_keylnuser, 'row_keyfrnd, 'emplyer)


        val linkedinCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keylnuser, linkedinFriendEmployers)
                .filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.rename(('row_keylnuser, 'row_keyfrnd, 'emplyer) ->('originalUId, 'friendUId, 'employer)).project('originalUId, 'friendUId, 'employer).unique('originalUId, 'friendUId, 'employer)

        val mergedCoWorkers = linkedinCoworkers.++(facebookCoworkers)
                .project('originalUId, 'friendUId, 'employer)

        mergedCoWorkers
    }

    def findCoworkerCheckins(serviceProfileInput: RichPipe, friendsInput: RichPipe, serviceIdsInput: RichPipe, checkinInput: RichPipe): RichPipe = {

        val employerGroupedEmployeeUserIds = (serviceProfileInput.map(('line) ->('employer, 'workers)) {
            line: String => {
                line match {
                    case ExtractFromList(employer, workers) => Some((employer, workers))
                    case _ => None
                }
            }
        })

                .project('employer, 'workers).map('employer -> 'emp) {
            fields: (String) =>
                val (employer) = fields
                val emp = employer
                val fuzzyemp = StemAndMetaphoneEmployer.getStemmedMetaphone(emp)
                fuzzyemp
        }.flatMap('workers -> ('listofworkers)) {
            fields: (String) =>
                val (workerString) = fields
                val employees = workerString.split(", ")
                employees
        }.project('emp, 'listofworkers)

        //        val userIdGroupedCheckins = (checkinsInput.map(('line) ->('userkey, 'checkin)) {
        //            line: String => {
        //                line match {
        //                    case ExtractFromCheckin(userkey, checkin) => (userkey, checkin)
        //                    case _ => ("None","None")
        //                }
        //            }
        //        }).project('userkey, 'checkin)

        val userIdGroupedFriends = (friendsInput.map(('line) ->('userId, 'friends)) {
            line: String => {
                line match {
                    case ExtractFromList(userId, friends) => Some((userId, friends))
                    case _ => None
                }
            }
        }).project('userId, 'friends).flatMap('friends -> ('listoffriends)) {
            fields: (String) =>
                val (friendsString) = fields
                val friendServiceProfileIds = friendsString.split(", ").toList
                friendServiceProfileIds
        }.project('userId, 'listoffriends).map('userId, 'uId) {
            fields: (String) =>
                val (userIdString) = fields
                val uIdString = userIdString
                uIdString
        }.project('uId, 'listoffriends)

        //        val employerAndFriends = userIdGroupedFriends.joinWithLarger('uId -> 'listofworkers, employerGroupedEmployeeUserIds).project('uId, 'emp, 'listoffriends).write(TextLine(EandF))

        val findFriendSonarId = (serviceIdsInput.map(('line) ->('row_keyfrnd, 'fbId, 'lnId)) {
            line: String => {
                line match {
                    case ExtractIds(userId, fbId, lnId) => Some((userId, fbId, lnId))
                    case _ => None
                }
            }
        }).project('row_keyfrnd, 'fbId, 'lnId)

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'listoffriends, userIdGroupedFriends)
                .rename(('uId, 'row_keyfrnd, 'fbId, 'lnId) ->('originalUserId, 'friendUId, 'fbookId, 'linkedinId))

        val facebookFriendEmployers = facebookFriends.joinWithLarger('friendUId -> 'listofworkers, employerGroupedEmployeeUserIds)
                .rename(('emp, 'originalUserId) ->('emplyer, 'row_keyfbuser))
                .project('row_keyfbuser, 'friendUId, 'emplyer).unique('row_keyfbuser, 'friendUId, 'emplyer)


        val facebookCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keyfbuser, facebookFriendEmployers)
                .filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.rename(('row_keyfbuser, 'emplyer) ->('originalUId, 'employer)).project('originalUId, 'friendUId, 'employer).unique('originalUId, 'friendUId, 'employer)

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'listoffriends, userIdGroupedFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val linkedinFriendEmployers = linkedinFriends.joinWithLarger('row_keyfrnd -> 'listofworkers, employerGroupedEmployeeUserIds)
                .rename(('emp, 'uId) ->('emplyer, 'row_keylnuser))
                .project('row_keylnuser, 'row_keyfrnd, 'emplyer).unique('row_keylnuser, 'row_keyfrnd, 'emplyer)


        val linkedinCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keylnuser, linkedinFriendEmployers)
                .filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.rename(('row_keylnuser, 'row_keyfrnd, 'emplyer) ->('originalUId, 'friendUId, 'employer)).project('originalUId, 'friendUId, 'employer).unique('originalUId, 'friendUId, 'employer)

        val mergedCoworkers = linkedinCoworkers.++(facebookCoworkers)
                .project('originalUId, 'friendUId, 'employer)

        val checkinGrouper = new CheckinGrouperFunction(args)
        val checkinPipe = checkinGrouper.groupCheckins(checkinInput)

        val mergedCoworkerCheckins = checkinPipe.joinWithSmaller('keyid -> 'friendUId, mergedCoworkers)
                .project('keyid, 'originalUId, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)
                .unique('keyid, 'originalUId, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

        mergedCoworkerCheckins
    }

    def findCoworkerCheckinsPipe(userEmployer: RichPipe, friendsInput: RichPipe, serviceIdsInput: RichPipe, pipeCheckIns: RichPipe): RichPipe = {

        val employerGroupedEmployeeUserIds = userEmployer.map('worked -> 'emp) {
            employer: String =>
            /*if (employer == null) null else TODO */
                println("findCoworkerCheckinsPipe export");
                StemAndMetaphoneEmployer.getStemmed(employer).take(30)

        }.project(('emp, 'key))

        val userIdGroupedFriends = friendsInput.project('userProfileId, 'serviceProfileId, 'friendName)
                .map(('userProfileId, 'serviceProfileId) ->('uId, 'serviceId)) {
            fields: (String, String) =>
                val (userIdString, serviceProfileId) = fields
                val uIdString = userIdString
                val serviceId = serviceProfileId
                (uIdString, serviceId)
        }.project('uId, 'serviceId)

        val findFriendSonarId = serviceIdsInput.project('row_keyfrnd, 'fbId, 'lnId)

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'serviceId, userIdGroupedFriends)
                .rename(('uId, 'row_keyfrnd, 'fbId, 'lnId) ->('originalUserId, 'friendUId, 'fbookId, 'linkedinId))

        val facebookFriendEmployers = facebookFriends.joinWithLarger('friendUId -> 'key, employerGroupedEmployeeUserIds)
                .rename(('emp, 'originalUserId) ->('emplyer, 'row_keyfbuser))
                .project('row_keyfbuser, 'friendUId, 'emplyer).unique('row_keyfbuser, 'friendUId, 'emplyer)

        val facebookCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('key -> 'row_keyfbuser, facebookFriendEmployers)
                .filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.rename(('row_keyfbuser, 'emplyer) ->('originalUId, 'employer)).project('originalUId, 'friendUId, 'employer).unique('originalUId, 'friendUId, 'employer)

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'serviceId, userIdGroupedFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val linkedinFriendEmployers = linkedinFriends.joinWithLarger('row_keyfrnd -> 'key, employerGroupedEmployeeUserIds)
                .rename(('emp, 'uId) ->('emplyer, 'row_keylnuser))
                .project('row_keylnuser, 'row_keyfrnd, 'emplyer).unique('row_keylnuser, 'row_keyfrnd, 'emplyer)


        val linkedinCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('key -> 'row_keylnuser, linkedinFriendEmployers)
                .filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.rename(('row_keylnuser, 'row_keyfrnd, 'emplyer) ->('originalUId, 'friendUId, 'employer)).project('originalUId, 'friendUId, 'employer).unique('originalUId, 'friendUId, 'employer)

        val mergedCoworkers = linkedinCoworkers.++(facebookCoworkers)
                .project('originalUId, 'friendUId, 'employer)

        val mergedCoworkerCheckins = pipeCheckIns.joinWithSmaller('keyid -> 'friendUId, mergedCoworkers)
                .rename('keyid -> 'key)
                .project('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)
                .unique('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

        mergedCoworkerCheckins
    }


}


object CoworkerFinderFunction {
    val ExtractFromList: Regex = """(.*)List\((.*)\)""".r
    val ExtractIds: Regex = """(.*)\t(.*)\t(.*)""".r
}
