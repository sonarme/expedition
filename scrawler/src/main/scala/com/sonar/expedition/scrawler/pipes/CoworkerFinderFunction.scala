package com.sonar.expedition.scrawler.pipes

import util.matching.Regex
import com.twitter.scalding.{RichPipe, Job, Args, TextLine}
import cascading.pipe.joiner._
import com.sonar.expedition.scrawler.clustering.StemAndMetaphoneEmployer
import com.sonar.expedition.scrawler.pipes.CoworkerFinderFunction._

class CoworkerFinderFunction(args: Args) extends Job(args) {

    def findCoworkers(serviceProfileInput: RichPipe, friendsInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val employerGroupedEmployeeUserIds = (serviceProfileInput.map(('line) ->('employer, 'workers)) {
            line: String => {
                line match {
                    case ExtractFromList(employer, workers) => (employer, workers)
                    case _ => ("None", "None")
                }
            }
        }).project('employer, 'workers).map('employer, 'emp) {
            fields: (String) =>
                val (employer) = fields
                val emp = employer.trim
                val empMetaphone = new StemAndMetaphoneEmployer
                val fuzzyemp = empMetaphone.getStemmedMetaphone(emp)
                fuzzyemp
        }.flatMap('workers -> ('listofworkers)) {
            fields: (String) =>
                val (workerString) = fields
                val employees = workerString.trim.split(", ")
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

        val checkinGrouper = new CheckinGrouperFunction(args)


        val userIdGroupedFriends = (friendsInput.map(('line) ->('userId, 'friends)) {
            line: String => {
                line match {
                    case ExtractFromList(userId, friends) => (userId, friends)
                    case _ => ("None", "None")
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
                val uIdString = userIdString.trim
                uIdString
        }.project('uId, 'listoffriends)

        //        val employerAndFriends = userIdGroupedFriends.joinWithLarger('uId -> 'listofworkers, employerGroupedEmployeeUserIds).project('uId, 'emp, 'listoffriends).write(TextLine(EandF))

        val findFriendSonarId = (serviceIdsInput.map(('line) ->('row_keyfrnd, 'fbId, 'lnId)) {
            line: String => {
                line match {
                    case ExtractIds(userId, fbId, lnId) => (userId, fbId, lnId)
                    case _ => ("None", "None", "None")
                }
            }
        }).project('row_keyfrnd, 'fbId, 'lnId)

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'listoffriends, userIdGroupedFriends).mapTo(('uId, 'row_keyfrnd, 'fbId, 'lnId) ->('originalUserId, 'friendUId, 'fbookId, 'linkedinId)) {
            fields: (String, String, String, String) =>
                val (originalUserId, friendUId, fbookId, linkedinId) = fields
                val originalUId = originalUserId
                val frId = friendUId
                val faceId = fbookId
                val linkedId = linkedinId
                (originalUId, frId, faceId, linkedId)
        }

        val facebookFriendEmployers = facebookFriends.joinWithLarger('friendUId -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'originalUserId) ->('emplyer, 'row_keyfbuser)) {
            fields: (String, String) =>
                val (emplyer, fbUserId) = fields
                val emplyr = emplyer
                val facebookUserId = fbUserId
                (emplyr, facebookUserId)
        }.project('row_keyfbuser, 'friendUId, 'emplyer).unique('row_keyfbuser, 'friendUId, 'emplyer)


        val facebookCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keyfbuser, facebookFriendEmployers).filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.project('row_keyfbuser, 'friendUId, 'emplyer).unique('row_keyfbuser, 'friendUId, 'emplyer)

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'listoffriends, userIdGroupedFriends).project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val linkedinFriendEmployers = linkedinFriends.joinWithLarger('row_keyfrnd -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'uId) ->('emplyer, 'row_keylnuser)) {
            fields: (String, String) =>
                val (emplyer, lnUserId) = fields
                val emplyr = emplyer
                val linkedUserId = lnUserId
                (emplyr, linkedUserId)
        }.project('row_keylnuser, 'row_keyfrnd, 'emplyer).unique('row_keylnuser, 'row_keyfrnd, 'emplyer)


        val linkedinCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keylnuser, linkedinFriendEmployers).filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.project('row_keylnuser, 'row_keyfrnd, 'emp).unique('row_keylnuser, 'row_keyfrnd, 'emp)

        val mergedCoWorkers = linkedinCoworkers.joinWithSmaller(('row_keylnuser, 'row_keyfrnd, 'emp) ->('row_keyfbuser, 'friendUId, 'emplyer), facebookCoworkers, joiner = new OuterJoin).project('row_keylnuser, 'row_keyfrnd, 'emp, 'row_keyfbuser, 'friendUId, 'emplyer)

        mergedCoWorkers
    }

    def findCoworkerCheckins(serviceProfileInput: RichPipe, friendsInput: RichPipe, serviceIdsInput: RichPipe, PipeCheckIns: RichPipe): RichPipe = {

        val employerGroupedEmployeeUserIds = (serviceProfileInput.rename(('key, 'worked) ->('workers, 'employer))

                .project('employer, 'workers).map('employer, 'emp) {
            fields: (String) =>
                val (employer) = fields
                val emp = employer.trim
                val empMetaphone = new StemAndMetaphoneEmployer
                val fuzzyemp = empMetaphone.getStemmedMetaphone(emp)
                fuzzyemp
        }.flatMap('workers -> ('listofworkers)) {
            fields: (String) =>
                val (workerString) = fields
                val employees = workerString.trim.split(", ")
                employees
        }.project('emp, 'listofworkers))

        //        val userIdGroupedCheckins = (checkinsInput.map(('line) ->('userkey, 'checkin)) {
        //            line: String => {
        //                line match {
        //                    case ExtractFromCheckin(userkey, checkin) => (userkey, checkin)
        //                    case _ => ("None","None")
        //                }
        //            }
        //        }).project('userkey, 'checkin)

        val checkinGrouper = new CheckinGrouperFunction(args)
        val userIdGroupedFriends = (friendsInput.map(('line) ->('userId, 'friends)) {
            line: String => {
                line match {
                    case ExtractFromList(userId, friends) => (userId, friends)
                    case _ => ("None", "None")
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
                val uIdString = userIdString.trim
                uIdString
        }.project('uId, 'listoffriends)

        //        val employerAndFriends = userIdGroupedFriends.joinWithLarger('uId -> 'listofworkers, employerGroupedEmployeeUserIds).project('uId, 'emp, 'listoffriends).write(TextLine(EandF))

        val findFriendSonarId = serviceIdsInput
        /*(serviceIdsInput.map(('line) ->('row_keyfrnd, 'fbId, 'lnId)) {
            line: String => {
                line match {
                    case ExtractIds(userId, fbId, lnId) => (userId, fbId, lnId)
                    case _ => ("None","None", "None")
                }
            }
        }).project('row_keyfrnd, 'fbId, 'lnId)  */

        /*val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'listoffriends, userIdGroupedFriends).mapTo(('uId, 'row_keyfrnd, 'fbId, 'lnId)  -> ('originalUserId, 'friendUId, 'fbookId, 'linkedinId)) {
            fields : (String, String, String, String) =>
                val (originalUserId, friendUId, fbookId, linkedinId ) = fields
                val originalUId = originalUserId
                val frId = friendUId
                val faceId = fbookId
                val linkedId = linkedinId
                (originalUId, frId, faceId, linkedId)
        }

        val facebookFriendEmployers = facebookFriends.joinWithLarger('friendUId -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'originalUserId) -> ('emplyer, 'row_keyfbuser)) {
            fields : (String, String) =>
                val (emplyer, fbUserId) = fields
                val emplyr = emplyer
                val facebookUserId = fbUserId
                (emplyr, facebookUserId)
        }.project('row_keyfbuser, 'friendUId, 'emplyer).unique('row_keyfbuser, 'friendUId, 'emplyer)


        val facebookCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keyfbuser, facebookFriendEmployers).filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.project('row_keyfbuser, 'friendUId, 'emplyer).unique('row_keyfbuser, 'friendUId, 'emplyer)
        */
        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'listoffriends, userIdGroupedFriends).project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val linkedinFriendEmployers = linkedinFriends.joinWithLarger('row_keyfrnd -> 'listofworkers, employerGroupedEmployeeUserIds).map(('emp, 'uId) ->('emplyer, 'row_keylnuser)) {
            fields: (String, String) =>
                val (emplyer, lnUserId) = fields
                val emplyr = emplyer
                val linkedUserId = lnUserId
                (emplyr, linkedUserId)
        }.project('row_keylnuser, 'row_keyfrnd, 'emplyer).unique('row_keylnuser, 'row_keyfrnd, 'emplyer)


        val linkedinCoworkers = employerGroupedEmployeeUserIds.joinWithSmaller('listofworkers -> 'row_keylnuser, linkedinFriendEmployers).filter('emp, 'emplyer) {
            fields: (String, String) => {
                val (originalEmployer, friendsEmployer) = fields
                originalEmployer.equalsIgnoreCase(friendsEmployer)
            }
        }.project('row_keylnuser, 'row_keyfrnd, 'emp).unique('row_keylnuser, 'row_keyfrnd, 'emp)

        //val mergedCoWorkers = linkedinCoworkers.joinWithSmaller(('row_keylnuser, 'row_keyfrnd, 'emp)  -> ('row_keyfbuser, 'friendUId, 'emplyer), facebookCoworkers, joiner = new OuterJoin).project('row_keylnuser, 'row_keyfrnd, 'emp, 'row_keyfbuser, 'friendUId, 'emplyer)

        //val lnCoworkerCheckins = linkedinCoworkers.joinWithSmaller('row_keyfrnd -> 'keyid, checkinGrouper.groupCheckins(checkinsInput)).project('row_keylnuser, 'row_keyfrnd, 'emp, 'venName, 'loc)
        //val lnCoworkerCheckins = linkedinCoworkers.joinWithSmaller('row_keyfrnd -> 'keyid, checkinGrouper.groupCheckins(checkinsInput)).project('row_keylnuser, 'row_keyfrnd)

        /*val fbCoworkerCheckins = facebookCoworkers.joinWithSmaller('friendUId -> 'keyid, checkinGrouper.groupCheckins(checkinsInput)).map(('venName, 'loc) -> ('fbvenName, 'fbloc)) {
            fields : (String, String) =>
                val (venName, loc) = fields
                val fbvenName = venName
                val fbloc = loc
                (fbvenName, fbloc)
        }.project('row_keyfbuser, 'friendUId, 'emplyer, 'fbvenName, 'fbloc)*/

        //val mergedCoworkerCheckins = lnCoworkerCheckins.++(fbCoworkerCheckins)
        val mergedCoworkerCheckins = linkedinCoworkers.joinWithLarger('row_keyfrnd -> 'keyid, PipeCheckIns).project('row_keyfrnd, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)
                .rename('row_keyfrnd -> 'key)
                .project('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc)

        mergedCoworkerCheckins

        //'key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'serType, 'serProfileID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc
    }

}


object CoworkerFinderFunction {
    val ExtractFromList: Regex = """(.*)List\((.*)\)""".r
    val ExtractIds: Regex = """(.*)\t(.*)\t(.*)""".r
}
