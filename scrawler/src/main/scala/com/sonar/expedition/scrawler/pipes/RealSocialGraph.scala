package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.Haversine
import java.util.Calendar

/*

group by friends, time, and location

group checkins by friends, sort by time, filter by location and

*/

class RealSocialGraph(args: Args) extends Job(args) {

    val havver = new Haversine


    // similar to friends at same venue
    /*
    def friendsNearbyByFriends(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val userIdGroupedFriends = friendsInput.project('userProfileId, 'serviceProfileId, 'friendName)
                .map(('userProfileId, 'serviceProfileId) ->('uId, 'serviceId)) {
            fields: (String, String) =>
                val (userIdString, serviceProfileId) = fields
                val uIdString = userIdString.trim
                val serviceId = serviceProfileId.trim
                (uIdString, serviceId)
        }.project('uId, 'serviceId)

        val findFriendSonarId = serviceIdsInput.project('row_keyfrnd, 'fbId, 'lnId)

        val facebookFriends = findFriendSonarId.joinWithLarger('fbId -> 'serviceId, userIdGroupedFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val linkedinFriends = findFriendSonarId.joinWithLarger('lnId -> 'serviceId, userIdGroupedFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val mergedFriends = linkedinFriends.++(facebookFriends)
                .project('uId, 'row_keyfrnd, 'fbId, 'lnId)

        val friendsCheckins = checkinInput.joinWithSmaller('keyid -> 'row_keyfrnd, mergedFriends)
                .rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour) ->
                ('friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour))
                .unique('uId, 'friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour)

        val matchingCheckins = checkinInput.joinWithSmaller('keyid -> 'uId, friendsCheckins)
                .filter('loc, 'friendLoc, 'friendDayOfYear, 'dayOfYear, 'friendHour, 'hour) {
            fields: (String, String, Int, Int, Double, Double) =>
                val (originalLoc, friendLoc, friendDay, originalDay, friendHour, originalHour) = fields
                val originalLat = originalLoc.split(":").head.toDouble
                val originalLng = originalLoc.split(":").last.toDouble
                val friendLat = friendLoc.split(":").head.toDouble
                val friendLng = friendLoc.split(":").last.toDouble
                (havver.haversine(originalLat, originalLng, friendLat, friendLng) < 0.3) && (friendDay == originalDay) && (originalHour <= (friendHour + 1.5)) && (originalHour >= (friendHour - 1.5)) && (!originalLoc.equals("0.0:0.0")) && (!friendLoc.equals("0.0:0.0"))
        } //.project('uId, 'friendKey, 'loc, 'friendLoc, 'dayOfYear, 'friendDayOfYear, 'hour, 'friendHour)

        matchingCheckins
    } */


    // group into chunks and cross within chunks
    def friendsNearbyByChunks(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {
        // ********
        // chunking
        // ********
        val chunked = checkinInput
                // filter out (0,0)
                .filter('loc) {
            loc: String => !loc.equals("0.0:0.0")
        }
                // group time by 4 hour chunks
                .map('chknTime ->('timeChunk, 'timeChunk2)) {
            checkinTime: String => {
                val timeFilter = Calendar.getInstance()
                val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                timeFilter.setTime(checkinDate)
                ((timeFilter.getTimeInMillis / 14400000), ((timeFilter.getTimeInMillis + 7200000) / 14400000)) //1000 * 60 * 60 * 4 = for 4 hour chunks
            }
        }
                // group location by 0.002 x 0.002 boxes
                .map('loc ->('locChunk, 'locChunk2, 'locChunk3, 'locChunk4)) {
            locString: String => {
                val lat = locString.split(":").head.toDouble
                val lng = locString.split(":").last.toDouble
                val latInt = (lat * 500).toInt
                val lngInt = (lng * 500).toInt
                val latInt2 = (lat * 500 + 0.5).toInt
                val lngInt2 = (lng * 500 + 0.5).toInt
                ((latInt + ":" + lngInt), (latInt + ":" + lngInt2), (latInt2 + ":" + lngInt), (latInt2 + ":" + lngInt2))
            }

        }

        // do 8 times, one for each timechunk and locchunk pair
        val chunked1 = groupChunk(chunked)
        val chunked2 = groupChunk(chunked.discard('locChunk).rename('locChunk2 -> 'locChunk))
        val chunked3 = groupChunk(chunked.discard('locChunk).rename('locChunk3 -> 'locChunk))
        val chunked4 = groupChunk(chunked.discard('locChunk).rename('locChunk4 -> 'locChunk))
        val chunked5 = groupChunk(chunked.discard('timeChunk).rename('timeChunk2 -> 'timeChunk))
        val chunked6 = groupChunk(chunked.discard('timeChunk).rename('timeChunk2 -> 'timeChunk).discard('locChunk).rename('locChunk2 -> 'locChunk))
        val chunked7 = groupChunk(chunked.discard('timeChunk).rename('timeChunk2 -> 'timeChunk).discard('locChunk).rename('locChunk3 -> 'locChunk))
        val chunked8 = groupChunk(chunked.discard('timeChunk).rename('timeChunk2 -> 'timeChunk).discard('locChunk).rename('locChunk4 -> 'locChunk))


        val chunked12 = chunked1.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))
        val chunked22 = chunked2.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))
        val chunked32 = chunked3.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))
        val chunked42 = chunked4.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))
        val chunked52 = chunked5.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))
        val chunked62 = chunked6.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))
        val chunked72 = chunked7.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))
        val chunked82 = chunked8.rename(('keyid, 'serType, 'serProfileID) ->('keyid2, 'serType2, 'serProfileID2))

        val joinedChunks1 = chunked1
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked12)
        val joinedChunks2 = chunked2
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked22)
        val joinedChunks3 = chunked3
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked32)
        val joinedChunks4 = chunked4
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked42)
        val joinedChunks5 = chunked5
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked52)
        val joinedChunks6 = chunked6
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked62)
        val joinedChunks7 = chunked7
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked72)
        val joinedChunks8 = chunked8
                .joinWithSmaller(('timeChunk, 'locChunk) ->('timeChunk, 'locChunk), chunked82)

        val joinedChunks = (joinedChunks1 ++ joinedChunks2 ++ joinedChunks3 ++ joinedChunks4 ++ joinedChunks5 ++ joinedChunks6 ++ joinedChunks7 ++ joinedChunks8)
                .unique(('timeChunk, 'locChunk, 'keyid, 'serType, 'serProfileID, 'keyid2, 'serType2, 'serProfileID2))
//        val joinedChunks = joinedChunks1

        //********
        // merging
        //********
        val userIdGroupedFriends = friendsInput.project('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)
                .map(('userProfileId, 'serviceProfileId) ->('uId, 'serviceId)) {
            fields: (String, String) =>
                val (userIdString, serviceProfileId) = fields
                val uIdString = userIdString.trim
                val serviceId = serviceProfileId.trim
                (uIdString, serviceId)
        }.project('uId, 'serviceType, 'serviceId)

        val findFriendSonarId = serviceIdsInput.project('friendkey, 'fbid, 'lnid, 'twid, 'fsid)

        val facebookFriends = findFriendSonarId
                .joinWithLarger('fbid -> 'serviceId, userIdGroupedFriends)
                .filter('serviceType) {
            serviceType: String => serviceType.equals("facebook")
        }
                .project('uId, 'friendkey, 'fbid, 'lnid, 'twid, 'fsid)

        val linkedinFriends = findFriendSonarId
                .joinWithLarger('lnid -> 'serviceId, userIdGroupedFriends)
                .filter('serviceType) {
            serviceType: String => serviceType.equals("linkedin")
        }
                .project('uId, 'friendkey, 'fbid, 'lnid, 'twid, 'fsid)

        val twitterFriends = findFriendSonarId
                .joinWithLarger('twid -> 'serviceId, userIdGroupedFriends)
                .filter('serviceType) {
            serviceType: String => serviceType.equals("twitter")
        }
                .project('uId, 'friendkey, 'fbid, 'lnid, 'twid, 'fsid)

        val foursquareFriends = findFriendSonarId
                .joinWithLarger('fsid -> 'serviceId, userIdGroupedFriends)
                .filter('serviceType) {
            serviceType: String => serviceType.equals("foursquare")
        }
                .project('uId, 'friendkey, 'fbid, 'lnid, 'twid, 'fsid)

        val mergedFriends = linkedinFriends.++(facebookFriends).++(twitterFriends).++(foursquareFriends)
                .unique('uId, 'friendkey, 'fbid, 'lnid, 'twid, 'fsid)

        val friendsCheckins = joinedChunks.joinWithSmaller(('keyid, 'keyid2) ->('uId, 'friendkey), mergedFriends)
                .groupBy(('keyid, 'keyid2)){
            _.size
        }
                .joinWithLarger('keyid2 -> 'friendkey, serviceIdsInput)
                .rename('uname -> 'uname2)
                .project(('keyid, 'keyid2, 'uname2, 'size))
                .joinWithLarger('keyid -> 'friendkey, serviceIdsInput)
                .project(('keyid, 'keyid2, 'uname, 'uname2, 'size))






        friendsCheckins
    }

    def groupChunk(input: RichPipe): RichPipe = {
        val output = input.unique(('timeChunk, 'locChunk, 'keyid, 'serType, 'serProfileID))
                // filter out chunks of size one, then flatten
                .groupBy('timeChunk, 'locChunk) {
            _.toList[(String, String, String)](('keyid, 'serType, 'serProfileID) -> 'checkinList)
                    .size
        }
                .filter('size) {
            size: Int => size > 1
        }
                .flatMap('checkinList ->('keyid, 'serType, 'serProfileID)) {
            fields: List[(String, String, String)] => fields
        }
                .project(('timeChunk, 'locChunk, 'keyid, 'serType, 'serProfileID))
        output
    }


}
