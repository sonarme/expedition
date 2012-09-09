package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.Haversine
import java.util.Calendar


/*

group by friends, time, and location

group checkins by friends, sort by time, filter by location and

*/

trait RealSocialGraph extends ScaldingImplicits {

    val hourPrecision = 3
    val locationPrecision = 0.3

    // similar to friends at same venue

    def friendsNearbyByFriends(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val mergedFriends = mergeFriends(serviceIdsInput, friendsInput)



        val chunkedCheckins = checkinInput
                // group by time to avoid counting too much, count once per day
                .map('chknTime -> 'timeChunk) {
            checkinTime: String => {
                val timeFilter = Calendar.getInstance()
                val checkinDate = CheckinTimeFilter.parseDateTime(checkinTime)
                timeFilter.setTime(checkinDate)
                (timeFilter.getTimeInMillis / 86400000) // 1000 * 60 * 60 * 24 = for 24 hour chunks
            }
        }
                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour, 'timeChunk))

        val friendsCheckins = chunkedCheckins.joinWithSmaller('keyid -> 'friendkey, mergedFriends)
                .rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour, 'timeChunk) ->
                ('friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour, 'friendTimeChunk))
                .unique('uId, 'friendKey, 'friendService, 'friendProfileID, 'friendCheckinID, 'friendVenName, 'friendVenAddress, 'friendChknTime, 'friendGhash, 'friendLoc, 'friendDayOfYear, 'friendHour, 'friendTimeChunk)

        val crossDayCheckins = chunkedCheckins.flatMap(('timeChunk, 'hour) ->('timeChunk2, 'hour2)) {
            fields: (Int, Double) => {
                val (chunk, hour) = fields
                if (hour < hourPrecision)
                    Some(chunk - 1, hour + 24)
                else if (hour > (24 - hourPrecision))
                    Some(chunk + 1, hour - 24)
                else
                    None
            }
        }
                .discard(('timeChunk, 'hour))
                .rename(('timeChunk2, 'hour2) ->('timeChunk, 'hour))
                .project(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc, 'dayOfYear, 'hour, 'timeChunk))

        val mergedCheckins = (chunkedCheckins ++ crossDayCheckins)
                .joinWithSmaller(('keyid, 'timeChunk) ->('uId, 'friendTimeChunk), friendsCheckins)
                .filter('loc, 'friendLoc, 'friendTimeChunk, 'timeChunk, 'friendHour, 'hour) {
            fields: (String, String, Int, Int, Double, Double) =>
                val (originalLoc, friendLoc, friendDay, originalDay, friendHour, originalHour) = fields
                val originalLat = originalLoc.split(":").head.toDouble
                val originalLng = originalLoc.split(":").last.toDouble
                val friendLat = friendLoc.split(":").head.toDouble
                val friendLng = friendLoc.split(":").last.toDouble
                (Haversine.haversineInKm(originalLat, originalLng, friendLat, friendLng) < locationPrecision) && (friendDay == originalDay) && (originalHour <= (friendHour + hourPrecision)) && (originalHour >= (friendHour - hourPrecision)) && (!originalLoc.equals("0.0:0.0")) && (!friendLoc.equals("0.0:0.0"))
        }


        val matchingCheckins = mergedCheckins

                // count once per day
                .unique(('uId, 'friendKey, 'timeChunk))
                .groupBy(('uId, 'friendKey)) {
            _.size
        }
                .joinWithLarger('friendKey -> 'friendkey, serviceIdsInput)
                .rename('uname -> 'uname2)
                .project(('uId, 'friendKey, 'uname2, 'size))
                .joinWithLarger('uId -> 'friendkey, serviceIdsInput)
                .project(('uId, 'friendKey, 'uname, 'uname2, 'size))

        matchingCheckins
    }


    // group into chunks and cross within chunks
    def friendsNearbyByChunks(friendsInput: RichPipe, checkinInput: RichPipe, serviceIdsInput: RichPipe): RichPipe = {

        val joinedChunks = timeLocChunking(checkinInput)

        //********
        // merging
        //********

        val mergedFriends = mergeFriends(serviceIdsInput, friendsInput)


        val friendsCheckins = joinedChunks.joinWithSmaller(('keyid, 'keyid2) ->('uId, 'friendkey), mergedFriends)
                .groupBy(('keyid, 'keyid2)) {
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

    def mergeFriends(serviceInput: RichPipe, friendInput: RichPipe): RichPipe = {

        val userIdGroupedFriends = friendInput.project('userProfileId, 'serviceType, 'serviceProfileId)
                .map(('userProfileId, 'serviceProfileId) ->('uId, 'serviceId)) {
            fields: (String, String) =>
                val (userIdString, serviceProfileId) = fields
                val uIdString = userIdString.trim
                val serviceId = serviceProfileId.trim
                (uIdString, serviceId)
        }.project('uId, 'serviceType, 'serviceId)

        val findFriendSonarId = serviceInput.project('friendkey, 'fbid, 'lnid, 'twid, 'fsid)

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

        mergedFriends

    }

    def timeLocChunking(checkinInput: RichPipe): RichPipe = {

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
                // group location by 0.002 x 0.002 boxes, roughly 200m by 200m at equator, slightly smaller when farther from equator
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

                //val joinedChunks = (joinedChunks1 ++ joinedChunks2 ++ joinedChunks3 ++ joinedChunks4 ++ joinedChunks5 ++ joinedChunks6 ++ joinedChunks7 ++ joinedChunks8)
                .unique(('timeChunk, 'locChunk, 'keyid, 'serType, 'serProfileID, 'keyid2, 'serType2, 'serProfileID2))
        val joinedChunks = joinedChunks1

        joinedChunks

    }


}
