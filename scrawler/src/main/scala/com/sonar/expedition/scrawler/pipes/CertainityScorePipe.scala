package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Job, Args, RichPipe}
import cascading.pipe.joiner.LeftJoin
import com.sonar.expedition.scrawler.util.LocationScorer
import java.security.MessageDigest

class CertainityScorePipe(args: Args) extends Job(args) {

    val scorer = new LocationScorer

    def stemmingAndScore(filteredProfiles: RichPipe, findcityfromchkins: RichPipe, placesPipe: RichPipe, numberOfFriends: RichPipe): RichPipe = {
        val filteredProfilesWithScore = filteredProfiles.joinWithSmaller('key -> 'key1, findcityfromchkins).project(('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'centroid, 'stemmedWorked))
                .map('mtphnWorked -> 'worked) {
            fields: (String) =>
                var (mtphnWorked) = fields
                if (mtphnWorked == null || mtphnWorked == "") {
                    mtphnWorked = " "
                }
                mtphnWorked
        }
                .joinWithSmaller('worked -> 'mtphnName, placesPipe, joiner = new LeftJoin).project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'centroid, 'geometryLatitude, 'geometryLongitude, 'stemmedName, 'stemmedWorked))
                .map('centroid ->('lat, 'long)) {
            fields: String =>
                val (centroid) = fields
                val latLongArray = centroid.split(":")
                val lat = latLongArray.head
                val long = latLongArray.last
                (lat, long)
        }
                .map(('stemmedWorked, 'lat, 'long, 'stemmedName, 'geometryLatitude, 'geometryLongitude) ->('score, 'certainty)) {
            fields: (String, String, String, String, String, String) =>
                val (work, workLatitude, workLongitude, place, placeLatitude, placeLongitude) = fields
                val score = scorer.getScore(work, workLatitude, workLongitude, place, placeLatitude, placeLongitude)
                val certainty = scorer.certaintyScore(score, work, place)
                (score, certainty)
        }
                .groupBy(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked)) {
            _
                    .toList[(Double, String, String)](('certainty, 'geometryLatitude, 'geometryLongitude) -> 'certaintyList)
        }
                .map(('certaintyList) ->('certaintyScore, 'geometryLatitude, 'geometryLongitude)) {
            fields: (List[(Double, String, String)]) =>
                val (certaintyList) = fields
                val certainty = certaintyList.max
                (certainty._1, certainty._2, certainty._3)
        }.project(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked, 'certaintyScore, 'geometryLatitude, 'geometryLongitude))
                .joinWithSmaller('key -> 'userProfileId, numberOfFriends, joiner = new LeftJoin)
                .project(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends))
                /*  .filter(('lat, 'long)) {
                    fields: (String, String) =>

                        val (lat, lng) = fields
                        (lat.toDouble > 40.7 && lat.toDouble < 40.9 && lng.toDouble > -74 && lng.toDouble < -73.8) ||
                                (lat.toDouble > 40.489 && lat.toDouble < 40.924 && lng.toDouble > -74.327 && lng.toDouble < -73.723) ||
                                (lat.toDouble > 33.708 && lat.toDouble < 34.303 && lng.toDouble > -118.620 && lng.toDouble < -117.780) ||
                                (lat.toDouble > 37.596 && lat.toDouble < 37.815 && lng.toDouble > -122.514 && lng.toDouble < -122.362) ||
                                (lat.toDouble > 30.139 && lat.toDouble < 30.521 && lng.toDouble > -97.941 && lng.toDouble < -97.568) ||
                                (lat.toDouble > 41.656 && lat.toDouble < 42.028 && lng.toDouble > -87.858 && lng.toDouble < -87.491) ||
                                (lat.toDouble > 29.603 && lat.toDouble < 29.917 && lng.toDouble > -95.721 && lng.toDouble < -95.200) ||
                                (lat.toDouble > 33.647 && lat.toDouble < 33.908 && lng.toDouble > -84.573 && lng.toDouble < -84.250) ||
                                (lat.toDouble > 38.864 && lat.toDouble < 39.358 && lng.toDouble > -94.760 && lng.toDouble < -94.371) ||
                                (lat.toDouble > 30.130 && lat.toDouble < 30.587 && lng.toDouble > -82.053 && lng.toDouble < -81.384)
                }*/ .project(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'stemmedWorked, 'certaintyScore, 'numberOfFriends))

        filteredProfilesWithScore

    }

}
