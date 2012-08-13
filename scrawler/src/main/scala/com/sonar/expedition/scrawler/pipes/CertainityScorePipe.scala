package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Job, Args, RichPipe}
import cascading.pipe.joiner.LeftJoin
import com.sonar.expedition.scrawler.util.LocationScorer
import java.security.MessageDigest

class CertainityScorePipe(args: Args) extends Job(args) {


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
                val score = LocationScorer.getScore(work, workLatitude, workLongitude, place, placeLatitude, placeLongitude)
                val certainty = LocationScorer.certaintyScore(score, work, place)
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

        filteredProfilesWithScore

    }

}
