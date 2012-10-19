package com.sonar.expedition.scrawler.util

import com.twitter.scalding.TupleConversions

object Tuples extends TupleConversions {
    val Place = ('serType, 'venId, 'venName, 'venAddress, 'lat, 'lng)
    val Profile = ('profileId, 'profile)
    val SonarFriend = ('sonarId, 'serviceType, 'serviceProfileId)
    val Checkin = ('serType, 'serProfileID, 'serCheckinID, 'venId, 'venName, 'venAddress, 'chknTime, 'lat, 'lng, 'msg)
    val Correlation = ('correlationId, 'serviceType, 'serviceProfileId)
}
