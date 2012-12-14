package com.sonar.expedition.scrawler.util

import com.twitter.scalding.{FieldConversions, TupleConversions}

object Tuples extends TupleConversions {
    val Place = ('serType, 'venId, 'venName, 'venAddress, 'lat, 'lng)
    val Profile = ('profileId, 'profile)
    val SonarFriend = ('sonarId, 'serviceType, 'serviceProfileId)
    val CheckinIdDTO = ('checkinId, 'checkinDto)
    val VenueIdDTO = ('venueId, 'venueDto)
    val Checkin = ('serType, 'serProfileID, 'serCheckinID, 'venId, 'venName, 'venAddress, 'chknTime, 'lat, 'lng, 'msg)
    val Correlation = ('correlationId, 'serviceType, 'serviceProfileId)

    object Crawler extends FieldConversions {
        val Links = ('url, 'timestamp, 'referer)
        val Status = ('url, 'status, 'timestamp, 'attempts, 'crawlDepth)
        val Raw = ('url, 'timestamp, 'status, 'content, 'links)
        val BaseVenue = ('url, 'timestamp, 'businessName, 'category, 'rating, 'latitude, 'longitude, 'address, 'city, 'state, 'zip, 'phone)
        val LivingSocial = BaseVenue append('priceRange, 'reviewCount, 'likes, 'dealRegion, 'dealPrice, 'purchased, 'savingsPercent, 'dealDescription, 'dealImage)
        val Foursquare = BaseVenue append('priceRange, 'reviewCount, 'reviews)
        val Facebook = BaseVenue append('priceRange, 'reviewCount, 'likes, 'peopleCount, 'checkins, 'wereHereCount, 'talkingAboutCount)
        val Twitter = BaseVenue
        val Yelp = BaseVenue append('priceRange, 'reviewCount, 'reviews)
        val CitySearch = BaseVenue append('priceRange, 'reviewCount)
    }

}
