package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, DTOPlacesInfoPipe, CheckinGrouperFunction}

class PlacesCorrelationTest(args: Args) extends Job(args) {

    val checkinData = args("checkinData")
    val placesData = args("placesData")

    val dtoPlacesInfoPipe = new DTOPlacesInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val placesCorrelationPipe = new PlacesCorrelation(args)

    val checkins = checkinGrouperPipe.correlationCheckins(TextLine(checkinData).read)
    val places = dtoPlacesInfoPipe.getPlacesInfo(TextLine(placesData).read)
    val correlatedPlaces = placesCorrelationPipe.correlatedPlaces(checkins, places).project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'stemmedVenName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId)
    .write(TextLine("/tmp/placescorrelationfunction.txt"))

}
