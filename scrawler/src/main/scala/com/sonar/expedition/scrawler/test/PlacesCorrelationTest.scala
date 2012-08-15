package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, DTOPlacesInfoPipe, CheckinGrouperFunction}
import cascading.tuple.Fields

class PlacesCorrelationTest(args: Args) extends Job(args) {

    val checkinData = args("newcheckinData")
    val oldcheckinData = args("oldcheckinData")
    val placesData = args("placesData")
//    val output = args("output")

//    val dtoPlacesInfoPipe = new DTOPlacesInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val placesCorrelationPipe = new PlacesCorrelation(args)

    val checkins = checkinGrouperPipe.correlationCheckins(TextLine(checkinData).read)
    val oldcheckins = checkinGrouperPipe.unfilteredCheckinsLatLon(TextLine(oldcheckinData).read)
//    val places = dtoPlacesInfoPipe.getPlacesInfo(TextLine(placesData).read)
    val correlatedPlaces = placesCorrelationPipe.withGoldenId(oldcheckins, checkins).project(Fields.ALL)
            .write(TextLine("/tmp/newPlacescorr.txt"))

}