package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Args}
import com.sonar.expedition.scrawler.pipes.{PlacesCorrelation, DTOPlacesInfoPipe, CheckinGrouperFunction}
import cascading.tuple.Fields
import com.twitter.scalding.Job

class PlacesCorrelationTest(args: Args) extends Job(args) with CheckinGrouperFunction with PlacesCorrelation {

    val checkinData = args("newcheckinData")
    val oldcheckinData = args("oldcheckinData")
    val placesData = args("placesData")
    //    val output = args("output")

    //    val dtoPlacesInfoPipe = new DTOPlacesInfoPipe(args)


    val checkins = correlationCheckins(TextLine(checkinData).read)
    val oldcheckins = unfilteredCheckinsLatLon(TextLine(oldcheckinData).read)
    //    val places = dtoPlacesInfoPipe.getPlacesInfo(TextLine(placesData).read)
    val correlatedPlaces = withGoldenId(oldcheckins, checkins).project(Fields.ALL)
            .write(TextLine("/tmp/newPlacescorr.txt"))

}
