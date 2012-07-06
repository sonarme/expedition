package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Args, Job}
import com.sonar.expedition.scrawler.pipes.DTOPlacesInfoPipe

class PlacesInfoTest(args: Args) extends Job(args) {

    var in = "/tmp/placesData.txt"
    var out = "/tmp/parsedPlaces.txt"
    val placesTest = new DTOPlacesInfoPipe(args)

    val pipe1 = TextLine(in).read.project('line)
    val pipe2 = placesTest.getPlacesInfo(pipe1).write(TextLine(out))

}
