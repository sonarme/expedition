package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Args, Job}
import com.sonar.expedition.scrawler.pipes.DTOPlacesInfoPipe

class PlacesInfoTest(args: Args) extends Job(args) {

    val placesData = args("placesData")
    val jobOutput = args("output")

    val placesTest = new DTOPlacesInfoPipe(args)

    val pipe1 = TextLine(in).read.project('line)
    val pipe2 = placesTest.getPlacesInfo(pipe1).write(TextLine(out))

}
