package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, Args}
import com.sonar.expedition.scrawler.pipes.DTOPlacesInfoPipe
import com.sonar.expedition.scrawler.jobs.Job

class PlacesInfoTest(args: Args) extends Job(args) with DTOPlacesInfoPipe {

    val placesData = args("placesData")
    val jobOutput = args("output")

    val pipe1 = TextLine(placesData).read.project('line)
    val pipe2 = getPlacesInfo(pipe1).write(TextLine(jobOutput))

}
