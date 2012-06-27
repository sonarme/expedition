package com.sonar.expedition.scrawler

import com.twitter.scalding._

// pipes from grouper to employer finder and gets result

class EmpCheckFuncTest(args: Args) extends Job(args) {

    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    var checkinInput = "/tmp/tcheckinData.txt"
    val out = "/tmp/locationMatchSmall.txt"

    val groupFuncTest = new CheckinGrouperFunction(args)
    val empFuncTest = new EmployerFinderFunction(args)



    val pipeserv = TextLine(serviceProfileInput).read.project('line)
    val pipecheck = groupFuncTest.groupCheckins(TextLine(checkinInput).read.project('line))
    val pipeout = empFuncTest.findEmployeesFromPipe(pipeserv, pipecheck).write(TextLine(out))

}
