package com.sonar.expedition.scrawler.test

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{EmployerFinderFunction, CheckinGrouperFunction}

// pipes from grouper to employer finder and gets result

class EmpCheckFuncTest(args: Args) extends Job(args) {

    val serviceProfileInput = args("employerGroupedServiceProfiles")
    val checkinInput = args("userGroupedCheckins")
    val out = args("locationMatch")

    val groupFuncTest = new CheckinGrouperFunction(args)
    val empFuncTest = new EmployerFinderFunction(args)


    val pipeserv = TextLine(serviceProfileInput).read.project('line)
    val pipecheck = groupFuncTest.groupCheckins(TextLine(checkinInput).read.project('line))
    val pipeout = empFuncTest.findEmployees(pipeserv, pipecheck).write(TextLine(out))

}
