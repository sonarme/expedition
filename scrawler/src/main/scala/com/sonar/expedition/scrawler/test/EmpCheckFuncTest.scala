package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.pipes.{EmployerFinderFunction, CheckinGrouperFunction}
import com.twitter.scalding.Job

// pipes from grouper to employer finder and gets result

class EmpCheckFuncTest(args: Args) extends Job(args) with CheckinGrouperFunction with EmployerFinderFunction {

    val serviceProfileInput = args("employerGroupedServiceProfiles")
    val checkinInput = args("userGroupedCheckins")
    val out = args("locationMatch")


    val pipeserv = TextLine(serviceProfileInput).read.project('line)
    val pipecheck = workCheckins(TextLine(checkinInput).read.project('line))
    val pipeout = findEmployees(pipeserv, pipecheck).write(TextLine(out))

}
