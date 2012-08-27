package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{TextLine, RichPipe, Args}
import com.sonar.expedition.scrawler.pipes.EmployerFinderFunction
import com.sonar.expedition.scrawler.jobs.Job

// reads in checkingrouped and employergrouped to output matches
class EmployerFinderFunctionTest(args: Args) extends Job(args) with EmployerFinderFunction {

    val serviceProfileInput = args("employerGroupedServiceProfiles")
    val checkinInput = args("userGroupedCheckins")
    val out = args("locationMatch")


    val pipe1 = TextLine(serviceProfileInput).read.project('line)
    val pipe2 = TextLine(checkinInput).read.project('line)
    val pipeout = findEmployeesFromText(pipe1, pipe2).write(TextLine(out))

}
