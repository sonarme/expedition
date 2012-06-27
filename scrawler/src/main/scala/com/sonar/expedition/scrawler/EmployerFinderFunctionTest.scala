package com.sonar.expedition.scrawler

import com.twitter.scalding._

// reads in checkingrouped and employergrouped to output matches
class EmployerFinderFunctionTest(args: Args) extends Job(args) {

    val serviceProfileInput = "/tmp/employerGroupedServiceProfiles.txt"
    val checkinInput = "/tmp/userGroupedCheckins.txt"
    val out = "/tmp/locationMatch.txt"

    val empFuncTest = new EmployerFinderFunction(args)


    val pipe1 = TextLine(serviceProfileInput).read.project('line)
    val pipe2 = TextLine(checkinInput).read.project('line)
    val pipeout = empFuncTest.findEmployees(pipe1, pipe2).write(TextLine(out))

}
