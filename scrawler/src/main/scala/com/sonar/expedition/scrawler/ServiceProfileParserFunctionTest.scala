package com.sonar.expedition.scrawler

import com.twitter.scalding.{Job, Args, TextLine}

class ServiceProfileParserFunctionTest(args: Args) extends Job(args) {

    val serviceProfileInput = "/tmp/serviceProfileData.txt"
    val parsedProfileData = "/tmp/parsedProfileData.txt"


    val serviceProfileFunctionTest = new ServiceProfileParserFunction(args)

    val serviceProfilePipe = TextLine(serviceProfileInput).read.project('line)
    val parseServiceProfilesPipe = serviceProfileFunctionTest.parseServiceProfiles(serviceProfilePipe).project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle).write(TextLine(parsedProfileData))

}
