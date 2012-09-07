package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe

class ConvertJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
    val input = args("input")
    val output = args("output")
    SequenceFile(input, ProfileTuple).mapTo(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
            ->('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)) {
        in: (String, String, String, String, String, String, String, String, String, String, String, String, String) =>
            (Option(in._1).getOrElse(""),
                    Option(in._2).getOrElse(""),
                    Option(in._3).getOrElse(""),
                    Option(in._4).getOrElse(""),
                    Option(in._5).getOrElse(""),
                    Option(in._6).getOrElse(""),
                    Option(in._7).getOrElse(""),
                    Option(in._8).getOrElse(""),
                    Option(in._9).getOrElse(""),
                    Option(in._10).getOrElse(""),
                    Option(in._11).getOrElse(""),
                    Option(in._12).getOrElse(""), Option(in._13).getOrElse(""))
    }.write(SequenceFile(output, ProfileTuple))
}
