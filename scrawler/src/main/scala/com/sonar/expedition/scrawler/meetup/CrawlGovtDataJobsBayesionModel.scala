package com.sonar.expedition.scrawler.meetup

import com.twitter.scalding.{SequenceFile, TextLine, Args}
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.sonar.expedition.scrawler.pipes.BayesModelPipe
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.jobs.Job

class CrawlGovtDataJobsBayesionModel(args: Args) extends Job(args) with BayesModelPipe {

    val trainingmodel = args.getOrElse("bayestrainingmodelforsalary", "/tmp/bayestrainingmodelforsalary")
    val jobsdata = "/tmp/datajobs"
    val jobsPipe = (TextLine(jobsdata).read)
            .project('line, 'offset)
            .filter('line) {
        line: String => (line.trim != "" || line.contains("about"))
    }
            .mapTo(('line, 'offset) ->('key, 'token, 'doc)) {
        fields: (String, Int) =>
            val (line, num) = fields
            val splittext = line.trim.split("\\t")
            if (splittext.isDefinedAt(1)) {
                (splittext(0), splittext(1), num)
            } else {
                ("", "", -1)
            }
    }.filter('doc) {
        line: Int => (line.!=(-1))
    }.project(('key, 'token, 'doc))

    val model = trainBayesModel(jobsPipe);
    model.write(SequenceFile(trainingmodel, Fields.ALL))

}
