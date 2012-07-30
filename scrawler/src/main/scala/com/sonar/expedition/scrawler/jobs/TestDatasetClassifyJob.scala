package com.sonar.expedition.scrawler.jobs

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.scalding.{Tsv, TextLine, Job, Args}

//convert data to inout format required for tarining tha bayes model
//com.sonar.expedition.scrawler.jobs.TestDatasetClassifyJob --local
//then copy the file to occupations
//cp /tmp/jobsdata5 /tmp/occupationCodetsv.txt and run the training model TrainBayesModel

class TestDatasetClassifyJob(args: Args) extends Job(args) {

    val jobpipe = TextLine("/tmp/jobsdata1").read.project('line)
            .mapTo('line -> 'link) {
        fields: String =>
            val (link) = fields

            parseJobs(link)
    }.mapTo('link ->('code, 'job, 'desc)) {
        fields: String =>
            val (line) = fields

            //println(line)
            if (line.contains("</h2>")) {
                val code = line.substring(0, line.indexOf(" "))
                val jobs = line.substring(line.indexOf(" ")).split("</h2><p>")
                //println(code)
                (code, jobs(0), jobs)
            } else {
                (None, None, None)
            }


    }.project('code, 'job, 'desc).filter('code, 'job, 'desc) {
        fields: (String, String, String) =>
            val (code, job, desc) = fields
            (code != "None")

    }.mapTo(('code, 'job, 'desc) ->('code1, 'desc1, 'code2, 'job1)) {
        fields: (String, String, String) =>
            val (code, job, desc) = fields
            (code, "\t" + desc.replaceAll("[^a-zA-Z\\s]", " "), "\t" + code, "\t" + job.replaceAll("[^a-zA-Z\\s]", " "))

    }.write(TextLine("/tmp/jobsdata5"))

    def parseJobs(line: String): String = {

        line.substring(line.indexOf("<h2>") + 4)

    }
}