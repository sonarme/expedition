package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Job, Args}
import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.JobCodeReader._
import mathematics.Field
import scala.Predef._
import scala.math._
import com.twitter.scalding.TextLine
import cascading.tuple.Fields

class ReadMahoutModel (args: Args) extends Job(args) {

    //val pipe = SequenceFile("/tmp/occupationCodetsvtest2.txt").read.project(Fields.ALL).write(TextLine("/tmp/occupationCodetsvtest8.txt"))



            val pipe = SequenceFile("/tmp/occupationCodetsvtest2.txt").read.project(Fields.ALL).mapTo((0,1,2)->('job,'term,'tfidf)){
                fields: (String,String,String) =>
                val (job,term,tfidf)= fields

                val terms = listify(job)
                (terms(1),term(2),term(3))
            }.write(TextLine("/tmp/occupationCodetsvtest9.txt"))


    def listify(line:String): List[String]={
        line.split("\\t").toList
    }
}
