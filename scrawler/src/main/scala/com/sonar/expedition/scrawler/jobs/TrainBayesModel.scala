package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{BayesModelPipe, JobCodeReader}
import util.matching.Regex
import TrainBayesModel._
import cascading.tuple.Fields._
import cascading.tuple.Fields
import scala.Some
import com.twitter.scalding.SequenceFile
import scala.Some
import com.twitter.scalding.TextLine


/*
run as
com.sonar.expedition.scrawler.jobs.TrainBayesModel --hdfs --jobtraininginput "/tmp/occupationCodetsv.txt" --bayesmodel "/tmp/bayesmodel"
and run TrainBayesModel
 after running bayes run DataAnalyser for the actual classification.

add contents to the file
 */
class TrainBayesModel(args: Args) extends DefaultJob(args) with BayesModelPipe {

    val input = args("jobtraininginput")
    val trainingmodel = args("bayesmodel")

    val reading = TextLine(input).read.project(('offset, 'line))
            .flatMapTo(('line, 'offset) ->('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctitle, 'offset1)) {
        fields: (String, String) =>
            val (line, offset) = fields

            //println(line)
            line match {
                case Occupation(matrixocccode, matrixocctitle, cpscode, cpsocctitle) => Some(matrixocccode, matrixocctitle, cpscode, cpsocctitle, offset)
                case _ => None
            }


    }.project(('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctitle, 'offset1))
            .rename('offset1 -> 'doc)
            .rename('cpsocctitle -> 'key)
            .flatMap('matrixocctitle -> 'token) {
        fields: (String) =>
            val (desc) = fields
            //println(desc)
            desc.split("\\s+")

    }
            .mapTo(('key, 'token, 'doc) ->('key1, 'token1, 'doc1)) {
        fields: (String, String, String) =>
            val (key, token, doc) = fields
            //println(key + token + doc)
            (key.trim, token.trim, doc.trim)
    }
            .project(('key1, 'token1, 'doc1)).rename(('key1, 'token1, 'doc1) ->('key, 'token, 'doc)).project(('key, 'token, 'doc))

    val model = trainBayesModel(reading)
    model.write(SequenceFile(trainingmodel, Fields.ALL))


}

object TrainBayesModel {
    val Occupation: Regex = """([a-zA-Z\d\-\s]+)\t([a-zA-Z\d\- ,\s]+)\t([a-zA-Z\d\-\s]+)\t([a-zA-Z\d\-\s]+)""".r

}
