package com.sonar.expedition.scrawler.jobs
import com.twitter.scalding.{SequenceFile, TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.{BayesModelPipe, JobCodeReader}
import util.matching.Regex
import TrainBayesModel._
import cascading.tuple.Fields._
import cascading.tuple.Fields


/*
run as
com.sonar.expedition.scrawler.jobs.TrainBayesModel --hdfs --jobtraininginput "/tmp/occupationCodetsv.txt" --bayestrainingmodel "/tmp/bayestrainingmodel"
 */
class TrainBayesModel (args: Args) extends Job(args) {
    val input         = args("jobtraininginput")
    val trainingmodel  = args("bayestrainingmodel")

    val reading = TextLine(input).read
            .map('line ->('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctitle)) {
            line: String => {
            line match {
                case Occupation(matrixocccode, matrixocctitle, cpscode, cpsocctitle) => (matrixocccode, matrixocctitle, cpscode, cpsocctitle)
                case _ => ("None", "None", "None", "None")
            }
        }

    }.rename('offset -> 'doc).rename('cpsocctitle -> 'key).flatMap('matrixocctitle -> 'token){
        title: String => {
            title.split("\\s+")
        }
    }.project(('key, 'token, 'doc))

    val trainer = new BayesModelPipe(args)

    val model =  trainer.trainBayesModel(reading);

    model.write(SequenceFile(trainingmodel, Fields.ALL))


}

object  TrainBayesModel{
    val Occupation: Regex = """([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)\t([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)""".r

}
