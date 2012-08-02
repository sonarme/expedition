package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, RichPipe, Job, Args}
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.pipes.{BayesModelPipe, GenderFromNameProbablity}

class JobTypeToRun(args: Args) extends Job(args) {

    val trainer = new BayesModelPipe(args)

    def jobTypeToRun(jobtypeToRun: String, filteredProfilesWithScore: RichPipe, bayestrainingmodel: String): RichPipe = {


        jobtypeToRun.toInt match {
            case 2 =>
                val jobtypes = filteredProfilesWithScore.project('worktitle).rename('worktitle -> 'data)

                val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
                    fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

                }
                val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('data, 'jobtypeclassified, 'weight1)) //.write(TextLine("/tmp")) //project('data, 'key, 'weight)

                filteredProfilesWithScore.joinWithSmaller('worktitle -> 'data1, trained)
                        .project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1))

            case 3 =>
                filteredProfilesWithScore.mapTo(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle) ->('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2)) {
                    fields: (String, String, String, String, String, String, String) =>
                        val (key, uname, fbid, lnid, stemmedWorked, city, worktitle) = fields
                        val (gender, probability) = GenderFromNameProbablity.gender(uname)
                        (key, uname, gender, probability, fbid, lnid, stemmedWorked, city, worktitle)
                }
                        .project(('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2))


            case 4 =>
                val jobtypes = filteredProfilesWithScore.project('worktitle).rename('worktitle -> 'data)

                val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
                    fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

                }
                val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('data, 'jobtypeclassified, 'weight1)) //.write(TextLine("/tmp")) //project('data, 'key, 'weight)

                filteredProfilesWithScore.joinWithSmaller('worktitle -> 'data1, trained)
                        .project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1))
                        .mapTo(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1) ->('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2, 'data2, 'jobtypeclassified2, 'weight2)) {
                    fields: (String, String, String, String, String, String, String, String, String, String) =>
                        val (key, uname, fbid, lnid, stemmedWorked, city, worktitle, data1, key1, weight1) = fields
                        val (gender, probability) = GenderFromNameProbablity.gender(uname)
                        (key, uname, gender, probability, fbid, lnid, stemmedWorked, city, worktitle, data1, key1, weight1)
                }
                        .project(('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2, 'data2, 'jobtypeclassified2, 'weight2))


            case _ => filteredProfilesWithScore

        }

    }

}
