package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.pipes.{BayesModelPipe, GenderFromNameProbability}
import com.twitter.scalding.SequenceFile

class JobTypeToRun(args: Args) extends Job(args) {

    val trainer = new BayesModelPipe(args)

    def jobTypeToRun(jobtypeToRun: String, filteredProfilesWithScore: RichPipe, seqModel: RichPipe, trainedseqmodel: String): RichPipe = {

        jobtypeToRun.toInt match {
            case 2 =>
                val jobtypes = filteredProfilesWithScore.project('worktitle).rename('worktitle -> 'data)

                val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('data, 'jobtypeclassified, 'weight1)).write(TextLine(trainedseqmodel)) //project('data, 'key, 'weight)

                filteredProfilesWithScore.joinWithSmaller('worktitle -> 'data, trained)
                        .project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1))

            case 3 =>
                filteredProfilesWithScore.mapTo(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle) ->('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2)) {
                    fields: (String, String, String, String, String, String, String) =>
                        val (key, uname, fbid, lnid, stemmedWorked, city, worktitle) = fields
                        val (gender, probability) = GenderFromNameProbability.gender(uname)
                        (key, uname, gender, probability, fbid, lnid, stemmedWorked, city, worktitle)
                }
                        .project(('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2))


            case 4 =>
                val jobtypes = filteredProfilesWithScore.project('worktitle).rename('worktitle -> 'data)



                val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('data, 'jobtypeclassified, 'weight1)).write(TextLine(trainedseqmodel)) //project('data, 'key, 'weight)

                filteredProfilesWithScore.joinWithSmaller('worktitle -> 'data, trained)
                        .project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1))
                        .mapTo(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1) ->('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2, 'data2, 'jobtypeclassified2, 'weight2)) {
                    fields: (String, String, String, String, String, String, String, String, String, String) =>
                        val (key, uname, fbid, lnid, stemmedWorked, city, worktitle, data, key1, weight1) = fields
                        val (gender, probability) = GenderFromNameProbability.gender(uname)
                        (key, uname, gender, probability, fbid, lnid, stemmedWorked, city, worktitle, data, key1, weight1)
                }
                        .project(('key1, 'uname2, 'gender, 'genderprob, 'fbid2, 'lnid2, 'stemmedWorked2, 'city2, 'worktitle2, 'data2, 'jobtypeclassified2, 'weight2))


            case _ => filteredProfilesWithScore

        }

    }

}
