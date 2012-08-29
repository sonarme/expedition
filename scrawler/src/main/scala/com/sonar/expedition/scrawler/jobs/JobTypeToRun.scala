package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, RichPipe, Args, SequenceFile}
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.pipes.{JobImplicits, BayesModelPipe, GenderFromNameProbability}


trait JobTypeToRun extends BayesModelPipe {

    def jobTypeToRun(jobtypeToRun: String, filteredProfilesWithScore: RichPipe, seqModel: RichPipe, trainedseqmodel: String): RichPipe = {

        jobtypeToRun.toInt match {
            case 2 =>
                val jobtypes = filteredProfilesWithScore.project('worktitle).rename('worktitle -> 'data)

                val trained = calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('data, 'jobtypeclassified, 'weight1)).write(TextLine(trainedseqmodel)) //project('data, 'key, 'weight)

                filteredProfilesWithScore.joinWithSmaller('worktitle -> 'data, trained)
                        .project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1))

            case 3 =>
                filteredProfilesWithScore.mapTo(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle) ->('key, 'uname, 'gender, 'genderprob, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle)) {
                    fields: (String, String, String, String, String, String, String) =>
                        val (key, uname, fbid, lnid, stemmedWorked, city, worktitle) = fields
                        val (gender, probability) = GenderFromNameProbability.gender(uname)
                        (key, uname, gender, probability, fbid, lnid, stemmedWorked, city, worktitle)
                }
                        .project(('key, 'uname, 'gender, 'genderprob, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle))


            case 4 =>
                val jobtypes = filteredProfilesWithScore.project('worktitle).rename('worktitle -> 'data)



                val trained = calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('data, 'key, 'weight) ->('data, 'jobtypeclassified, 'weight1)).write(TextLine(trainedseqmodel)) //project('data, 'key, 'weight)

                filteredProfilesWithScore.joinWithSmaller('worktitle -> 'data, trained)
                        .project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1))
                        .mapTo(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight1) ->('key, 'uname, 'gender, 'genderprob, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight)) {
                    fields: (String, String, String, String, String, String, String, String, String, String) =>
                        val (key, uname, fbid, lnid, stemmedWorked, city, worktitle, data, key1, weight1) = fields
                        val (gender, probability) = GenderFromNameProbability.gender(uname)
                        (key, uname, gender, probability, fbid, lnid, stemmedWorked, city, worktitle, data, key1, weight1)
                }
                        .project(('key, 'uname, 'gender, 'genderprob, 'fbid, 'lnid, 'stemmedWorked, 'city, 'worktitle, 'data, 'jobtypeclassified, 'weight))


            case _ => filteredProfilesWithScore

        }

    }

}
