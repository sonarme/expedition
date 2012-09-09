package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile


class ExtractIncome(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {

    val bayesmodel = args("bayesmodelforsalary")

    val seqModel = SequenceFile(bayesmodel, ('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)).read

    val jobtypes = serviceProfiles(args).unique('worktitle).rename('worktitle -> 'data)
            .filter('data) {
        data: String => !isNullOrEmpty(data)
    }
    calcProb(seqModel, jobtypes)
            .unique('data, 'key, 'weight)
            .rename(('data, 'key) ->('worktitle, 'income))
            .write(SequenceFile(args("output"), ('worktitle, 'income, 'weight)))

}
