package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTapIncome(args: Args) extends Job(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {

    val input = args("serviceProfileInput")
    val twinput = args("twitterServiceProfileInput")
    val friendinput = args("friendInput")
    val bayesmodel = args("bayesmodelforsalary")
    val sequenceOutputIncome = args("sequenceOutputIncome")
    val textOutputIncome = args("textOutputIncome")

    val data = (TextLine(input).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val twdata = (TextLine(twinput).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ServiceProfileExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))


    val (checkins, checkinsWithGoldenId) = checkinSource(args, false, true)
    val checkinsWithGolden = checkinsWithGoldenId
            .map(('lat, 'lng) -> ('loc)) {
        fields: (String, String) =>
            val (lat, lng) = fields
            val loc = lat + ":" + lng
            (loc)
    }

    val total = getTotalProfileTuples(data, twdata).map('uname ->('impliedGender, 'impliedGenderProb)) {
        name: String => GenderFromNameProbability.gender(name)
    }

    val profiles = ageEducationPipe(total)
            .project(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree))


    val joinedProfiles = profiles.rename('key -> 'rowkey)

    val seqModel = SequenceFile(bayesmodel, ('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)).read

    val jobtypes = joinedProfiles.rename('worktitle -> 'data)
            .filter('data) {
        data: String => !isNullOrEmpty(data)
    }
    val trained = calcProb(seqModel, jobtypes)
            .project(('data, 'key, 'weight))
            .rename(('key, 'weight) ->('income, 'weight1))

    val profilesWithIncome = joinedProfiles
            .joinWithSmaller('worktitle -> 'data, trained)
            .project(('rowkey, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree, 'income))
            .discard('rowkey)
            .flatMap(('fbid, 'lnid, 'fsid, 'twid) -> 'key) {
        in: (String, String, String, String) =>
            val (fbid, lnid, fsid, twid) = in
            //nned not handle linked in because there ar no checkins from linked in and sonar checkins dont have id , so key comes as sonar: empty, need to fix it, ask Paul, todo.
            List("facebook:" + fbid, "twitter:" + twid, "foursquare:" + fsid)
    }


    val combined = combineCheckinsProfiles(checkinsWithGolden, profilesWithIncome)


    val byIncome = groupByIncome(combined)
            .mapTo(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, income, frequency) = in

            val targetVenueGoldenId = venueKey + "_income"
            val column = income
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)
    }

    val totalIncome = byIncome
            .groupBy('columnName) {
        _.sum('columnValue)
    }.map('columnName -> 'rowKey) {
        columnName: String => "totalAll_income"
    }.project(('rowKey, 'columnName, 'columnValue))

    val staticOutput = byIncome ++ totalIncome

    staticOutput
            .write(SequenceFile(sequenceOutputIncome, Fields.ALL))
            .write(TextLine(textOutputIncome))


}
