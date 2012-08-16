package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import com.twitter.scalding.TextLine
import cascading.tuple.Fields

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTapIncome(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")

    val input = args("serviceProfileInput")
    val twinput = args("twitterServiceProfileInput")
    val checkininput = args("checkinInput")
    val friendinput = args("friendInput")
    val bayestrainingmodel = args("bayestrainingmodelforsalary")
    val newcheckininput = args("newCheckinInput")

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

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val ageEducationPipe = new AgeEducationPipe(args)
    val checkinGroup = new CheckinGrouperFunction(args)
    val friendGroup = new FriendGrouperFunction(args)
    val businessGroup = new BusinessGrouperFunction(args)
    val ageEducation = new AgeEducationPipe(args)
    val reachLoyalty = new ReachLoyaltyAnalysis(args)
    val coworkerPipe = new CoworkerFinderFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)
    val placesCorrelation = new PlacesCorrelation(args)





    val checkins = checkinGroup.unfilteredCheckinsLatLon(TextLine(checkininput))
    val newcheckins = checkinGroup.correlationCheckins(TextLine(newcheckininput))
    val checkinsWithGolden = placesCorrelation.withGoldenId(checkins, newcheckins)
            .map(('lat, 'lng) -> ('loc)) {
        fields: (String, String) =>
            val (lat, lng) = fields
            val loc = lat + ":" + lng
            (loc)
    }

    val total = dtoProfileGetPipe.getTotalProfileTuples(data, twdata).map('uname ->('impliedGender, 'impliedGenderProb)) {
        name: String => GenderFromNameProbability.gender(name)
    }

    val profiles = ageEducation.ageEducationPipe(total)
            .project(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree))



    val joinedProfiles = profiles.rename('key->'rowkey)
    val trainer = new BayesModelPipe(args)

    val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
        fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

    }

    val jobtypes = joinedProfiles.rename('worktitle -> 'data)
            .filter('data) {
        data: String => !isNullOrEmpty(data)
    }
    val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('key, 'weight) ->('income, 'weight1))
    val profilesWithIncome = joinedProfiles.joinWithSmaller('worktitle -> 'data, trained).project(('rowkey, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree, 'income))
            .rename('rowkey -> 'key)



    val combined = businessGroup.combineCheckinsProfiles(checkinsWithGolden, profilesWithIncome)


    val byIncome = businessGroup.byIncome(combined)
            .map(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, income, frequency) = in

            val targetVenueGoldenId = venueKey + "_income"
            val column = income
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val totalIncome = byIncome.groupBy('columnName) {
        _.sum('columnValue)
    }
            .map('columnName -> 'rowKey) {
        columnName: String => "totalAll_income"
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val staticOutput =
        (byIncome ++ totalIncome)
                .write(
            CassandraSource(
                rpcHost = rpcHostArg,
                privatePublicIpMap = ppmap,
                keyspaceName = "dossier",
                columnFamilyName = "MetricsVenueStatic",
                scheme = WideRowScheme(keyField = 'rowKey)
            )
        )

}
