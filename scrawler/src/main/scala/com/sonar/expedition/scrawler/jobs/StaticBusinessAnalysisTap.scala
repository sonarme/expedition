package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{StringSerializer, DoubleSerializer}
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.TextLine

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTap(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")

    val input = args("serviceProfileInput")
    val twinput = args("twitterServiceProfileInput")
    val checkininput = args("checkinInput")

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
    val businessGroup = new BusinessGrouperFunction(args)
    val ageEducation = new AgeEducationPipe(args)

    val checkins = checkinGroup.unfilteredCheckins(TextLine(checkininput))

    val total = dtoProfileGetPipe.getTotalProfileTuples(data, twdata).map('uname -> ('impliedGender, 'impliedGenderProb)){
        name: String => GenderFromNameProbability.gender(name)
    }

    val profiles = ageEducation.ageEducationPipe(total)
            .project(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree))

    val combined = businessGroup.combineCheckinsProfiles(checkins, profiles)


//    val byAge = businessGroup.byAge(combined)
//            .map(('venueKey, 'ageBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
//        in: (String, String, Int) =>
//            val (venueKey, ageBracket, frequency) = in
//
//            val targetVenueGoldenId = venueKey + "_age"
//            val column = ageBracket
//            val value = frequency.toDouble
//
//            (targetVenueGoldenId, column, value)
//
//    }
//            .project(('rowKey, 'columnName, 'columnValue))
//
//
//    val byGender = businessGroup.byGender(combined)
//            .map(('venueKey, 'impliedGender, 'size) ->('rowKey, 'columnName, 'columnValue)) {
//        in: (String, String, Int) =>
//            val (venueKey, impliedGender, frequency) = in
//
//            val targetVenueGoldenId = venueKey + "_gender"
//            val column = impliedGender
//            val value = frequency.toDouble
//
//            (targetVenueGoldenId, column, value)
//
//    }
//            .project(('rowKey, 'columnName, 'columnValue))
//
//    val byDegree = businessGroup.byDegree(combined)
//            .map(('venueKey, 'degreeCat, 'size) ->('rowKey, 'columnName, 'columnValue)) {
//        in: (String, String, Int) =>
//            val (venueKey, degreeCat, frequency) = in
//
//            val targetVenueGoldenId = venueKey + "_education"
//            val column = degreeCat
//            val value = frequency.toDouble
//
//            (targetVenueGoldenId, column, value)
//
//    }
//            .project(('rowKey, 'columnName, 'columnValue))
//
//    val byIncome = businessGroup.byIncome(combined)
//            .map(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
//        in: (String, String, Int) =>
//            val (venueKey, income, frequency) = in
//
//            val targetVenueGoldenId = venueKey + "_income"
//            val column = income
//            val value = frequency.toDouble
//
//            (targetVenueGoldenId, column, value)
//    }
//            .project(('rowKey, 'columnName, 'columnValue))
//
//    val staticOutput = byAge.++(byDegree).++(byGender)
//            .write(
//        CassandraSource(
//            rpcHost = rpcHostArg,
//            privatePublicIpMap = ppmap,
//            keyspaceName = "dossier",
//            columnFamilyName = "MetricsVenueStatic",
//            scheme = WideRowScheme(keyField = 'rowKey)
//        )
//    )

    val byTime = businessGroup.timeSeries(combined)
            .map(('venueKey, 'hourChunk, 'serType, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Int, String, Int) =>
            val (venueKey, hour, serviceType, frequency) = in

            val targetVenueGoldenId = venueKey + "_checkinFrequencyPerHour_" + serviceType
            val column = hour.toLong * 3600000
            val value = frequency * 1.0

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))


    val timeSeriesOutput = byTime
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueTimeseries",
            scheme = WideRowScheme(keyField = 'rowKey)
        )
    )
}
