package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import com.sonar.dossier.dto._
import com.sonar.dossier.dao.cassandra.JSONSerializer
import com.sonar.scalding.cassandra._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import me.prettyprint.cassandra.serializers.{DateSerializer, LongSerializer, StringSerializer, DoubleSerializer}
import com.twitter.scalding.TextLine
import cascading.tuple.Fields
import java.nio.ByteBuffer
import com.sonar.expedition.scrawler.util.CommonFunctions._

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTap(args: Args) extends Job(args) {


    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")

    val input = args("serviceProfileInput")
    val twinput = args("twitterServiceProfileInput")
    val friendinput = args("friendInput")
    val bayestrainingmodel = args("bayestrainingmodelforsalary")
    val sequenceOutputStatic = args("sequenceOutputStatic")
    val sequenceOutputTime = args("sequenceOutputTime")
    val textOutputStatic = args("textOutputStatic")
    val textOutputTime = args("textOutputTime")
    val cassandraKeyspaceName = args.getOrElse("keyspace", "dossier")


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


    val checkins = CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = cassandraKeyspaceName,
        columnFamilyName = "Checkin",
        scheme = NarrowRowScheme(keyField = 'serviceCheckinIdBuffer,
            nameFields = ('userProfileIdBuffer, 'serTypeBuffer, 'serProfileIDBuffer, 'serCheckinIDBuffer,
                    'venNameBuffer, 'venAddressBuffer, 'venIdBuffer, 'chknTimeBuffer,
                    'ghashBuffer, 'latBuffer, 'lngBuffer, 'msgBuffer),
            columnNames = List("userProfileId", "serviceType", "serviceProfileId",
                "serviceCheckinId", "venueName", "venueAddress",
                "venueId", "checkinTime", "geohash", "latitude",
                "longitude", "message"))
    ).map(('serviceCheckinIdBuffer, 'userProfileIdBuffer, 'serTypeBuffer, 'serProfileIDBuffer, 'serCheckinIDBuffer,
            'venNameBuffer, 'venAddressBuffer, 'venIdBuffer, 'chknTimeBuffer,
            'ghashBuffer, 'latBuffer, 'lngBuffer, 'msgBuffer) ->('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID,
            'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer) => {
            val rowKeyDes = StringSerializer.get().fromByteBuffer(in._1)
            val keyId = Option(in._2).map(StringSerializer.get().fromByteBuffer).getOrElse("missingKeyId")
            val serType = Option(in._3).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val serProfileID = Option(in._4).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val serCheckinID = Option(in._5).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val venName = Option(in._6).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val venAddress = Option(in._7).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val venId = Option(in._8).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val chknTime = Option(in._9).map(DateSerializer.get().fromByteBuffer).getOrElse(DEFAULT_NO_DATE)
            val ghash = Option(in._10).map(LongSerializer.get().fromByteBuffer).orNull
            val lat: Double = Option(in._11).map(DoubleSerializer.get().fromByteBuffer).orNull
            val lng: Double = Option(in._12).map(DoubleSerializer.get().fromByteBuffer).orNull
            val msg = Option(in._13).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)

            (rowKeyDes, keyId, serType, serProfileID, serCheckinID,
                    venName, venAddress, venId, chknTime, ghash, lat, lng, msg)
        }
    }


    val newCheckins = checkinGroup.correlationCheckinsFromCassandra(checkins)
    val checkinsWithGolden = placesCorrelation.withGoldenId(newCheckins)
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


    /*val joinedProfiles = profiles.rename('key->'rowkey)
val trainer = new BayesModelPipe(args)

val seqModel = SequenceFile(bayestrainingmodel, Fields.ALL).read.mapTo((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ->('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)) {
fields: (String, String, Int, Int, Int, Double, Double, Double, Double, Double, Double) => fields

}


val jobtypes = joinedProfiles.rename('worktitle -> 'data)

val trained = trainer.calcProb(seqModel, jobtypes).project(('data, 'key, 'weight)).rename(('key, 'weight) ->('income, 'weight1))

val profilesWithIncome = joinedProfiles.joinWithSmaller('worktitle -> 'data, trained).project(('rowkey, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree, 'income))
 .rename('rowkey -> 'key)
*/

    val combined = businessGroup.combineCheckinsProfiles(checkinsWithGolden, profiles)

    val chkindata = checkinGroup.groupCheckins(newCheckins)

    val friendData = TextLine(friendinput).read.project('line)

    val profilesAndCheckins = combined.project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val employerGroupedServiceProfiles = total.project(('key, 'worked))

    val serviceIds = total.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val friendsForCoworker = friendGroup.groupFriends(friendData)

    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))

    val homeCheckins = checkinGroup.groupHomeCheckins(newCheckins)

    val homeProfilesAndCheckins = profiles.joinWithLarger('key -> 'keyid, homeCheckins).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val findhomefromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(homeProfilesAndCheckins)

    val withHomeWork = combined.joinWithSmaller('key -> 'key1, findcityfromchkins)
            .map('centroid -> 'workCentroid) {
        centroid: String => centroid
    }
            .discard(('key1, 'centroid))
            .joinWithSmaller('key -> 'key1, findhomefromchkins)
            .map('centroid -> 'homeCentroid) {
        centroid: String => centroid
    }
    //            .map('centroid -> 'homeCentroid) {centroid: String => "0.0:0.0"}

    val byAge = businessGroup.byAge(combined)
            .map(('venueKey, 'ageBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, ageBracket, frequency) = in

            val targetVenueGoldenId = venueKey + "_age"
            val column = ageBracket
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))


    val byGender = businessGroup.byGender(combined)
            .map(('venueKey, 'impliedGender, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, impliedGender, frequency) = in

            val targetVenueGoldenId = venueKey + "_gender"
            val column = impliedGender
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))

    val byDegree = businessGroup.byDegree(combined)
            .map(('venueKey, 'degreeCat, 'size) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, degreeCat, frequency) = in

            val targetVenueGoldenId = venueKey + "_education"
            val column = degreeCat
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))

    val totalDegree = byDegree.groupBy('columnName) {
        _.sum('columnValue)
    }
            .map('columnName -> 'rowKey) {
        columnName: String => "totalAll_education"
    }

    val totalAge = byAge.groupBy('columnName) {
        _.sum('columnValue)
    }
            .map('columnName -> 'rowKey) {
        columnName: String => "totalAll_age"
    }

    val totalGender = byGender.groupBy('columnName) {
        _.sum('columnValue)
    }
            .map('columnName -> 'rowKey) {
        columnName: String => "totalAll_gender"
    }

    /* val totalIncome = byIncome.groupBy('columnName) {
        _.sum('columnValue)
    }
            .map('columnName -> 'rowKey) {
        columnName: String => "totalAll_income"
    } */

    val totalStatic = (totalAge ++ totalDegree ++ totalGender).project(('rowKey, 'columnName, 'columnValue))


    val loyalty = reachLoyalty.findLoyalty(combined)

    val loyaltyCount = loyalty
            .map(('venueKey, 'loyalty, 'customers) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, customerType, frequency) = in

            val targetVenueGoldenId = venueKey + "_loyalty_customerCount"
            val column = customerType
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))

    val loyaltyVisits = loyalty
            .map(('venueKey, 'loyalty, 'visitsType) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Int) =>
            val (venueKey, customerType, frequency) = in

            val targetVenueGoldenId = venueKey + "_loyalty_visitCount"
            val column = customerType
            val value = frequency.toDouble

            (targetVenueGoldenId, column, value)

    }
            .project(('rowKey, 'columnName, 'columnValue))


    val reach = reachLoyalty.findReach(withHomeWork)

    val reachMean = reach
            .map(('venueKey, 'meanDist) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Double) =>
            val (venueKey, mean) = in

            val targetVenueGoldenId = venueKey + "_reach_distance"
            val column = "meanDist"
            val value = mean

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachStdev = reach
            .map(('venueKey, 'stdevDist) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Double) =>
            val (venueKey, stdev) = in

            val targetVenueGoldenId = venueKey + "_reach_distance"
            val column = "stdevDist"
            val value = stdev

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachLat = reach
            .map(('venueKey, 'lat) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Double) =>
            val (venueKey, lat) = in

            val targetVenueGoldenId = venueKey + "_reach_distance"
            val column = "latitude"
            val value = lat

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachLong = reach
            .map(('venueKey, 'lng) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Double) =>
            val (venueKey, lng) = in

            val targetVenueGoldenId = venueKey + "_reach_distance"
            val column = "longitude"
            val value = lng

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachHome = reach
            .map(('venueKey, 'numHome) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Int) =>
            val (venueKey, count) = in

            val targetVenueGoldenId = venueKey + "_reach_originCount"
            val column = "numHome"
            val value = count.toDouble

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    val reachWork = reach
            .map(('venueKey, 'numWork) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, Int) =>
            val (venueKey, count) = in

            val targetVenueGoldenId = venueKey + "_reach_originCount"
            val column = "numWork"
            val value = count.toDouble

            (targetVenueGoldenId, column, value)
    }
            .project(('rowKey, 'columnName, 'columnValue))

    /* val byIncome = businessGroup.byIncome(combined)
                .map(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, income, frequency) = in

                val targetVenueGoldenId = venueKey + "_income"
                val column = income
                val value = frequency.toDouble

                (targetVenueGoldenId, column, value)
        }
                .project(('rowKey, 'columnName, 'columnValue))
    */
    val staticOutput =
        (totalStatic ++ reachHome ++ reachWork ++ reachLat ++ reachLong ++ reachMean ++ reachStdev ++ loyaltyCount ++ loyaltyVisits ++ byAge ++ byDegree ++ byGender)

    val staticSequence = staticOutput.write(SequenceFile(sequenceOutputStatic, Fields.ALL))
    val staticText = staticOutput.write(TextLine(textOutputStatic))

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

    val timeSequence = byTime.write(SequenceFile(sequenceOutputTime, Fields.ALL))
    val timeText = staticOutput.write(TextLine(textOutputTime))


}
