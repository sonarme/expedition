package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine
import java.util.{Date, TimeZone}
import java.text.SimpleDateFormat
import ch.hsr.geohash.GeoHash

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222

/*
com.sonar.expedition.scrawler.jobs.StaticBusinessAnalysisTapFromTextFile --hdfs --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
--serviceProfileInput "/tmp/serviceProfileData.txt" --twitterServiceProfileInput "/tmp/twitterServiceProfileDatasmallest.txt" --checkinInput "/tmp/checkinDatatest.txt" --friendInput "/tmp/friendData.txt"
 --bayestrainingmodelforsalary "/tmp/bayestrainingmodelforsalary" --sequenceOutputStatic "/tmp/sequenceOutputStatic" --sequenceOutputTime "/tmp/sequenceOutputTime" --textOutputStatic "/tmp/textOutputStatic" --textOutputTime "/tmp/textOutputTime"
 */
class StaticBusinessAnalysisTapFromTextFile(args: Args) extends Job(args) {


    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val DEFAULT_FILTER_DATE = "2000-08-04T16:23:13.000Z"
    val input = args("serviceProfileInput")
    val twinput = args("twitterServiceProfileInput")
    val friendinput = args("friendInput")
    val bayestrainingmodel = args("bayestrainingmodelforsalary")
    val sequenceOutputStatic = args("sequenceOutputStatic")
    val sequenceOutputTime = args("sequenceOutputTime")
    val textOutputStatic = args("textOutputStatic")
    val textOutputTime = args("textOutputTime")
    val timeSeriesOnly = args.getOrElse("timeSeriesOnly", "false").toBoolean
    val checkininput = args("checkinInput")

    implicit val defaultTimezone = TimeZone.getTimeZone(args.getOrElse("tz", "America/New_York"))

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


    val checkins = checkinGroup.checkinsWithMessage(TextLine(checkininput))

            .mapTo(
        ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'msg)
                ->
                ('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg)
    ) {

        fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>
            val (keyid, serType, serProfileID, serCheckinID, venName, venAddress, venId, chknTime, ghash, lat, lng, dayOfYear, dayOfWeek, hour, msg) = fields

            val gHashAsLong = Option(ghash).map(GeoHash.fromGeohashString(_).longValue()).getOrElse(0L)
            val checkinTime = getDate(chknTime)
            //val richDate = RichDate(checkinTime)
            (serType + ":" + venId, keyid, serType, hashed(serProfileID), serCheckinID, venName, venAddress, venId, checkinTime, gHashAsLong, lat.toDouble, lng.toDouble, msg)
    }

    def getDate(chknTime: String): Date = {

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'")
        val date = {
            try {
                val chekingTime = chknTime.substring(chknTime.lastIndexOf(":") + 1)
                simpleDateFormat.parse(chekingTime)

            } catch {
                //handle date parsing if RE doesnt match:
                //2012-08-04T16:23:13.000Z
                //::foursquare::1694154::501d4c71e4b0af03cf4a440a::Marinas House(:::::2012-08-04T16:23:13.000Z::dn0pr6yrc5xz::35.040870666503906::-89.67276000976562::4f879915e4b0202c048fcaef::
                case e => simpleDateFormat.parse(DEFAULT_FILTER_DATE)


            }
        }
        date

    }


    //facebook:428112530552458                facebook        100000013594714 486290991381350 Kobe Airport - 神戸空港         428112530552458 Fri Aug 04 16:23:13 EDT 2000    -1368785845864561184    34.636644171583 135.22792465066 無事到着！      217     6       16      facebook:100000013594714
    // ('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg,'dayOfYear, 'dayOfWeek, 'hour, 'keyid))
    val newCheckins = checkinGroup.correlationCheckinsFromCassandra(checkins)
    val checkinsWithGolden = placesCorrelation.withGoldenId(newCheckins)
            .map(('lat, 'lng) -> ('loc)) {
        fields: (String, String) =>
            val (lat, lng) = fields
            val loc = lat + ":" + lng
            (loc)
    }

    val total = dtoProfileGetPipe.getTotalProfileTuples(data, twdata).map('uname ->('impliedGender, 'impliedGenderProb)) {
        name: String =>
            val (gender, prob) = GenderFromNameProbability.gender(name)
            (gender, prob)
    }
    //0011df62-82fe-3cf7-8fee-ab676fbc3c27    Tom B.  13e25b49bd71f5628c7fc92ca8ced47b                0011df6282fe8cf78feeab676fbc3c27        9df9309b2c938b6bb50ed8b0584b48ef                        Winston-Salem, NC                                       male    1.0     -1      NA

    val profiles = ageEducation.ageEducationPipe(total)
            .project(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree))
            .flatMapTo(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree)
            ->
            ('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree)) {
        fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, Gender, Double, String, String) =>
        //nned not handle linked in because there ar no checkins from linked in and sonar checkins dont have id , so key comes as sonar: empty, need to fix it, ask Paul, todo.
            val keys = ("facebook:" + fields._3 + "," + "twitter:" + fields._6 + "," + "foursquare:" + fields._5).split(",")
            for (key <- keys)
            yield (key, fields._2, fields._3, fields._4, fields._5, fields._6, fields._7, fields._8, fields._9, fields._10, fields._11, fields._12, fields._13, fields._14, fields._15, fields._16, fields._17)

    }

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

    if (!timeSeriesOnly) {

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
            in: (String, Gender, Int) =>
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

    }

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
    val timeText = byTime.write(TextLine(textOutputTime))


}

