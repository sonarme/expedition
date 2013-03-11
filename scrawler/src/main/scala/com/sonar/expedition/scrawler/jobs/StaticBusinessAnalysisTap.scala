package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.Tuples
import org.joda.time.DateMidnight
import com.sonar.dossier.dto
import dto._
import dto.ServiceProfileDTO
import dto.UserEducation
import org.scala_tools.time.Imports._
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.twitter.scalding.IterableSource
import collection.JavaConversions._
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.twitter.scalding.IterableSource

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class StaticBusinessAnalysisTap(args: Args) extends DefaultJob(args) with CheckinSource with DTOProfileInfoPipe with CheckinGrouperFunction with FriendGrouperFunction with BusinessGrouperFunction with AgeEducationPipe with ReachLoyaltyAnalysis with CoworkerFinderFunction with CheckinInfoPipe with PlacesCorrelation with BayesModelPipe {


    val sequenceOutputStaticOption = args.optional("staticOutput")
    val sequenceOutputTimeOption = args.optional("timeOutput")
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)
    val profiles = if (test) IterableSource(Seq(
        ("roger", {
            val roger = ServiceProfileDTO(ServiceType.facebook, "123")
            roger.gender = Gender.male
            roger.birthday = new DateMidnight(1981, 2, 24).toDate
            roger
        }, ServiceType.sonar),
        ("katie", {
            val katie = ServiceProfileDTO(ServiceType.foursquare, "234")
            katie.gender = Gender.female
            katie.birthday = new DateMidnight(1999, 1, 1).toDate
            katie
        }, ServiceType.sonar),
        ("ben", {
            val ben = ServiceProfileDTO(ServiceType.sonar, "2345")
            ben.gender = Gender.male
            ben.education = Seq(UserEducation(year = "2004", degree = "BS"), UserEducation(year = "2005", degree = "MSc"))
            ben
        }, ServiceType.sonar)
    ), Tuples.ProfileIdDTO)
    else SequenceFile(args("profilesIn"), Tuples.ProfileIdDTO)
    val checkinSource = if (test) IterableSource(Seq(

        dto.CheckinDTO(ServiceType.sonar,
            "corner1",
            GeodataDTO(40.745241, -73.982942),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "corner2",
            GeodataDTO(40.744575, -73.983028),
            DateTime.now,
            "ben123",
            None
        )

    ).map(c => c.id -> c), Tuples.CheckinIdDTO)
    else SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO)
    val checkins = checkinSource.read.filter('checkinDTO) {
        c: CheckinDTO => c.venueId != null
    }
    val correlation = if (test) IterableSource(Seq(
        (ServiceProfileLink(ServiceType.sonar, "ben123"), ServiceProfileLink(ServiceType.foursquare, "ben123")),
        (ServiceProfileLink(ServiceType.sonar, "ben123"), ServiceProfileLink(ServiceType.sonar, "ben123"))
    ), Tuples.CorrelationGolden)
    else SequenceFile(args("correlationIn") + "_golden", Tuples.CorrelationGolden)

    val checkinsWithVenueKey = checkins.mapTo('checkinDto ->('checkinId, 'serviceType, 'lat, 'lng, 'hourChunk, 'venueKey, 'profileId)) {
        c: CheckinDTO =>
            val ldt = localDateTime(c.latitude, c.longitude, c.checkinTime.toDate)

            (c.canonicalId, c.serviceType.name(), c.latitude, c.longitude, ldt.getHourOfDay, c.serviceVenue.canonicalId, c.profileId)
    }
    val combined = checkinsWithVenueKey.joinWithSmaller('profileId -> 'profileId, profiles.map('profileDto ->('age, 'impliedGender, 'degree)) {
        p: ServiceProfileDTO =>
            val (age, education) = getAgeAndEducation(p, true)
            (age, p.gender, education)
    }.discard('profileDto))

    sequenceOutputStaticOption foreach {
        sequenceOutputStatic =>

            val ageGenderDegreeCheckins = ageGenderDegree(combined)

            val totalCheckins = checkinsWithVenueKey.groupBy('venueKey) {
                _.size
            }.mapTo(('goldenId, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, size) = in
                    (venueKey + "_numCheckins", "all", size.toDouble)

            } ++ combined.groupBy('venueKey) {
                _.size
            }.mapTo(('venueKey, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, size) = in
                    (venueKey + "_numCheckins", "withProfile", size.toDouble)

            }



            val visits = checkinsWithVenueKey.groupBy('venueKey, 'profileId) {
                _.size('visits)
            }

            val visitsStats = visits.groupBy('venueKey) {
                _.sizeAveStdev('visits ->('_size_, 'visitsAve, 'visitsStdev))
            }.discard('_size_).flatMapTo(('venueKey, 'visitsAve, 'visitsStdev) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double, Double) =>
                    val (venueKey, visitsAve, visitsStdev) = in
                    List((venueKey + "_visits", "ave", visitsAve), (venueKey + "_visits", "stdev", visitsStdev))
            }

            val loyalty = visits.map('visits -> 'loyalty) {
                size: Int =>
                    if (size <= 1)
                        "Passers-By"
                    else if (size <= 3)
                        "Regulars"
                    else
                        "Addicts"

            }.groupBy('venueKey, 'loyalty) {
                _.size('customers).sum('visits -> 'loyaltyVisitCount)
            }.flatMapTo(('venueKey, 'loyalty, 'customers, 'loyaltyVisitCount) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, String, Int, Int) =>
                    val (venueKey, customerType, customers, loyaltyVisitCount) = in
                    List((venueKey + "_loyalty_customerCount", customerType, customers.toDouble), (venueKey + "_loyalty_visitCount", customerType, loyaltyVisitCount.toDouble))
            }


            val withHomeWork = checkinsWithVenueKey.joinWithSmaller('profileId -> 'userGoldenId, SequenceFile(args("centroidsIn"), Tuples.Centroid))
            val reach = findReach(withHomeWork)

            val reachMean = reach
                    .mapTo(('venueKey, 'meanDist) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double) =>
                    val (venueKey, mean) = in
                    (venueKey + "_reach_distance", "meanDist", mean)
            }

            val reachStdev = reach
                    .mapTo(('venueKey, 'stdevDist) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Double) =>
                    val (venueKey, stdev) = in
                    (venueKey + "_reach_distance", "stdevDist", stdev)
            }

            val venues = if (test) IterableSource(Seq(
                ("gg", ServiceVenueDTO(ServiceType.foursquare, "gg", "gg", location = LocationDTO(GeodataDTO(40.744916, -73.982599), "x"), category = Seq("coffee"))),
                ("dd", ServiceVenueDTO(ServiceType.foursquare, "dd", "dd", location = LocationDTO(GeodataDTO(40.744835, -73.982706), "x"), category = Seq("coffee", "bagels"))),
                ("hp24", ServiceVenueDTO(ServiceType.foursquare, "hp24", "hp24", location = LocationDTO(GeodataDTO(40.745144, -73.983006), "x"), category = Seq("hair")))
            ), Tuples.VenueIdDTO)
            else SequenceFile(args("venuesIn"), Tuples.VenueIdDTO)

            val reachLatLong = venues
                    .flatMapTo(('venueDto) ->('rowKey, 'columnName, 'columnValue)) {
                v: ServiceVenueDTO =>
                    Seq((v.canonicalId + "_reach_distance", "latitude", v.location.geodata.latitude),
                        (v.canonicalId + "_reach_distance", "longitude", v.location.geodata.longitude))
            }

            val reachHome = reach
                    .mapTo(('venueKey, 'numHome) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, count) = in
                    (venueKey + "_reach_originCount", "numHome", count.toDouble)
            }

            val reachWork = reach
                    .mapTo(('venueKey, 'numWork) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int) =>
                    val (venueKey, count) = in
                    (venueKey + "_reach_originCount", "numWork", count.toDouble)
            }
            /*
                        val income = SequenceFile(args("income"), ('worktitle, 'income, 'weight)).read
                        val byIncome = groupByIncome(combined.joinWithSmaller('worktitle -> 'worktitle1, income.rename('worktitle -> 'worktitle1)).discard('worktitle1))
                                .map(('venueKey, 'incomeBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                            in: (String, String, Int) =>
                                val (venueKey, income, frequency) = in
                                (venueKey + "_income", income, frequency.toDouble)
                        }.project('rowKey, 'columnName, 'columnValue)
                        val totalIncome = byIncome.groupBy('columnName) {
                            _.sum('columnValue)
                        }.map(() -> 'rowKey) {
                            u: Unit => "totalAll_income"
                        }.project('rowKey, 'columnName, 'columnValue)*/

            val staticOutput =
                (/*byIncome ++ totalIncome ++*/ ageGenderDegreeCheckins ++ totalCheckins ++ reachHome ++ reachWork ++ reachLatLong ++ reachMean ++ reachStdev ++ loyalty ++ visitsStats)

            staticOutput.write(SequenceFile(sequenceOutputStatic, Fields.ALL))
                    .write(Tsv(sequenceOutputStatic + "_tsv", Fields.ALL))
    }
    sequenceOutputTimeOption foreach {
        sequenceOutputTime =>
            val byTime = timeSeries(checkinsWithVenueKey)
                    .mapTo(('venueKey, 'hourChunk, 'serviceType, 'size) ->('rowKey, 'columnName, 'columnValue)) {
                in: (String, Int, String, Int) =>
                    val (venueKey, hour, serviceType, frequency) = in
                    (venueKey + "_checkinFrequencyPerHour_" + serviceType, hour.toLong * 3600000, frequency.toDouble)

            }

            byTime.write(SequenceFile(sequenceOutputTime, Fields.ALL))
                    .write(Tsv(sequenceOutputTime + "_tsv", Fields.ALL))
    }


    def ageGenderDegree(combined: RichPipe) = {


        val ageMetrics = combined.groupBy('venueKey) {
            _.sizeAveStdev('age ->('ageSize, 'ageAve, 'ageStdev))
        }.flatMapTo(('venueKey, 'ageSize, 'ageAve, 'ageStdev) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, Int, Double, Double) =>
                val (venueKey, ageSize, ageAve, ageStdev) = in
                List("ageSize" -> ageSize.toDouble, "ageAve" -> ageAve, "ageStdev" -> ageStdev) map {
                    case (metricName, metricValue) => (venueKey + "_" + metricName, "metric", metricValue)
                }
        }

        val byAge = groupByAge(combined)
                .mapTo(('venueKey, 'ageBracket, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, ageBracket, frequency) = in
                (venueKey + "_age", ageBracket, frequency.toDouble)
        }

        val byGender = groupByGender(combined)
                .mapTo(('venueKey, 'impliedGender, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, impliedGender, frequency) = in
                (venueKey + "_gender", impliedGender, frequency.toDouble)

        }

        val byDegree = groupByDegree(combined)
                .mapTo(('venueKey, 'degreeCat, 'size) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Int) =>
                val (venueKey, degreeCat, frequency) = in
                (venueKey + "_education", degreeCat, frequency.toDouble)

        }
        val totalDegree = byDegree.groupBy('columnName) {
            _.sum('columnValue)
        }.map(() -> 'rowKey) {
            u: Unit => "totalAll_education"
        }

        val totalAge = byAge.groupBy('columnName) {
            _.sum('columnValue)
        }.map(() -> 'rowKey) {
            u: Unit => "totalAll_age"
        }

        val totalGender = byGender.groupBy('columnName) {
            _.sum('columnValue)
        }.map(() -> 'rowKey) {
            u: Unit => "totalAll_gender"
        }
        val totalStatic = (totalAge ++ totalDegree ++ totalGender).project('rowKey, 'columnName, 'columnValue)
        byAge ++ byDegree ++ byGender ++ ageMetrics ++ totalStatic
    }
}
