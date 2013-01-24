package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.twitter.scalding.{Tsv, SequenceFile, Args}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.dossier.dto.{ServiceProfileDTO, ServiceVenueDTO}
import collection.JavaConversions._

class SocialCohortAggregationJob(args: Args) extends DefaultJob(args) {

    val profiles = SequenceFile(args("profilesIn"), Tuples.ProfileIdDTO).read
    val venues = SequenceFile(args("venuesIn"), Tuples.VenueIdDTO).read
    val placeInference = Tsv(args("placeInferenceIn"), Tuples.PlaceInference).read
    val log15 = math.log(1.5)
    val userPlaceTypeScoresTimeSegment = placeInference
            .joinWithSmaller('canonicalVenueId -> 'venueId, venues).discard('venueId)
            .flatMap('venueDto -> 'placeType) {
        dto: ServiceVenueDTO => if (dto.category == null) Seq.empty else dto.category
    }.discard('venueDto)
            .map(('numVisits, 'score) -> 'computedScore) {
        in: (Int, Double) =>
            val (numVisits, score) = in
            numVisits * score
    }
            .groupBy('userGoldenId, 'timeSegment, 'placeType) {
        _.sum('computedScore -> 'timeSegmentScores)
    }
    val categories = userPlaceTypeScoresTimeSegment.groupBy('userGoldenId) {
        _.mapList(('placeType, 'timeSegment, 'score) -> ('categories)) {
            timeSegments: List[(String, TimeSegment, Double)] =>
                timeSegments.groupBy(_._1).mapValues(_.map {
                    case (placeType, timeSegment, score) =>
                        val bucketedScore = (math.log(score) / log15).toInt
                        FeatureSegment(if (timeSegment == null) null else timeSegment.toIndexableString, 1, bucketedScore)
                })
        }
    }

    profiles.joinWithLarger('profileId -> 'userGoldenId, categories).mapTo(('profileDto, 'categories) -> 'features) {
        in: (ServiceProfileDTO, Map[String, List[FeatureSegment]]) =>
            val (serviceProfile, segmentMap) = in

            Features(id = serviceProfile.profileId, base = Seq(FeatureSegment("gender", 0, serviceProfile.gender.ordinal())),
                categories = segmentMap)
    }.write(Tsv(args("featuresOut"), 'features))

}

case class Features(id: Any, base: Iterable[FeatureSegment], categories: Map[_ <: Any, Iterable[FeatureSegment]])

case class FeatureSegment(segment: String, op: Int = 1, value: Int)

/* val source = io.Source.fromInputStream(getClass.getResourceAsStream("/foursquare_venues_categories.json"))
    val venuesJson = try {
        source.mkString
    } finally {
        source.close()
    }
    val categories = JSONFieldParser.parseEntities(classOf[Category], new JSONArray(venuesJson), true).asInstanceOf[Array[Category]]
    // this is not stable if categories change, maybe use a DB or something?
    val catIdMap = (for ((category, categoryIdx) <- categories.zipWithIndex;
                         (subCategory, subCategoryIdx) <- (category.getCategories.zipWithIndex))
    yield subCategory.getId -> (categoryIdx * 100 + subCategoryIdx)).toMap[String, Int]

    println(categories)*/
