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

    val userPlaceTypeScoresTimeSegment = placeInference
            .joinWithSmaller('canonicalVenueId -> 'venueId, venues).discard('venueId)
            .flatMap('venueDto -> 'placeType) {
        dto: ServiceVenueDTO => if (dto.category == null) Seq.empty else dto.category
    }.discard('venueDto)
            .groupBy('userGoldenId, 'timeSegment, 'placeType) {
        _.sum('score -> 'timeSegmentScores)
    }
    val userPlaceTypeScores = userPlaceTypeScoresTimeSegment.groupBy('userGoldenId, 'placeType) {
        _.sum('timeSegmentScores -> 'allScore)
    }
    val categories = userPlaceTypeScores.groupBy('userGoldenId) {
        _.mapList(('placeType, 'timeSegment, 'score) -> ('categories)) {
            timeSegments: List[(String, TimeSegment, Double)] =>
                timeSegments.groupBy(_._1).mapValues(_.map {
                    case (placeType, timeSegment, score) =>
                        Segment(if (timeSegment == null) null else timeSegment.toIndexableString, 1, score.toInt)
                })
        }
    }

    profiles.joinWithLarger('profileId -> 'userGoldenId, categories).mapTo(('profileDto, 'categories) -> 'features) {
        in: (ServiceProfileDTO, Map[String, List[Segment]]) =>
            val (serviceProfile, segmentMap) = in

            Features(id = serviceProfile.profileId, base = Seq(Segment("gender", 0, serviceProfile.gender.ordinal())),
                categories = segmentMap)
    }.write(Tsv(args("featuresOut"), 'features))

}

case class Features(id: Any, base: Iterable[Segment], categories: Map[_ <: Any, Iterable[Segment]])

case class Segment(segment: String, op: Int = 1, value: Int)

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
