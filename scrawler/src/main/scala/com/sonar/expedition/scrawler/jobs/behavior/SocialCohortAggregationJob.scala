package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.twitter.scalding.Args
import com.sonar.expedition.scrawler.util.{CommonFunctions, Tuples}
import com.sonar.dossier.dto._
import collection.JavaConversions._
import org.joda.time.DateMidnight
import com.sonar.expedition.scrawler.source.LuceneIndexOutputFormat
import org.apache.lucene.document.Field.Store
import cascading.tuple.{Tuple => CTuple, TupleEntry}
import CommonFunctions._
import org.apache.lucene.index.FieldInfo.IndexOptions
import org.apache.lucene.document.FieldType
import com.sonar.dossier.dto.ServiceVenueDTO
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.source.LuceneSource
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.dossier.dto.LocationDTO
import com.twitter.scalding.Tsv
import com.twitter.scalding.IterableSource
import com.sonar.dossier.dto.GeodataDTO
import com.sonar.expedition.common.adx.search.model.IndexField

class SocialCohortAggregationJob(args: Args) extends DefaultJob(args) {
    val test = args.optional("test").map(_.toBoolean).getOrElse(false)

    // Profiles
    val profiles = if (test) IterableSource(Seq(
        ("roger", {
            val roger = ServiceProfileDTO(ServiceType.facebook, "123")
            roger.gender = Gender.male
            roger.birthday = new DateMidnight(1981, 2, 24).toDate
            roger
        }, ServiceType.facebook),
        ("katie", {
            val katie = ServiceProfileDTO(ServiceType.foursquare, "234")
            katie.gender = Gender.male
            katie.birthday = new DateMidnight(1999, 1, 1).toDate
            katie
        }, ServiceType.foursquare)
    ), Tuples.ProfileIdDTO)
    else SequenceFile(args("profilesIn"), Tuples.ProfileIdDTO)

    // Venues
    val venues = if (test) IterableSource(Seq(
        ("sonar", ServiceVenueDTO(ServiceType.foursquare, "sonar", "Sonar", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("office"))),
        ("tracks", ServiceVenueDTO(ServiceType.foursquare, "tracks", "Tracks", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("office"))),
        ("nysc", ServiceVenueDTO(ServiceType.foursquare, "nysc", "NYSC", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("gym"))),
        ("penn", ServiceVenueDTO(ServiceType.foursquare, "penn", "Penn", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("train"))),
        ("esen", ServiceVenueDTO(ServiceType.foursquare, "esen", "Esen", location = LocationDTO(GeodataDTO(40.0, -74.0), "x"), category = Seq("deli")))
    ), Tuples.VenueIdDTO)
    else SequenceFile(args("venuesIn"), Tuples.VenueIdDTO)

    // PlaceInference
    val placeInference = if (test) IterableSource(Seq(
        ("roger", "location1", 2, "nysc", 10, new TimeSegment(true, "7")),
        ("roger", "location1", 3, "penn", 4, new TimeSegment(true, "7")),
        ("roger", "location1", 2, "sonar", 1, new TimeSegment(true, "7")),
        ("roger", "location2", 1, "tracks", 8, new TimeSegment(true, "16")),
        ("roger", "location2", 2, "sonar", 2, new TimeSegment(true, "16")),
        ("katie", "location1", 2, "esen", 10, new TimeSegment(true, "16"))
    ), Tuples.PlaceInference)
    else SequenceFile(args("placeInferenceIn"), Tuples.PlaceInference)


    val log15 = math.log(1.5)

    val userPlaceTypeScoresTimeSegment = placeInference.read
            .joinWithSmaller('canonicalVenueId -> 'venueId, venues.read).discard('venueId)
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
        _.mapList(('placeType, 'timeSegment, 'timeSegmentScores) -> ('categories)) {
            timeSegments: List[(String, TimeSegment, Double)] =>
                timeSegments.groupBy(_._1).mapValues(_.map {
                    case (placeType, timeSegment, score) =>
                        val bucketedScore = (math.log(score) / log15).toInt
                        FeatureSegment(if (timeSegment == null) null else timeSegment.toIndexableString, Op.`>`, bucketedScore)
                })
        }
    }

    profiles.read.joinWithLarger('profileId -> 'userGoldenId, categories).mapTo(('profileDto, 'categories) -> ('explodedFeatures)) {
        in: (ServiceProfileDTO, Map[String, List[FeatureSegment]]) =>
            val (serviceProfile, segmentMap) = in

            val features = Features(
                id = serviceProfile.profileId,
                base = Seq(FeatureSegment("gender", Op.`=`, serviceProfile.gender.name())),
                categories = segmentMap)
            features.explode()

    }.write(Tsv(args("featuresOut") + "_tsv"))
            .shard(5).write(LuceneSource(args("featuresOut"), classOf[SocialCohortOutputFormat]))


}

case class ExplodedFeatures(id: Any, features: Iterable[String])

case class Features(id: Any, base: Iterable[FeatureSegment], categories: Map[_ <: Any, Iterable[FeatureSegment]]) {
    def explode() = {
        val baseSet = base.map(_.toString).toSet
        val exploded = (for ((key, featureSegments) <- categories;
                             featureSegment <- featureSegments.map(_.toString)) yield {
            val keyedSegment = key.toString + "@" + featureSegment
            powerSet(baseSet + keyedSegment).map(_.mkString("&"))
        }).reduce(_ ++ _)

        ExplodedFeatures(id, exploded)
    }

}

object Op extends Enumeration("<", "=", ">") {
    val `<`, `=`, `>` = Value
}

case class FeatureSegment(segment: String, op: Op.Value = Op.`>`, value: Any) {
    def this() = this(null, Op.`>`, 0)

    override def toString = segment + op.toString + value.toString


}


class SocialCohortOutputFormat extends LuceneIndexOutputFormat[CTuple, TupleEntry] {
    val SocialCohortFieldType = new FieldType
    SocialCohortFieldType.setIndexed(true)
    SocialCohortFieldType.setOmitNorms(true)
    SocialCohortFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS)
    SocialCohortFieldType.setTokenized(false)
    SocialCohortFieldType.setStoreTermVectors(true)
    SocialCohortFieldType.freeze()

    def buildDocument(key: CTuple, value: TupleEntry) = {
        import org.apache.lucene.document._
        val doc = new Document
        val features = value.getObject(0).asInstanceOf[ExplodedFeatures]
        doc.add(new StringField(IndexField.Key.toString, features.id.toString, Store.YES))
        // write all social cohorts
        features.features foreach {
            feature => doc.add(new Field(IndexField.SocialCohort.toString, feature, SocialCohortFieldType))
        }
        doc
    }
}
