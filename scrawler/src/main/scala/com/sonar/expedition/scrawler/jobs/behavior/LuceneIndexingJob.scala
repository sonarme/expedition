package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._

import com.sonar.expedition.scrawler.util.Tuples
import com.twitter.scalding.SequenceFile

import com.sonar.expedition.scrawler.jobs.DefaultJob
import com.sonar.dossier.dto
import dto._
import dto.GeodataDTO
import dto.LocationDTO
import dto.ServiceVenueDTO
import org.scala_tools.time.Imports._
import scala.Some
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.checkins.CheckinInference
import com.sonar.expedition.scrawler.source.{LuceneIndexOutputFormat, LuceneSource}
import org.apache.lucene.document.Field.Store
import ch.hsr.geohash.GeoHash
import cascading.tuple.{Tuple => CTuple, TupleEntry}
import LuceneIndexingJob._
import org.joda.time.{Hours => JTHours}
import com.sonar.expedition.common.segmentation.TimeSegmentation
import com.sonar.expedition.common.adx.search.model.IndexField

class LuceneIndexingJob(args: Args) extends DefaultJob(args) with CheckinInference with TimeSegmentation {

    val test = args.optional("test").map(_.toBoolean).getOrElse(false)

    val checkins = if (test) IterableSource(Seq(
        dto.CheckinDTO(ServiceType.foursquare,
            "test1a",
            GeodataDTO(40.7505800, -73.9935800),
            DateTime.now,
            "ben1234",
            Some(ServiceVenueDTO(ServiceType.foursquare, "chi", "Chipotle", location = LocationDTO(GeodataDTO(40.7505800, -73.9935800), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test1",
            GeodataDTO(40.7505800, -73.9935800),
            DateTime.now,
            "ben123",
            Some(ServiceVenueDTO(ServiceType.foursquare, "chi", "Chipotle", location = LocationDTO(GeodataDTO(40.7505800, -73.9935800), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test2",
            GeodataDTO(40.749712, -73.993092),
            DateTime.now,
            "ben123",
            Some(ServiceVenueDTO(ServiceType.foursquare, "gg", "G&G", location = LocationDTO(GeodataDTO(40.0, -74.0000012), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test2b",
            GeodataDTO(40.749712, -73.993092),
            DateTime.now - 1.minute,
            "ben123",
            Some(ServiceVenueDTO(ServiceType.foursquare, "gg", "G&G", location = LocationDTO(GeodataDTO(40.0, -74.0000012), "x")))
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test2c",
            GeodataDTO(40.750817, -73.992405),
            DateTime.now - 1.minute,
            "ben12345",
            Some(ServiceVenueDTO(ServiceType.foursquare, "cosi", "Cosi", location = LocationDTO(GeodataDTO(40.0, -74.0000013), "x")))
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test3",
            GeodataDTO(40.750183, -73.992512),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test4",
            GeodataDTO(40.750183, -73.992513),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test5",
            GeodataDTO(40.750183, -73.992514),
            DateTime.now,
            "ben123",
            None
        ),
        dto.CheckinDTO(ServiceType.foursquare,
            "test5b",
            GeodataDTO(40.791979, -73.957215),
            DateTime.now - 1.minute,
            "ben12345",
            Some(ServiceVenueDTO(ServiceType.foursquare, "xy", "XY", location = LocationDTO(GeodataDTO(40.791979, -73.957214), "x")))
        ),
        dto.CheckinDTO(ServiceType.sonar,
            "test5a",
            GeodataDTO(40.791979, -73.957214),
            DateTime.now,
            "ben123",
            None
        )
    ).map(c => c.id -> c), Tuples.CheckinIdDTO)
    else SequenceFile(args("checkinsIn"), Tuples.CheckinIdDTO)

    //    userLocations.read
    //    SequenceFile(userCategories, Tuples.Behavior.UserCategories).read
    checkins.read
            .mapTo('checkinDto -> IndexedFields) {
        fields: CheckinDTO => {
            val checkin = fields
            val serviceId = checkin.profileId
            val timeSegment = hourSegment(checkin.latitude, checkin.longitude, checkin.checkinTime.toDate)
            val geosector = GeoHash.withCharacterPrecision(checkin.latitude, checkin.longitude, 7).toBase32
            val timeWindow = JTHours.hoursBetween(new DateTime(0), checkin.checkinTime).getHours
            (serviceId, geosector, geosector + ":" + timeSegment, geosector + ":" + timeWindow, checkin.ip)
        }
    }.groupBy('serviceId) {
        _.toList[(String, String, String, String)](('geosector, 'geosectorTimesegment, 'geosectorTimewindow, 'ip) -> ('indexedFields))
    }
            .shard(5).write(LuceneSource(args("output"), classOf[CheckinIndexOutputFormat]))
}

object LuceneIndexingJob {
    val IndexedFields = ('serviceId, 'geosector, 'geosectorTimesegment, 'geosectorTimewindow, 'ip)
}

class CheckinIndexOutputFormat extends LuceneIndexOutputFormat[CTuple, TupleEntry] {

    def buildDocument(key: CTuple, value: TupleEntry) = {
        import org.apache.lucene.document._
        val doc = new Document
        doc.add(new StringField(IndexField.ServiceId.toString, value.getString("serviceId"), Store.YES))
        value.getObject("indexedFields").asInstanceOf[Iterable[(String, String, String, String)]] foreach {
            case (geosector, geosectorTimesegment, geosectorTimewindow, ip) =>
                doc.add(new StringField(IndexField.Geosector.toString, geosector, Store.NO))
                doc.add(new StringField(IndexField.GeosectorTimesegment.toString, geosectorTimesegment, Store.NO))
                doc.add(new StringField(IndexField.GeosectorTimewindow.toString, geosectorTimewindow, Store.NO))
                if (ip != null)
                    doc.add(new StringField(IndexField.Ip.toString, ip, Store.NO))
        }
        doc
    }
}
