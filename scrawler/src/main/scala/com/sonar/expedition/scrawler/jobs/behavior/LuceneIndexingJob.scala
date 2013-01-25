package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._

import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.dto.indexable.{UserLocationDTO, IndexField, UserDTO}
import com.twitter.scalding.SequenceFile

import com.sonar.expedition.scrawler.jobs.DefaultJob
import org.apache.lucene.store.{SimpleFSLockFactory, NoLockFactory, FSDirectory}
import java.io.File
import org.apache.hadoop
import hadoop.conf.Configuration
import hadoop.fs.{FileSystem, Path}
import org.apache.blur.store.hdfs.HdfsDirectory
import java.util.Random
import org.apache.lucene.index.{IndexReader, IndexWriterConfig}
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryParser.QueryParser
import com.sonar.dossier.dto
import dto._
import dto.GeodataDTO
import dto.LocationDTO
import dto.ServiceVenueDTO
import org.scala_tools.time.Imports._
import com.sonar.expedition.scrawler.dto.indexable.UserLocationDTO
import com.twitter.scalding.Tsv
import scala.Some
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.checkins.CheckinInference
import org.apache.blur.index.IndexWriter
import org.apache.blur.store.lock.BlurLockFactory

class LuceneIndexingJob(args: Args) extends DefaultJob(args) with CheckinInference {

    val test = args.optional("test").map(_.toBoolean).getOrElse(false)

    val checkins = if(test) IterableSource(Seq(
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
        .map(('checkinId, 'checkinDto) -> ('indexed)) {
        fields: (String, CheckinDTO) => {
            val (checkinId, checkin) = fields

            val filePath = new Path(args("hdfsPath"))

            val hdfsDirectory = new HdfsDirectory(filePath)
            val configuration:Configuration = new Configuration()
            val lockFactory = args("lockFactory") match {
                case "blur" => new BlurLockFactory(configuration, filePath, args("hdfsPath"), 0)
                case "fs" => new SimpleFSLockFactory()
                case _ => NoLockFactory.getNoLockFactory
            }
            hdfsDirectory.setLockFactory(lockFactory)

            val indexWriter = new IndexWriter(hdfsDirectory, new IndexWriterConfig(Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36)).setMaxBufferedDocs(2))
            val serviceId = checkin.serviceType.toString + "_" + checkin.serviceProfileId
            val ldt = localDateTime(checkin.latitude, checkin.longitude, checkin.checkinTime.toDate)
            val weekday = isWeekDay(ldt)
            val timeSegment = new TimeSegment(weekday, ldt.hourOfDay.toString)
            val userLocation = new UserLocationDTO(serviceId, new GeodataDTO(checkin.latitude, checkin.longitude), checkin.ip, timeSegment)
            indexWriter.addDocument(userLocation.getDocument())
            indexWriter.close()
            hdfsDirectory.close()

            "success"
        }
    }.discard('checkinDto)
    .write(Tsv(args("output")))
}