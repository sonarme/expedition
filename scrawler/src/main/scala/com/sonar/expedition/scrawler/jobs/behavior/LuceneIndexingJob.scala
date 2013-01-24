package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.expedition.scrawler.service.SearchServiceImpl
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.dto.indexable.{UserLocationDTO, IndexField, UserDTO}
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.sonar.expedition.scrawler.jobs.DefaultJob
import org.apache.lucene.store.{NoLockFactory, FSDirectory}
import java.io.File
import org.apache.hadoop
import hadoop.conf.Configuration
import hadoop.fs.{FileSystem, Path}
import org.apache.blur.store.hdfs.HdfsDirectory
import java.util.Random
import org.apache.lucene.index.{IndexReader, IndexWriter, IndexWriterConfig}
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
import com.sonar.expedition.scrawler.jobs.behavior.TimeSegment
import com.twitter.scalding.Tsv
import scala.Some
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.dto.indexable.UserLocationDTO
import com.sonar.expedition.scrawler.jobs.behavior.TimeSegment
import com.twitter.scalding.Tsv
import scala.Some
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.dto.indexable.UserLocationDTO
import com.sonar.expedition.scrawler.jobs.behavior.TimeSegment
import com.twitter.scalding.Tsv
import scala.Some
import com.twitter.scalding.IterableSource
import com.sonar.expedition.scrawler.checkins.CheckinInference

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
            hdfsDirectory.setLockFactory(NoLockFactory.getNoLockFactory)

            val indexWriter = new IndexWriter(hdfsDirectory, new IndexWriterConfig(Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36)))
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