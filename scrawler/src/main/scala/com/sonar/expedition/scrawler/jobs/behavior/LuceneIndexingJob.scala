package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.expedition.scrawler.service.SearchServiceImpl
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.dto.indexable.{UserLocationDTO, IndexField, UserDTO}
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.sonar.dossier.dto.{GeodataDTO, LocationDTO, ServiceType, ServiceVenueDTO}
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

class LuceneIndexingJob(args: Args) extends DefaultJob(args) {
//    val userCategories = args("userCategories")

    val userCategories = IterableSource(Seq(
    ("roger", Seq("gymrat", "foodie")),
    ("katie", Seq("entrepreneur")),
    ("brett", Seq("entrepreneur")),
    ("paul", Seq("geek", "hipster"))
    ), Tuples.Behavior.UserCategories)

    val userLocations = IterableSource(Seq(
        new UserLocationDTO("roger", new GeodataDTO(40.750580, -73.993580), "1.2.3.4", new TimeSegment(true, "7")),
        new UserLocationDTO("roger", new GeodataDTO(40.748433, -73.985656), "2.3.4.5", new TimeSegment(true, "7")),
        new UserLocationDTO("paul", new GeodataDTO(40.7505, -73.9935), "1.2.33.4", new TimeSegment(false, "5")),
        new UserLocationDTO("paul", new GeodataDTO(40.752531, -73.977449), "3.4.5.6", new TimeSegment(true, "7")),
        new UserLocationDTO("ben", new GeodataDTO(40.752531, -73.977449), "3.4.5.6", new TimeSegment(true, "7")),
        new UserLocationDTO("brett", new GeodataDTO(40.750580, -73.993580), "13.2.3.4", new TimeSegment(true, "17")),
        new UserLocationDTO("brett", new GeodataDTO(40.748433, -73.985656), "2.3.4.5", new TimeSegment(true, "7")),
        new UserLocationDTO("roger", new GeodataDTO(40.750580, -73.993580), "1.2.3.4", new TimeSegment(false, "7"))
    ), Tuples.Behavior.UserLocation)

    userLocations.read
//    SequenceFile(userCategories, Tuples.Behavior.UserCategories).read
        .map(('userLocation) -> ('indexed)) {
        userlocation: UserLocationDTO => {

            val filePath = new Path(args("hdfsPath"))

            val hdfsDirectory = new HdfsDirectory(filePath)
            hdfsDirectory.setLockFactory(NoLockFactory.getNoLockFactory)

            val indexWriter = new IndexWriter(hdfsDirectory, new IndexWriterConfig(Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36)))
            indexWriter.addDocument(userlocation.getDocument())
            indexWriter.close()

            hdfsDirectory.close()

            "success"
        }
    }.write(Tsv(args("output"), Tuples.Behavior.UserLocation append ('indexed)))
}