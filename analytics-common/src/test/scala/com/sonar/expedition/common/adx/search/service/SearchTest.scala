package com.sonar.expedition.common.adx.search.service

import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.apache.lucene.store.RAMDirectory

import org.apache.lucene.index._
import org.apache.lucene.util.Version
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.analysis.standard.StandardAnalyzer
import com.sonar.expedition.common.adx.search.model.{IndexField, UserLocation}

class SearchTest extends FlatSpec with BeforeAndAfterEach {

    var searchService: SearchService = _

    val userLocations = Seq(
        new UserLocation("roger", 40.750580, -73.993580, "1.2.3.4", "true_7"),
        new UserLocation("roger", 40.748433, -73.985656, "2.3.4.5", "true_7"),
        new UserLocation("paul", 40.7505, -73.9935, "1.2.33.4", "false_5"),
        new UserLocation("paul", 40.752531, -73.977449, "3.4.5.6", "true_7"),
        new UserLocation("ben", 40.752531, -73.977449, "3.4.5.6", "true_7"),
        new UserLocation("brett", 40.750580, -73.993580, "13.2.3.4", "true_17"),
        new UserLocation("brett", 40.748433, -73.985656, "2.3.4.5", "true_7"),
        new UserLocation("roger", 40.750580, -73.993580, "1.2.3.4", "false_7")
    )

    override def beforeEach() {
        val directory = new RAMDirectory()
        val writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_41, new StandardAnalyzer(Version.LUCENE_41)).setOpenMode(OpenMode.CREATE_OR_APPEND))
        writer.commit()
        val reader = DirectoryReader.open(directory)
        searchService = new SearchService(
            reader,
            writer
        )
    }

    "Lucene" should "index a UserLocation" in {
        val docs = searchService.index(userLocations.head)
        searchService.reopen()
        assert(searchService.numDocs === 1)
    }

    "Lucene" should "return 1 Hit" in {
        searchService.indexMulti(userLocations)
        searchService.reopen()

        val hits = searchService.search(IndexField.ServiceId, "ben")
        assert(hits.totalHits === 1)
    }

    "Lucene" should "return multiple Hits" in {
        searchService.indexMulti(userLocations)
        searchService.reopen()

        val hits = searchService.search(IndexField.Ip, "1.2.3.4")
        assert(hits.totalHits === 2)
    }

    "Lucene" should "produce 'more like this' results" in {
        searchService.indexMulti(userLocations)
        searchService.reopen()
        assert(searchService.numDocs === 8)

        val ul = new UserLocation("foo", 40.750580, -73.993580, "1.2.3.4", "true_8")

        val more = searchService.moreLikeThis(Map(IndexField.Ip.toString -> ul.ip, IndexField.Geosector.toString -> ul.geosector))
        assert(more.totalHits === 4)

        val ul2 = new UserLocation("bar", 40.750580, -73.993580, "1.2.3.4", "true_7")
        val more2 = searchService.moreLikeThis(Map(IndexField.Ip.toString -> ul2.ip, IndexField.Geosector.toString -> ul2.geosector))
        assert(more2.totalHits === 4)
    }

}
