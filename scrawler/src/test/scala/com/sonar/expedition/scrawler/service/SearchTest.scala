package com.sonar.expedition.scrawler.service

import com.sonar.expedition.scrawler.dto.indexable.{UserLocationDTO, IndexField, UserDTO}
import com.sonar.expedition.scrawler.dto.indexable.IndexField._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import com.sonar.dossier.dto.GeodataDTO
import com.sonar.expedition.scrawler.jobs.behavior.TimeSegment
import org.apache.lucene.store.RAMDirectory

class SearchTest extends FlatSpec with BeforeAndAfter {
    var searchService: SearchService = _

    val users = List[UserDTO](
        new UserDTO("roger", List[String]("gymrat, workaholic"), new GeodataDTO(42, -73), "1.2.3.4"),
        new UserDTO("paul", List[String]("gymrat", "serial_eater"), new GeodataDTO(42.22, -73.1), "2.2.3.4"),
        new UserDTO("ben", List[String](), new GeodataDTO(42.3, -73), "1.2.3.4"),
        new UserDTO("katie", List[String](), new GeodataDTO(42, -73), "4.2.3.4"),
        new UserDTO("brett", List[String]("workaholic", "entrepreneur"), new GeodataDTO(42.3, -73), "1.2.34.4"),
        new UserDTO("ximena", List[String]("social_butterfly", "gymrat"), new GeodataDTO(42, -73), "1.2.34.4"),
        new UserDTO("daniel", List[String](), null, null)
    )

    val userLocations = List[UserLocationDTO](
        new UserLocationDTO("roger", new GeodataDTO(40.750580, -73.993580), "1.2.3.4", new TimeSegment(true, "7")),
        new UserLocationDTO("roger", new GeodataDTO(40.748433, -73.985656), "2.3.4.5", new TimeSegment(true, "7")),
        new UserLocationDTO("paul", new GeodataDTO(40.7505, -73.9935), "1.2.33.4", new TimeSegment(false, "5")),
        new UserLocationDTO("paul", new GeodataDTO(40.752531, -73.977449), "3.4.5.6", new TimeSegment(true, "7")),
        new UserLocationDTO("ben", new GeodataDTO(40.752531, -73.977449), "3.4.5.6", new TimeSegment(true, "7")),
        new UserLocationDTO("brett", new GeodataDTO(40.750580, -73.993580), "13.2.3.4", new TimeSegment(true, "17")),
        new UserLocationDTO("brett", new GeodataDTO(40.748433, -73.985656), "2.3.4.5", new TimeSegment(true, "7")),
        new UserLocationDTO("roger", new GeodataDTO(40.750580, -73.993580), "1.2.3.4", new TimeSegment(false, "7"))
    )

    before {
        searchService = new SearchServiceImpl(new RAMDirectory())
    }

    "Lucene" should "index a User" in {
        searchService.index(users.take(1))
        assert(searchService.numDocs() == 1)
    }

    "Lucene" should "return 1 Hit" in {
        searchService.index(users)

        val hits = searchService.search(IndexField.Name, "roger")
        assert(hits.totalHits == 1)
    }

    "Lucene" should "return multiple Hits" in {
        searchService.index(users)

        val hits = searchService.search(IndexField.Categories, "gymrat")
        assert(hits.totalHits == 3)

        val hits2 = searchService.search(IndexField.Ip, "1.2.3.4")
        assert(hits2.totalHits == 2)
    }

    "Lucene" should "return Hits based on LongField" in {
        searchService.index(users)

        val hits = searchService.search(IndexField.Geohash, new GeodataDTO(42, -73).geoHash.toString)
        assert(hits.totalHits == 3)
    }

    "Lucene" should "produce 'more like this' results" in {
        searchService.index(userLocations)
        assert(searchService.numDocs() == 8)

        val doc = searchService.index(new UserLocationDTO("foo", new GeodataDTO(40.750580, -73.993580), "1.2.3.4", new TimeSegment(true, "8")))
        assert(searchService.numDocs() == 9)

        val hit = searchService.search(IndexField.Key, doc.get(IndexField.Key.toString))
        val more2 = searchService.moreLikeThis(hit.scoreDocs.head.doc, List[IndexField](IndexField.Ip, IndexField.Geosector, IndexField.TimeSegment))
        assert(more2.totalHits == 4)

        val more3 = searchService.moreLikeThis(hit.scoreDocs.head.doc, List[IndexField](IndexField.Geosector))
        assert(more3.totalHits == 4)

        val doc2 = searchService.index(new UserLocationDTO("bar", new GeodataDTO(40.750580, -73.993580), "1.2.3.4", new TimeSegment(true, "7")))
        val hits2 = searchService.search(IndexField.Key, doc2.get(IndexField.Key.toString))
        val more4 = searchService.moreLikeThis(hits2.scoreDocs.head.doc, List[IndexField](IndexField.Ip, IndexField.Geosector, IndexField.TimeSegment))
        assert(more4.totalHits == 9)
    }
}