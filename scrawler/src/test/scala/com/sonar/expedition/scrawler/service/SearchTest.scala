package com.sonar.expedition.scrawler.service

import com.sonar.expedition.scrawler.dto.indexable.IndexField
import com.sonar.expedition.scrawler.dto.indexable.IndexField._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import com.sonar.expedition.scrawler.dto.indexable.UserDTO
import com.sonar.dossier.dto.GeodataDTO

class SearchTest extends FlatSpec with BeforeAndAfter {
    var searchService: SearchService = _

    val users = List[UserDTO](
        new UserDTO("roger", "roger", List[String]("gymrat, workaholic"), new GeodataDTO(42, -73), "1.2.3.4"),
        new UserDTO("paul", "paul", List[String]("gymrat", "serial_eater"), new GeodataDTO(42.22, -73.1), "2.2.3.4"),
        new UserDTO("ben", "ben", List[String](), new GeodataDTO(42.3, -73), "1.2.3.4"),
        new UserDTO("katie", "katie", List[String](), new GeodataDTO(42, -73), "4.2.3.4"),
        new UserDTO("brett", "brett", List[String]("workaholic", "entrepreneur"), new GeodataDTO(42.3, -73), "1.2.34.4"),
        new UserDTO("ximena", "ximena", List[String]("social_butterfly", "gymrat"), new GeodataDTO(42, -73), "1.2.34.4"),
        new UserDTO("daniel", "daniel", List[String](), null, null)
    )

    before {
        searchService = new SearchServiceImpl("index", true)
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
        searchService.index(users)

        val hits = searchService.search(IndexField.Name, "roger")

        val more2 = searchService.moreLikeThis(hits.scoreDocs.head.doc, List[IndexField](IndexField.Name))
        assert(more2.totalHits == 0)

        val more3 = searchService.moreLikeThis(hits.scoreDocs.head.doc, List[IndexField](IndexField.Categories))
        assert(more3.totalHits == 3)

        val more4 = searchService.moreLikeThis(hits.scoreDocs.head.doc)
        assert(more4.totalHits == 4)
    }
}