package com.sonar.expedition.scrawler.service

import com.sonar.expedition.scrawler.dto.indexable.IndexField
import com.sonar.expedition.scrawler.dto.indexable.IndexField._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import com.sonar.expedition.scrawler.dto.indexable.UserDTO
import com.sonar.dossier.dto.GeodataDTO

class SearchTest extends FlatSpec with BeforeAndAfter {
    var searchService: SearchService = _

    val users = List[UserDTO](
        new UserDTO("roger", "roger", "roger lives in jersey city", List[String]("gymrat, workaholic"), new GeodataDTO(42, -73), "1.2.3.4"),
        new UserDTO("paul", "paul", "paul poops all day n night", List[String]("gymrat", "serial_eater"), new GeodataDTO(42.22, -73.1), "2.2.3.4"),
        new UserDTO("ben", "ben", "ben lives in manhattan", List[String](), new GeodataDTO(42.3, -73), "1.2.3.4"),
        new UserDTO("katie", "katie", "katie loves owls", List[String](), new GeodataDTO(42, -73), "4.2.3.4"),
        new UserDTO("brett", "brett", "brett has a place in maryland", List[String]("workaholic", "entrepreneur"), new GeodataDTO(42.3, -73), "1.2.34.4"),
        new UserDTO("ximena", "ximena", "ximena dwells on staten island", List[String]("social_butterfly", "gymrat"), new GeodataDTO(42, -73), "1.2.34.4")
    )

    before {
        searchService = new SearchServiceImpl
    }

    "Lucene" should "index a User" in {
        searchService.index(users.take(1))
        assert(searchService.numDocs() == 1)
    }

    "Lucene" should "return 1 Hit" in {
        searchService.index(users)

        val hits = searchService.search(IndexField.Content, "roger")
        assert(hits.totalHits == 1)
    }

    "Lucene" should "return multiple Hits" in {
        searchService.index(users)

        val hits = searchService.search(IndexField.Content, "lives")
        assert(hits.totalHits == 2)

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

        val hits = searchService.search(IndexField.Content, "roger")

        val more = searchService.moreLikeThis(hits.scoreDocs.head.doc, List[IndexField](IndexField.Content))
        assert(more.totalHits == 2)

        val more2 = searchService.moreLikeThis(hits.scoreDocs.head.doc, List[IndexField](IndexField.Name))
        assert(more2.totalHits == 1)

        val more3 = searchService.moreLikeThis(hits.scoreDocs.head.doc, List[IndexField](IndexField.Categories))
        assert(more3.totalHits == 4)

        val more4 = searchService.moreLikeThis(hits.scoreDocs.head.doc)
        assert(more4.totalHits == 5)
    }
}