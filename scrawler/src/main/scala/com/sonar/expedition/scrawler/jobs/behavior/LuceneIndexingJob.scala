package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding._
import com.sonar.expedition.scrawler.service.SearchServiceImpl
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.dto.indexable.{IndexField, UserDTO}
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import com.sonar.dossier.dto.{GeodataDTO, LocationDTO, ServiceType, ServiceVenueDTO}
import com.sonar.expedition.scrawler.jobs.DefaultJob
import org.apache.lucene.store.FSDirectory
import java.io.File

class LuceneIndexingJob(args: Args) extends DefaultJob(args) {
//    val userCategories = args("userCategories")

    val userCategories = IterableSource(Seq(
    ("roger", Seq("gymrat", "foodie")),
    ("katie", Seq("entrepreneur"))
    ), Tuples.Behavior.UserCategories)
    /*
    val userCategories = IterableSource(Seq(
            ("katie", Seq("foodie")),
            ("roger", Seq("entrepreneur", "gymrat"))
        ), Tuples.Behavior.UserCategories)
    */
    //todo: do join to get other data
    userCategories.read
//    SequenceFile(userCategories, Tuples.Behavior.UserCategories).read
        .map(('userGoldenId, 'categories) -> ('indexed)) {
        fields: (String, List[String]) => {
            val (userGoldenId, categories) = fields
            val user = new UserDTO(userGoldenId, categories, null, null)
            val searchService = new SearchServiceImpl(FSDirectory.open(new File(args("index"))), true)

            searchService.index(user)
//            searchService.search(IndexField.Categories, "gymrat")
            "success"
        }
    }.write(Tsv(args("output"), Tuples.Behavior.UserCategories append ('indexed)))
}