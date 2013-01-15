package com.sonar.expedition.scrawler.service

import com.sonar.expedition.scrawler.dto.indexable.IndexField._
import org.apache.lucene.search.{Query, IndexSearcher, TopDocs}
import com.sonar.expedition.scrawler.dto.indexable.UserDTO

trait SearchService {
    def index(user: UserDTO)
    def index(users: Seq[UserDTO])
    def search(field: IndexField, queryStr: String): TopDocs
    def moreLikeThis(docNum: Int, moreLikeThisfields: List[IndexField]): TopDocs
    def moreLikeThis(docNum: Int): TopDocs
    def explain(indexSearcher: IndexSearcher, query: Query, hits: TopDocs)
    def numDocs(): Int
}