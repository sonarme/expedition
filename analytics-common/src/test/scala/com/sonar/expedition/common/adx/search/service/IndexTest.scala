package com.sonar.expedition.common.adx.search.service

import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.apache.lucene.store.FSDirectory
import java.io.File
import org.apache.lucene.index.DirectoryReader
import com.sonar.expedition.common.adx.search.model.IndexField

class IndexTest extends FlatSpec with BeforeAndAfter {


    "Lucene" should "be able to read index generated by scalding" in {
        val path = "/develop/expedition/testLucene/part-00000"
        val indexReader = DirectoryReader.open(FSDirectory.open(new File(path)))
        val doc1 = indexReader.document(1)
        val searchService = new SearchService(indexReader)
        val numdocs = searchService.numDocs
        println(numdocs)

        val hits = searchService.search(IndexField.Geosector, "dr5ru4r")
        println(hits.totalHits)
    }

}
