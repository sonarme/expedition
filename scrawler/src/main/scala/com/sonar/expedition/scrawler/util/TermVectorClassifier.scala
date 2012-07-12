package com.sonar.expedition.scrawler.util

import org.apache.lucene.index._
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.TopDocs
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.spans.SpanTermQuery
import org.apache.lucene.util.Version
import java.io.IOException
import java.util.LinkedHashMap
import java.util.List
import java.util.ArrayList

/**
 * This class is for demonstration purposes only.  No warranty, guarantee, etc. is implied.
 *

 * This is not production quality code!
 */
object TermVectorClassifier {
    def main(args: Array[String]) {
        val ramDir: RAMDirectory = new RAMDirectory
        var DOCS: Array[String] = Array("The quick red fox jumped over the lazy brown dogs.", "Mary had a little lamb whose fleece was white as snow.", "Moby Dick is a story of a whale and a man obsessed.", "The robber wore a black flece jacket and a baseball cap.", "The English Springer Spaniel is the best of all dogs.")

        val writer: IndexWriter = new IndexWriter(ramDir, new StandardAnalyzer(Version.LUCENE_30), true, IndexWriter.MaxFieldLength.UNLIMITED)
        for (i <- Range(0, DOCS.length)) {
            val doc: Document = new Document
            val id: Field = new Field("id", "doc_" + i, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS)
            doc.add(id)
            val text: Field = new Field("content", DOCS(i), Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS)
            doc.add(text)
            writer.addDocument(doc)
        }

        writer.close()
        val searcher: IndexSearcher = new IndexSearcher(ramDir)
        val fleeceQ: SpanTermQuery = new SpanTermQuery(new Term("content", "brown dogs"))
        val results: TopDocs = searcher.search(fleeceQ, 10)
        for (i <- Range(0, results.scoreDocs.length)) {
            val scoreDoc: ScoreDoc = results.scoreDocs(i)
            System.out.println("Score Doc: " + scoreDoc)
        }

    }

}

class WindowTermVectorMapper extends TermVectorMapper {
    def map(term: String, frequency: Int, offsets: Array[TermVectorOffsetInfo], positions: Array[Int]) {
        var entry:WindowEntry = null
        for (i <- Range(0, positions.length) if (positions(i) >= start && positions(i) < end)) {
            if (entry == null) {
                entry = new WindowEntry(term)
                entries.put(term, entry)
            }
            entry.positions.add(positions(i))
        }
    }

    def setExpectations(field: String, numTerms: Int, storeOffsets: Boolean, storePositions: Boolean) {
    }

    private[util] var start: Int = 0
    private[util] var end: Int = 0
    private[util] var entries: LinkedHashMap[String, WindowEntry] = new LinkedHashMap[String, WindowEntry]
}

class WindowEntry {
    private[util] def this(term: String) {
        this()
        this.term = term
    }

    override def toString: String = {
        return "WindowEntry{" + "term='" + term + '\'' + ", positions=" + positions + '}'
    }

    private[util] var term: String = null
    private[util] var positions: List[Integer] = new ArrayList[Integer]
}