package com.sonar.expedition.scrawler.util

import org.apache.lucene.analysis.shingle.ShingleFilter
import org.apache.lucene.analysis.tokenattributes.TermAttribute
import org.apache.mahout.classifier.bayes.mapreduce.common.BayesFeatureMapper
import org.apache.mahout.common.iterator.ArrayIterator
import org.apache.mahout.math.map.OpenObjectIntHashMap
import java.util.regex.Pattern

class ShingleTokenizer {
}

object ShingleTokenizer {

    val SPACE_PATTERN = Pattern.compile("[ ]+")

    def shingleize(value: String, gramSize: Int): java.util.List[String] = {

        val tokens = SPACE_PATTERN.split(value)
        val wordList = new OpenObjectIntHashMap[String](tokens.length * gramSize)

        val sf: ShingleFilter = new ShingleFilter(new BayesFeatureMapper.IteratorTokenStream(new ArrayIterator[String](tokens)), gramSize)
        do {
            val term: String = (sf.getAttribute(classOf[TermAttribute])).term
            if (term.length > 0) {
                if (wordList.containsKey(term)) {
                    wordList.put(term, 1 + wordList.get(term))
                }
                else {
                    wordList.put(term, 1)
                }
            }
        } while (sf.incrementToken)
    wordList.keys()
    }

}
