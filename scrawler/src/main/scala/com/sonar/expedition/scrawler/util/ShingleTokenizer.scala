package com.sonar.expedition.scrawler.util

import org.apache.lucene.analysis.shingle.ShingleFilter
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.mahout.classifier.bayes.mapreduce.common.BayesFeatureMapper
import org.apache.mahout.common.iterator.ArrayIterator
import org.apache.mahout.math.map.OpenObjectIntHashMap
import java.util.regex.Pattern

object ShingleTokenizer {

    def shingleize(value: String, gramSize: Int) = {

        val tokens = value.split("\\s+")
        val wordList = new OpenObjectIntHashMap[String](tokens.length * gramSize)

        val sf: ShingleFilter = new ShingleFilter(new BayesFeatureMapper.IteratorTokenStream(new ArrayIterator[String](tokens)), gramSize)
        do {
            val term: String = (sf.getAttribute(classOf[CharTermAttribute])).toString
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
