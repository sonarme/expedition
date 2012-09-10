package com.sonar.expedition.scrawler.util

import org.apache.commons.codec.language._
import scala.transient
import com.sonar.expedition.scrawler.pipes.JobImplicits
import com.twitter.scalding.{FieldConversions, TupleConversions}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.{Attribute, ReaderUtil, Version}
import org.apache.commons.io.IOUtils
import java.io.StringReader

object StemAndMetaphoneEmployer extends Serializable {
    @transient
    val metaphone = new DoubleMetaphone

    @transient
    val metaphoneWithCodeLength = new DoubleMetaphone
    metaphoneWithCodeLength.setMaxCodeLen(6)

    @transient
    def metaphoner(maxCodeLength: Option[Int] = None) = {
        maxCodeLength match {
            case Some(6) => metaphoneWithCodeLength
            case _ => metaphone
        }
    }

    /* removes stop words, punctuation and extra whitespace from a employer string */

    def removeStopWords(employer: String) =
        if (employer == null)
            ""
        else {
            employer
                    .replaceAll( """\.[a-zA-Z][a-zA-Z][a-zA-Z]?(?= |$)""", "")
                    .replaceAll( """\p{P}""", "")
                    .replaceAll( """(^|(?<= ))(?i)(a|an|and|are|as|at|be|but|by|for|if|in|into|is|it|no|not|of|on|or|such|that|the|their|then|there|these|they|this|to|was|will|with|inc|incorporated|co|ltd|llc|group|corp|corporation|company|limited|hq)(?= |$)""", "")
                    .replaceAll( """\s+""", " ")
                    .trim
                    .toLowerCase
        }


    def extractTokens(tokenString: String) = {
        val standardAnalyzer = new StandardAnalyzer(Version.LUCENE_36)
        val tokenStream = standardAnalyzer.tokenStream("textField", new StringReader(tokenString))
        var tokens: Seq[String] = Seq[String]()
        try {
            while (tokenStream.incrementToken()) {
                // getting tokens
            }
            val tokenIterator = tokenStream.getAttributeImplsIterator
            /*tokens = for (token: Attribute <- tokenIterator) yield {
                token.toString
            }*/
        } finally {
            tokenStream.end()
            tokenStream.close()
        }
        tokens
    }

    /* outputs the metaphone encoding of an employer string */

    def getMetaphone(employer: String): String = {
        if (employer == null)
            ""
        else {

            metaphoner(Option(6)).doubleMetaphone(employer)
        }
    }

    /* outputs the alternative metaphone encoding of an employer string */

    def getAlternateMetaphone(employer: String): String = {
        if (employer == null)
            ""
        else {

            metaphoner().doubleMetaphone(employer, true)
        }
    }

    /* outputs the stemmed metaphone encoding of an employer string */

    def getStemmedMetaphone(employer: String): String = {
        if (employer == null)
            ""
        else {

            val stem = removeStopWords(employer)
            metaphoner().doubleMetaphone(stem)

        }
    }

    /* outputs the alternative stemmed metaphone encoding of an employer string */

    def getStemmedAlternateMetaphone(employer: String): String = {
        if (employer == null)
            ""
        else {
            val stem = removeStopWords(employer)
            metaphoner().doubleMetaphone(stem, true)
        }

    }

    /* returns a tuple of the exact employer string, stemmed employer, and both metaphone encodings of the employer */

    def getTuple(employer: String): Tuple4[String, String, String, String] = {
        if (employer == null)
            ("", "", "", "")
        else {

            val stem = removeStopWords(employer)
            val meta1 = getMetaphone(stem)
            val meta2 = getAlternateMetaphone(stem)
            (employer, stem, meta1, meta2)
        }
    }

}
