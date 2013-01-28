package com.sonar.expedition.scrawler.util

import org.apache.commons.codec.language._
import scala.transient
import com.sonar.expedition.scrawler.pipes.JobImplicits
import com.twitter.scalding.{FieldConversions, TupleConversions}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.{Attribute, Version}
import org.apache.commons.io.IOUtils
import java.io.StringReader
import collection._
import collection.JavaConversions._
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute}
import scala.Predef._
import scala.Tuple4
import scala.Some
import collection.Set
import org.apache.lucene.analysis.util.CharArraySet

object StemAndMetaphoneEmployer extends Serializable {
    @transient
    val metaphone = new DoubleMetaphone

    @transient
    val metaphoneWithCodeLength = new DoubleMetaphone
    metaphoneWithCodeLength.setMaxCodeLen(6)
    val StopWords = StandardAnalyzer.STOP_WORDS_SET ++ Set("inc", "incorporated", "co", "ltd", "llc", "group", "corp", "corporation", "company", "limited", "hq")

    @transient
    def metaphoner(maxCodeLength: Option[Int] = None) = {
        maxCodeLength match {
            case Some(6) => metaphoneWithCodeLength
            case _ => metaphone
        }
    }

    /* removes stop words, punctuation and extra whitespace from a employer string */

    def removeStopWords(employer: String) = extractTokens(employer).mkString(" ")

    def extractTokens(tokenString: String): Seq[String] =
        if (tokenString == null) Seq.empty[String]
        else {
            val tokenStream = new StandardAnalyzer(Version.LUCENE_41, new CharArraySet(Version.LUCENE_41, StopWords, true)).tokenStream("textField", new StringReader(tokenString))
            try {
                val tokens = mutable.ListBuffer[String]()
                while (tokenStream.incrementToken()) {
                    tokens += tokenStream.getAttribute(classOf[CharTermAttribute]).toString
                }
                tokens
            } finally {
                tokenStream.end()
                tokenStream.close()
            }
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
