package com.sonar.expedition.scrawler

import util.matching.Regex
import com.sonar.expedition.scrawler.StemAndMetaphoneEmployer._
import org.apache.commons.codec.language._

class StemAndMetaphoneEmployer {

    /* removes stop words, punctuation and extra whitespace from a employer string */

    def removeStopWords(employer: String) : String = employer.replaceAll("""\.[a-zA-Z][a-zA-Z][a-zA-Z]?(?= |$)""","").replaceAll("""\p{P}""","").replaceAll("""(^|(?<= ))(?i)(a|an|and|are|as|at|be|but|by|for|if|in|into|is|it|no|not|of|on|or|such|that|the|their|then|there|these|they|this|to|was|will|with|inc|incorporated|co|ltd|llc|group|corp|corporation|company|limited)(?= |$)""", "").replaceAll("""\s+"""," ").replaceFirst("""\s*""","").replaceFirst(" $","").toLowerCase

    /* outputs the metaphone encoding of an employer string */

    def getMetaphone(employer: String) : String = {
        val metaphoner = new DoubleMetaphone
        metaphoner.doubleMetaphone(employer)
    }

    /* outputs the alternative metaphone encoding of an employer string */

    def getAlternateMetaphone(employer: String) : String = {
        val altMetaphoner = new DoubleMetaphone
        altMetaphoner.doubleMetaphone(employer, true)
    }

    /* outputs the stemmed metaphone encoding of an employer string */

    def getStemmedMetaphone(employer: String) : String = {
        val stem = removeStopWords(employer)
        val metaphoner = new DoubleMetaphone
        metaphoner.doubleMetaphone(stem)
    }

    /* outputs the alternative stemmed metaphone encoding of an employer string */

    def getStemmedAlternateMetaphone(employer: String) : String = {
        val stem = removeStopWords(employer)
        val altMetaphoner = new DoubleMetaphone
        altMetaphoner.doubleMetaphone(stem, true)
    }

    /* returns a tuple of the exact employer string, stemmed employer, and both metaphone encodings of the employer */

    def getTuple(employer: String) : Tuple4[String, String, String, String] = {
        val stem = removeStopWords(employer)
        val meta1 = getMetaphone(stem)
        val meta2 = getAlternateMetaphone(stem)
        (employer, stem, meta1, meta2)
    }

}

object StemAndMetaphoneEmployer {

}
