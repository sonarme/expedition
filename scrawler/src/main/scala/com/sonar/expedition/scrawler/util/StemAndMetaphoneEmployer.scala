package com.sonar.expedition.scrawler.util

import org.apache.commons.codec.language._

class StemAndMetaphoneEmployer(defaultMetaphoner: DoubleMetaphone = null) extends Serializable {

    val metaphoner = if (Option(defaultMetaphoner).isDefined) defaultMetaphoner else new DoubleMetaphone

    /* changes max code length of metaphoner */
    def setMaxCodeLen(length: Int) {
        metaphoner.setMaxCodeLen(length)
    }

    /* removes stop words, punctuation and extra whitespace from a employer string */

    def removeStopWords(employer: String): String = employer.replaceAll( """\.[a-zA-Z][a-zA-Z][a-zA-Z]?(?= |$)""", "").replaceAll( """\p{P}""", "").replaceAll( """(^|(?<= ))(?i)(a|an|and|are|as|at|be|but|by|for|if|in|into|is|it|no|not|of|on|or|such|that|the|their|then|there|these|they|this|to|was|will|with|inc|incorporated|co|ltd|llc|group|corp|corporation|company|limited|hq)(?= |$)""", "").replaceAll( """\s+""", " ").replaceFirst( """\s*""", "").replaceFirst(" $", "").toLowerCase

    /* outputs the metaphone encoding of an employer string */

    def getMetaphone(employer: String): String = {

        metaphoner.setMaxCodeLen(6)
        metaphoner.doubleMetaphone(employer)
    }

    /* outputs the alternative metaphone encoding of an employer string */

    def getAlternateMetaphone(employer: String): String = {
        metaphoner.doubleMetaphone(employer, true)
    }

    /* outputs the stemmed metaphone encoding of an employer string */

    def getStemmedMetaphone(employer: String): String = {
        val stem = removeStopWords(employer)
        metaphoner.doubleMetaphone(stem)
    }

    /* outputs the alternative stemmed metaphone encoding of an employer string */

    def getStemmedAlternateMetaphone(employer: String): String = {
        val stem = removeStopWords(employer)
        metaphoner.doubleMetaphone(stem, true)
    }

    /* returns a tuple of the exact employer string, stemmed employer, and both metaphone encodings of the employer */

    def getTuple(employer: String): Tuple4[String, String, String, String] = {
        val stem = removeStopWords(employer)
        val meta1 = getMetaphone(stem)
        val meta2 = getAlternateMetaphone(stem)
        (employer, stem, meta1, meta2)
    }

}

object StemAndMetaphoneEmployer {

}
