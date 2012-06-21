package com.sonar.expedition.scrawler

import util.matching.Regex
import com.sonar.expedition.scrawler.StemAndMetaphoneEmployer._
import org.apache.commons.codec.language._

class StemAndMetaphoneEmployer {


    def removeStopWords(employer: String) : String = employer.replaceAll("""\.[a-zA-Z][a-zA-Z][a-zA-Z]?(?= |$)""","").replaceAll("""\p{P}""","").replaceAll("""(^|(?<= ))(?i)(a|an|and|are|as|at|be|but|by|for|if|in|into|is|it|no|not|of|on|or|such|that|the|their|then|there|these|they|this|to|was|will|with|inc|incorporated|co|ltd|llc|group|corp|corporation|company|limited)(?= |$)""", "").replaceAll("""\s+"""," ").replaceFirst("""\s*""","").replaceFirst(" $","")

    def getMetaphone(employer: String) : String = {
        val metaphoner = new DoubleMetaphone
        metaphoner.doubleMetaphone(employer)
    }

    def getAlternateMetaphone(employer: String) : String = {
        val altMetaphoner = new DoubleMetaphone
        altMetaphoner.doubleMetaphone(employer, true)
    }

    def getStemmedMetaphone(employer: String) : String = {
        val stem = removeStopWords(employer)
        val metaphoner = new DoubleMetaphone
        metaphoner.doubleMetaphone(stem)
    }

    def getStemmedAlternateMetaphone(employer: String) : String = {
        val stem = removeStopWords(employer)
        val altMetaphoner = new DoubleMetaphone
        altMetaphoner.doubleMetaphone(stem, true)
    }

    def getTuple(employer: String) : Tuple4[String, String, String, String] = {
        val stem = removeStopWords(employer)
        val meta1 = getMetaphone(stem)
        val meta2 = getAlternateMetaphone(stem)
        (employer, stem, meta1, meta2)
    }

}

object StemAndMetaphoneEmployer {

}
