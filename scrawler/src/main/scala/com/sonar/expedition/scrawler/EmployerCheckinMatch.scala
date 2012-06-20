package com.sonar.expedition.scrawler

import util.matching.Regex
import com.sonar.expedition.scrawler.StemAndMetaphoneEmployer
import org.apache.commons.codec.language._

class EmployerCheckinMatch {

    def checkStemMatch(employer: String, venueName: String): Boolean = {
        val stemmer = new StemAndMetaphoneEmployer
        stemmer.removeStopWords(employer).matches(stemmer.removeStopWords(venueName))
    }

    def checkMeta

}

object EmployerCheckinMatch {

}
