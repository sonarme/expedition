package com.sonar.expedition.scrawler

import util.matching.Regex
import com.sonar.expedition.scrawler.StemAndMetaphoneEmployer
import org.apache.commons.codec.language._

class EmployerCheckinMatch {

    /* checks if the employer stem matches the venueName stem and returns a boolean */

    def checkStemMatch(employer: String, venueName: String): Boolean = {
        val stemmer = new StemAndMetaphoneEmployer
        stemmer.removeStopWords(employer).matches(stemmer.removeStopWords(venueName))
    }

    /* checks if any of the employer stemmed metaphones matches any of the venueName stemmed metaphones and returns a boolean */

    def checkMetaphone(employer: String, venueName: String): Boolean = {
        val metaphoner = new StemAndMetaphoneEmployer
        var bool = false
        if (!employer.matches("") && !venueName.matches("")){
        val empMet1 = metaphoner.getStemmedMetaphone(employer)
        val empMet2 = metaphoner.getStemmedAlternateMetaphone(employer)
        val venMet1 = metaphoner.getStemmedMetaphone(venueName)
        val venMet2 = metaphoner.getStemmedAlternateMetaphone(venueName)
        bool = empMet1.matches(venMet1) || empMet1.matches(venMet2)  || empMet2.matches(venMet1) || empMet2.matches(venMet2)}
        bool
    }

}

object EmployerCheckinMatch {

}
