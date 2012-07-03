package com.sonar.expedition.scrawler.pipes

import util.matching.Regex
import com.sonar.expedition.scrawler.clustering.{StemAndMetaphoneEmployer, Levenshtein}
import com.sonar.expedition.scrawler.clustering.{Levenshtein, StemAndMetaphoneEmployer}
import org.apache.commons.codec.language._

class EmployerCheckinMatch {
    val metaphoner = new StemAndMetaphoneEmployer
    val levver = new Levenshtein
    //metaphoner.setMaxCodeLen(6)
    val maxDistance = 1

    /* checks if the employer stem matches the venueName stem and returns a boolean */

    def checkStemMatch(employer: String, venueName: String): Boolean = {

        metaphoner.removeStopWords(employer).matches(metaphoner.removeStopWords(venueName))
    }

    /* checks if any of the employer stemmed metaphones matches any of the venueName stemmed metaphones and returns a boolean */

    def checkMetaphone(employer: String, venueName: String): Boolean = {

        var bool = false
        if (!employer.matches("") && !venueName.matches("")) {
            val empMet1 = metaphoner.getStemmedMetaphone(employer)
            val empMet2 = metaphoner.getStemmedAlternateMetaphone(employer)
            val venMet1 = metaphoner.getStemmedMetaphone(venueName)
            val venMet2 = metaphoner.getStemmedAlternateMetaphone(venueName)
            bool = empMet1.matches(venMet1) || empMet1.matches(venMet2) || empMet2.matches(venMet1) || empMet2.matches(venMet2)
        }
        bool
    }

    def checkMetaphoneJobType(job: String): String = {
        var bool = false
        if (!job.matches("") && !job.matches("")) {
            val job_type1 = metaphoner.getStemmedMetaphone(job)
            val job_type2 = metaphoner.getStemmedAlternateMetaphone(job)
            bool = job_type1.matches(job_type2)
            if (bool)
                job_type2
            else {
                job
            }
        } else {
            job
        }
    }

    /* checks if the employer and venue stems are within a Levenshtein distance of one. will return false for null strings */
    def checkLevenshtein(employer: String, venueName: String): Boolean = {
        val emp = metaphoner.removeStopWords(employer)
        val ven = metaphoner.removeStopWords(venueName)

        (levver.compareInt(emp, ven) <= maxDistance) && !emp.matches("") && !ven.matches("")

    }


}

object EmployerCheckinMatch {

}
