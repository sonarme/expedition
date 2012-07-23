package com.sonar.expedition.scrawler.util


class EmployerCheckinMatch extends Serializable {
    val levver = new Levenshtein
    //metaphoner.setMaxCodeLen(6)
    val maxDistance = 1

    /* checks if the employer stem matches the venueName stem and returns a boolean */

    def checkStemMatch(employer: String, venueName: String): Boolean = {

        StemAndMetaphoneEmployer.removeStopWords(employer).matches(StemAndMetaphoneEmployer.removeStopWords(venueName))
    }

    /* checks if any of the employer stemmed metaphones matches any of the venueName stemmed metaphones and returns a boolean */

    def checkMetaphone(employer: String, venueName: String): Boolean = {

        var bool = false
        if (!employer.matches("") && !venueName.matches("")) {
            val empMet1 = StemAndMetaphoneEmployer.getStemmedMetaphone(employer)
            val empMet2 = StemAndMetaphoneEmployer.getStemmedAlternateMetaphone(employer)
            val venMet1 = StemAndMetaphoneEmployer.getStemmedMetaphone(venueName)
            val venMet2 = StemAndMetaphoneEmployer.getStemmedAlternateMetaphone(venueName)
            bool = empMet1.matches(venMet1) || empMet1.matches(venMet2) || empMet2.matches(venMet1) || empMet2.matches(venMet2)
        }
        bool
    }

    def checkMetaphoneJobType(job: String): String = {
        var bool = false
        if (!job.matches("") && !job.matches("")) {
            val job_type1 = StemAndMetaphoneEmployer.getStemmedMetaphone(job)
            val job_type2 = StemAndMetaphoneEmployer.getStemmedAlternateMetaphone(job)
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
        val emp = StemAndMetaphoneEmployer.removeStopWords(employer)
        val ven = StemAndMetaphoneEmployer.removeStopWords(venueName)

        (levver.compareInt(emp, ven) <= maxDistance) && !emp.matches("") && !ven.matches("")

    }


}

object EmployerCheckinMatch extends Serializable {

}
