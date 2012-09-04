package com.sonar.expedition.scrawler.util


object EmployerCheckinMatch extends Serializable {

    //metaphoner.setMaxCodeLen(6)
    val MaxDistance = 1

    /* checks if the employer stem matches the venueName stem and returns a boolean */

    def checkStemMatch(employer: String, venueName: String) = StemAndMetaphoneEmployer.removeStopWords(employer) == StemAndMetaphoneEmployer.removeStopWords(venueName)

    /* checks if any of the employer stemmed metaphones matches any of the venueName stemmed metaphones and returns a boolean */

    def checkMetaphone(employer: String, venueName: String) = {
        if (employer != "" && venueName != "") {
            val empMet1 = StemAndMetaphoneEmployer.getStemmedMetaphone(employer)
            val empMet2 = StemAndMetaphoneEmployer.getStemmedAlternateMetaphone(employer)
            val venMet1 = StemAndMetaphoneEmployer.getStemmedMetaphone(venueName)
            val venMet2 = StemAndMetaphoneEmployer.getStemmedAlternateMetaphone(venueName)
            empMet1 == venMet1 || empMet1 == venMet2 || empMet2 == venMet1 || empMet2 == venMet2
        } else false
    }

    def checkMetaphoneJobType(job: String): String = {
        if (job.isEmpty) job
        else {
            val job_type1 = StemAndMetaphoneEmployer.getStemmedMetaphone(job)
            val job_type2 = StemAndMetaphoneEmployer.getStemmedAlternateMetaphone(job)
            if (job_type1 == job_type2)
                job_type2
            else
                job
        }
    }

    /* checks if the employer and venue stems are within a Levenshtein distance of one. will return false for null strings */
    def checkLevenshtein(employer: String, venueName: String) = {
        val emp = StemAndMetaphoneEmployer.removeStopWords(employer)
        val ven = StemAndMetaphoneEmployer.removeStopWords(venueName)
        emp != "" && ven != "" && Levenshtein.compareInt(emp, ven) <= MaxDistance

    }


}
