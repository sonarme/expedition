package com.sonar.expedition.scrawler.pipes

import util.matching.Regex
import com.sonar.dossier.dto.Gender

object GenderFromNameProbability {

    def dataFileMap(file: String) = io.Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().flatMap {
        line =>

            line match {
                case GenderInfoReadPipe.GenderInfo(name, freq, cum_freq, rank) => Some(name -> freq.toDouble /*, cum_freq.toDouble, rank.toInt*/)
                case _ => None
            }
    }.toMap[String, Double]

    private val malelist = dataFileMap("/datafiles/male.txt")
    private val femalelist = dataFileMap("/datafiles/female.txt")
    val splitName = """([a-zA-Z\d]+)\s*(.*)""".r

    @transient
    def gender(name: String) =
        if (name == null) Gender.unknown -> 0.0
        else {
            val firstName = name match {
                case splitName(first, second) => first
                case name => name //firstname always exists, it will come here only if name is null or an empty string in which case "unknown::0.0" will be returned.
            }
            val upperCaseName = firstName.toUpperCase
            val maleprob = malelist.getOrElse(upperCaseName, 0.0)
            val femaleprob = femalelist.getOrElse(upperCaseName, 0.0)

            val prob = maleprob / (maleprob + femaleprob)

            if (maleprob > femaleprob) {
                Gender.male -> prob
            } else if (maleprob < femaleprob) {
                Gender.female -> (1 - prob)
            } else {
                Gender.unknown -> 0.0
            }
        }

}



