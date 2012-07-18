package com.sonar.expedition.scrawler.pipes

import util.matching.Regex

import GenderFromNameProbablity._

object GenderFromNameProbablity {

    private var malelist: Map[String, Double] = Map.empty
    private var femalelist: Map[String, Double] = Map.empty
    val firstname: Regex = """([a-zA-Z\d]+)\s*(.*)""".r

}


class GenderFromNameProbablity extends Serializable {

    def addMaleItems(name: String, freq: Double) {

        malelist += (name->freq)
    }

    def addFemaleItems(name: String, freq: Double) {

        femalelist+=(name->freq)
    }

    def gender(name: String): String = {

        val fname=name  match {
            case firstname(firstname,secondnames)=>firstname
            case _ => "0"   //firstname always exists, it will come here only if name is null or an empty string in which case "unknown::0.0" will be returned.
        }
        val maleprob = malelist.getOrElse(fname,0.0)
        val femaleprob = femalelist.getOrElse(fname,0.0)

        val prob = maleprob / (maleprob + femaleprob)

        if (maleprob > femaleprob) {
            "male::" + prob
        } else if (maleprob < femaleprob) {
            "female::" + (1 - prob)
        } else {
            "unknown::0.0"
        }

    }
}




