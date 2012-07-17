package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, TextLine}
import util.matching.Regex
import GenderFromNameProbablity._
import com.mongodb.util.Hash
import collection.immutable.HashMap

object GenderFromNameProbablity {

    private var malelist: java.util.Map[String, String] = new java.util.HashMap[String, String]
    private var femalelist: java.util.Map[String, String] = new java.util.HashMap[String, String]

}

class GenderFromNameProbablity extends Serializable {

    def addMaleItems(name: String, freq: String) {

        malelist.put(name, freq)
    }

    def addFemaleItems(name: String, freq: String) {

        femalelist.put(name, freq)
    }

    def getGender(name: String): String = {

        val maleprob = Option(malelist.get(name)).getOrElse("0").toFloat
        val femaleprob = Option(femalelist.get(name)).getOrElse("0").toFloat
        val prob = maleprob / (maleprob + femaleprob)

        println(name + ", " + maleprob + malelist.containsKey(name) + malelist.size());

        if (maleprob > femaleprob) {
            "male " + prob
        } else if (maleprob < femaleprob) {
            "female " + (1 - prob)
        } else {
            "unknown 0.0"
        }

    }

}


