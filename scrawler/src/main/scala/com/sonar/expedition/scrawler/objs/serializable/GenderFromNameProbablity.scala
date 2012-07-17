package com.sonar.expedition.scrawler.objs.serializable

import GenderFromNameProbablity._
import util.matching.Regex

object GenderFromNameProbablity {

    private var malelist:java.util.Map[String,String] = new java.util.HashMap[String,String]
    private var femalelist:java.util.Map[String,String] = new java.util.HashMap[String,String]
    val firstname: Regex = """([a-zA-Z\d]+)\s*(.*)""".r

}

class GenderFromNameProbablity extends Serializable {

    def addMaleItems(name: String, freq: String) {

        malelist.put(name, freq)
    }

    def addFemaleItems(name: String, freq: String) {

        femalelist.put(name, freq)
    }

    def getGender(name: String): String = {

        val fname=name  match {
            case firstname(firstname,secondnames)=>firstname
            case _ => "0"
        }
        val maleprob = Option(malelist.get(fname)).getOrElse("0").toFloat
        val femaleprob = Option(femalelist.get(fname)).getOrElse("0").toFloat
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


