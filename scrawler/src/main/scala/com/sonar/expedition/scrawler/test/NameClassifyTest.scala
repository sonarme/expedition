/*
package com.sonar.expedition.scrawler.test

import com.twitter.scalding.{Job, Args, TextLine}
import javax.print.attribute.standard.JobSheets
import com.sonar.expedition.scrawler.pipes.GenderFromNameProbablity
import util.matching.Regex
import NameClassifyTest._
import com.sonar.expedition.scrawler.objs.serializable.GenderFromNameProbablity

class NameClassifyTest(args: Args) extends Job(args){

    var genderprob: GenderFromNameProbablity = new  GenderFromNameProbablity()

    val malepipe = TextLine("/tmp/male.txt")

    val malegender = (malepipe.read.project('line).mapTo('line->('name, 'freq, 'cum_freq, 'rank)){
        line: String => {
            line match {
                case GenderInfo(name, freq, cum_freq, rank) =>  (name, freq, cum_freq, rank)
                case _ =>  ("None", 0, 0, -1)
            }
        }
    })
   .filter('name, 'freq, 'cum_freq, 'rank){
        fields : (String, String,String, String) =>
            val (name, freq, cum_freq, rank) = fields
            (rank != "-1")

    }

            val mpipe=malegender.project(('name, 'freq)).mapTo(('name, 'freq)->('name1, 'freq1)){
        fields: (String, String) =>
        val (name, freq) = fields
        genderprob.addMaleItems(name.toUpperCase,freq)
        (name,freq)
    }.write(TextLine("/tmp/gen1.txt"))



    val femalepipe = TextLine("/tmp/female.txt")

    val femalegender = (femalepipe.read.project('line).mapTo('line->('name, 'freq, 'cum_freq, 'rank)){
        line: String => {
            line match {
                case GenderInfo(name, freq, cum_freq, rank) => (name, freq, cum_freq, rank)
                case _ => ("None", 0, 0, -1)
            }
        }
    })
    .filter('name, 'freq, 'cum_freq, 'rank){
        fields : (String, String,String, String) =>
            val (name, freq, cum_freq, rank) = fields
            (rank != "-1")

    }

   val fpipe = femalegender.project(('name, 'freq)).mapTo(('name, 'freq)->('name1, 'freq1)){
            fields: (String, String) =>
            val (name, freq) = fields
            genderprob.addFemaleItems(name.toUpperCase,freq)
            (name, freq)
    }.write(TextLine("/tmp/gen2.txt"))



    val test = malegender.++(femalegender).project('name).mapTo('name->('name1,'gender)){
        name:String =>
            val gender = genderprob.getGender(name.toUpperCase)
            (name,gender)
    }.project('name1,'gender).write(TextLine("/tmp/genderoutput.txt"))
}

object  NameClassifyTest{
    val GenderInfo:Regex= """([a-zA-Z]+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+)""".r;
}
*/
