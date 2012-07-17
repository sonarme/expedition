package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding._

import com.twitter.scalding.{Job, Args}
import util.matching.Regex

import  GenderInfoReadPipe._

class GenderInfoReadPipe(args: Args) extends Job(args) {
    def DataPipe(malepipe: RichPipe): RichPipe = {

       val pipe = malepipe.project('line).mapTo('line->('name, 'freq, 'cum_freq, 'rank)){
            line: String => {
                line match {
                    case GenderInfo(name, freq, cum_freq, rank) =>  (name, freq, cum_freq, rank)
                    case _ =>  ("None", 0, 0, -1)
                }
            }
        }
         .filter('name, 'freq, 'cum_freq, 'rank){
            fields : (String, String,String, String) =>
                val (name, freq, cum_freq, rank) = fields
                (rank != "-1")

        }.project('name, 'freq, 'cum_freq, 'rank)

        pipe
    }

}
object  GenderInfoReadPipe{
       val GenderInfo:Regex= """([a-zA-Z]+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+)""".r;

}
