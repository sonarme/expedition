package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}

import com.twitter.scalding.{Args}
import util.matching.Regex

import GenderInfoReadPipe._


trait GenderInfoReadPipe extends ScaldingImplicits {
    def DataPipe(malepipe: RichPipe): RichPipe = {

        val pipe = malepipe.project('line).flatMapTo('line ->('name, 'freq, 'cum_freq, 'rank)) {
            line: String => {
                line match {
                    case GenderInfo(name, freq, cum_freq, rank) => Some((name, freq.toDouble, cum_freq.toDouble, rank.toInt))
                    case _ => None
                }
            }
        }


        pipe
    }

}

object GenderInfoReadPipe {
    val GenderInfo: Regex = """([a-zA-Z]+)\s+(\d+.\d+)\s+(\d+.\d+)\s+(\d+)""".r;

}
