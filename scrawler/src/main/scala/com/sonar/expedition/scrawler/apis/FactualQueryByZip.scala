package com.sonar.expedition.scrawler.apis


import com.twitter.scalding.{RichPipe, Args}
import com.twitter.scalding.TextLine
import FactualQueryByZip._
import com.twitter.scalding.Job

class FactualQueryByZip(args: Args) extends Job(args) {

    val inputData = "/tmp/zipssmall.txt"
    val wrtData = TextLine("/tmp/zipsout.txt")


    val data = (TextLine(inputData).read.project('line).mapTo(('line) -> ('zipcode)) {
        line: String => {
            line match {
                case zipcode(id, zip, state, name, lat, lon, code, dist) => (zip)
                case _ => ("None")
            }
        }
    }).project('zipcode).flatMap('zipcode -> 'name) {
        line: String => getFactualRes(line).split("\\n")
    }.write(wrtData)

    def getFactualRes(zip: String): String = {
        val hp = new HttpClientRest()
        "none"
    }

}

object FactualQueryByZip {
    val zipcode = """(.*),"(.*)",(.*),(.*),(.*),(.*),(.*),(.*)""".r
}
