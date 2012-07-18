package com.sonar.expedition.scrawler.dto

import com.twitter.scalding.{TextLine, RichPipe, Job, Args}
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper

class JsonParserDTO (args: Args) extends Job(args) {
    def importData(dataPath: String): RichPipe = {
        val data = TextLine(dataPath).read.project('line).mapTo('line -> 'json) {
            fields: String =>
                val line = fields
                val parsed: Option[ServiceProfileDTO] = Option(ScrawlerObjectMapper.mapper().readValue(line, classOf[ServiceProfileDTO]))
                parsed

        }

        data
    }
}


