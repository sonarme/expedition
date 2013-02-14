package com.sonar.expedition.common.adx.storm

import storm.scala.dsl.StormBolt
import backtype.storm.tuple.Tuple
import com.sonar.expedition.common.adx.search.dao.BidRequestDao
import com.sonar.expedition.common.adx.search.model.{BidRequestHolder, BidRequest}

class BidRequestWriterBolt extends StormBolt(List.empty[String]) {
    var dao: BidRequestDao = _
    setup {
        dao = new BidRequestDao
    }

    def execute(tuple: Tuple) {
        try {
            tuple matchSeq {
                case Seq(bidRequest: BidRequestHolder) => dao.save(bidRequest)
            }
            tuple ack
        } catch {
            case t: Throwable =>
                _collector.reportError(t)
                _collector.fail(tuple)
        }

    }
}
