package com.sonar.expedition.common.adx.storm

import storm.scala.dsl.StormBolt
import backtype.storm.tuple.Tuple
import com.sonar.expedition.common.adx.search.dao.BidRequestDao
import com.sonar.expedition.common.adx.search.model.{BidRequestHolder, BidRequest}
import com.sonar.expedition.common.avro.AvroSerialization._


class BidRequestWriterBolt extends StormBolt(List.empty[String]) {
    var dao: BidRequestDao = _
    setup {
        dao = new BidRequestDao
    }

    def execute(tuple: Tuple) {
        try {
            tuple matchSeq {
                case Seq(bytes: Array[Byte]) =>
                    dao.save(fromByteArray[BidRequestHolder](bytes))
            }
            tuple ack
        } catch {
            case t: Throwable =>
                _collector.reportError(t)
                _collector.fail(tuple)
        }

    }
}
