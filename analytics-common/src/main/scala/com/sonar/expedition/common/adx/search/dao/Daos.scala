package com.sonar.expedition.common.adx.search.dao

import me.prettyprint.cassandra.serializers.{LongSerializer, BytesArraySerializer, StringSerializer, AbstractSerializer}
import org.joda.time.DateTime
import java.nio.ByteBuffer
import com.sonar.expedition.common.adx.search.model.BidRequestHolder
import com.sonar.expedition.common.serialization.Serialization._

class BidRequestDao extends BaseDao[String, java.lang.Long, String](
    columnFamily = "BidRequest",
    hashKey = "userId",
    rangeKey = "timestamp",
    valueAttribute = "data",
    keySerializer = StringSerializer.get,
    nameSerializer = LongSerializer.get,
    valueSerializer = StringSerializer.get) {
    def save(bidRequest: BidRequestHolder) {
        saveValue(bidRequest.getUserId, bidRequest.getTimestamp, new String(toByteArray(bidRequest.getBidRequest), "UTF-8"))
    }
}

object DateTimeSerializer extends AbstractSerializer[DateTime] {
    def toByteBuffer(obj: DateTime) = throw new RuntimeException("X")

    def fromByteBuffer(byteBuffer: ByteBuffer) = throw new RuntimeException("X")
}