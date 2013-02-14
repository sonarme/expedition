package com.sonar.expedition.common.adx.search.dao

import me.prettyprint.cassandra.serializers.{LongSerializer, BytesArraySerializer, StringSerializer, AbstractSerializer}
import org.joda.time.DateTime
import java.nio.ByteBuffer
import com.sonar.expedition.common.adx.search.model.BidRequestHolder


class BidRequestDao extends BaseDao[String, java.lang.Long, Array[Byte]](
    columnFamily = "BidRequest",
    hashKey = "bidId",
    rangeKey = "timestamp",
    valueAttribute = "data",
    keySerializer = StringSerializer.get,
    nameSerializer = LongSerializer.get,
    valueSerializer = BytesArraySerializer.get) {
    def save(bidRequest: BidRequestHolder) {
        saveValue(bidRequest.userId.toString, bidRequest.timestamp, bidRequest.bidRequest.array())
    }
}

object DateTimeSerializer extends AbstractSerializer[DateTime] {
    def toByteBuffer(obj: DateTime) = throw new RuntimeException("X")

    def fromByteBuffer(byteBuffer: ByteBuffer) = throw new RuntimeException("X")
}
