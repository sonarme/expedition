package com.sonar.expedition.common.adx.search.dao

import me.prettyprint.cassandra.serializers.{LongSerializer, BytesArraySerializer, StringSerializer, AbstractSerializer}
import org.joda.time.DateTime
import java.nio.ByteBuffer
import com.sonar.expedition.common.adx.search.model.{ClickThrough, BidNotification, BidRequestHolder}
import com.sonar.expedition.common.serialization.Serialization._
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.dyuproject.protostuff.Message


trait BiddingDao[T <: Message[T]] {
    var dynamoDBClient: AmazonDynamoDBClient

    def save(t: T)
}

class BidRequestDao extends BaseDao[String, java.lang.Long, String](
    columnFamily = "BidRequest",
    hashKey = "userId",
    rangeKey = "timestamp",
    valueAttribute = "data",
    keySerializer = StringSerializer.get,
    nameSerializer = LongSerializer.get,
    valueSerializer = StringSerializer.get) with BiddingDao[BidRequestHolder] {
    def save(entity: BidRequestHolder) {
        saveValue(entity.getUserId, entity.getTimestamp, new String(toByteArray(entity.getBidRequest), "UTF-8"))
    }
}


class BidNotificationDao extends BaseDao[String, java.lang.Long, String](
    columnFamily = "BidNotification",
    hashKey = "bidId",
    rangeKey = "timestamp",
    valueAttribute = "data",
    keySerializer = StringSerializer.get,
    nameSerializer = LongSerializer.get,
    valueSerializer = StringSerializer.get) with BiddingDao[BidNotification] {
    def save(entity: BidNotification) {
        saveValue(entity.getBidId, entity.getTimestamp, new String(toByteArray(entity), "UTF-8"))
    }
}


class ClickThroughDao extends BaseDao[String, java.lang.Long, String](
    columnFamily = "ClickThrough",
    hashKey = "bidId",
    rangeKey = "timestamp",
    valueAttribute = "data",
    keySerializer = StringSerializer.get,
    nameSerializer = LongSerializer.get,
    valueSerializer = StringSerializer.get) with BiddingDao[ClickThrough] {
    def save(entity: ClickThrough) {
        saveValue(entity.getBidId, entity.getTimestamp, new String(toByteArray(entity), "UTF-8"))
    }
}

