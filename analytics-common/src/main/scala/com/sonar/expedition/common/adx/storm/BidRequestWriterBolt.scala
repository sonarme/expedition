package com.sonar.expedition.common.adx.storm

import storm.scala.dsl.StormBolt
import backtype.storm.tuple.Tuple
import com.sonar.expedition.common.adx.search.dao.{BidNotificationDao, BiddingDao, BidRequestDao}
import com.sonar.expedition.common.serialization.Serialization._
import com.amazonaws.auth.{BasicAWSCredentials, PropertiesCredentials}
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.sonar.dossier.util.StaticApplicationContext
import com.sonar.expedition.common.adx.search.model.{BidNotification, BidRequestHolder}
import org.openrtb.BidRequest
import com.dyuproject.protostuff.Message


class BidNotificationBolt extends BiddingBolt[BidNotification, BidNotificationDao] {
    def instance = new BidNotification

    lazy val dao = new BidNotificationDao
}

class BidRequestBolt extends BiddingBolt[BidRequestHolder, BidRequestDao] {
    def instance = new BidRequestHolder

    lazy val dao = new BidRequestDao
}

abstract class BiddingBolt[T <: Message[T], Q <: BiddingDao[T]] extends StormBolt(List.empty[String]) {
    def instance: T

    def dao: Q

    setup {
        dao.dynamoDBClient = new AmazonDynamoDBClient(
            new BasicAWSCredentials(_conf.get(StaticApplicationContext.AWS_ACCESS_KEY_ID).toString, _conf.get(StaticApplicationContext.AWS_SECRET_KEY).toString))
    }

    def execute(tuple: Tuple) {
        try {
            tuple matchSeq {
                case Seq(bytes: Array[Byte]) =>
                    dao.save(fromByteArray(bytes, instance))
            }
            tuple ack
        } catch {
            case t: Throwable =>
                _collector.fail(tuple)
                _collector.reportError(t)
        }

    }
}
