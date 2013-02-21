package com.sonar.expedition.common.adx.storm

import storm.scala.dsl.StormBolt
import backtype.storm.tuple.Tuple
import com.sonar.expedition.common.adx.search.dao.BidRequestDao
import com.sonar.expedition.common.serialization.Serialization._
import com.amazonaws.auth.{BasicAWSCredentials, PropertiesCredentials}
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.sonar.dossier.util.StaticApplicationContext
import com.sonar.expedition.common.adx.search.model.BidRequestHolder


class BidRequestWriterBolt extends StormBolt(List.empty[String]) {

    var dao: BidRequestDao = _
    setup {
        dao = new BidRequestDao
        dao.dynamoDBClient = new AmazonDynamoDBClient(
            new BasicAWSCredentials(_conf.get(StaticApplicationContext.AWS_ACCESS_KEY_ID).toString, _conf.get(StaticApplicationContext.AWS_SECRET_KEY).toString))
    }

    def execute(tuple: Tuple) {
        try {
            tuple matchSeq {
                case Seq(bytes: Array[Byte]) =>
                    dao.save(fromByteArray(bytes, new BidRequestHolder()))
            }
            tuple ack
        } catch {
            case t: Throwable =>
                _collector.reportError(t)
                _collector.fail(tuple)
        }

    }
}
