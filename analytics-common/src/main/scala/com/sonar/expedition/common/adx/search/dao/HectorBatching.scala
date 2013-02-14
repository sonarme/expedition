package com.sonar.expedition.common.adx.search.dao

import org.springframework.beans.factory.annotation.Autowired
import me.prettyprint.cassandra.service.spring.{HectorTemplateImpl, HectorTemplate}
import java.nio.ByteBuffer
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.cassandra.serializers.{TypeInferringSerializer, ByteBufferSerializer}
import collection.mutable.ListBuffer
import collection.JavaConversions._
import util.DynamicVariable
import me.prettyprint.hector.api.exceptions.HInvalidRequestException
import grizzled.slf4j.Logging
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model._
import scala.Some

object HectorBatch {
    val ThreadLocal = new ThreadLocal[Batch]
}

trait HectorBatching {
    @Autowired
    var dynamoDBClient: AmazonDynamoDBClient = _

    private def threadLocal = HectorBatch.ThreadLocal

    def currentBatch = Option(threadLocal get)

    /**
     * execute all operations within one batch mutator
     * @param operations
     * @param clock can be used for migrations
     * @return
     */
    def executeBatch[T](operations: => T, clock: Option[Long] = None) = {
        currentBatch match {
            case Some(batch) =>
                operations
            case None =>
                val newBatch = new Batch(dynamoDBClient)
                threadLocal set newBatch
                try {
                    val retVal = operations
                    newBatch execute clock
                    retVal
                } finally {
                    threadLocal remove()
                }
        }
    }


}

class Batch(dynamoDBClient: AmazonDynamoDBClient) extends Logging {

    trait Operation {
        def cf: String

        def key: AttributeValue
    }

    case class Delete(cf: String, key: AttributeValue, name: Option[AttributeValue]) extends Operation

    case class Insert(cf: String, key: AttributeValue, column: PutRequest) extends Operation

    val items = ListBuffer[Operation]()

    def delete(columnFamily: String, key: AttributeValue, name: Option[AttributeValue] = None) {
        items += Delete(columnFamily, key, name)
    }

    def insert(columnFamily: String, key: AttributeValue, column: PutRequest) {
        items += Insert(columnFamily, key, column)
    }

    def execute(clock: Option[Long] = None) {
        if (items.nonEmpty) {
            try {
                val batchedItems = items.map {
                    case Delete(cf, key, None) => cf -> new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key(key)))
                    case Delete(cf, key, Some(name)) =>
                        cf -> new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key(key, name)))
                    case Insert(cf, key, column) => {
                        cf -> new WriteRequest().withPutRequest(column)
                    }
                }.groupBy(_._1).mapValues(_.map(_._2).toSeq: java.util.List[WriteRequest])
                debug("Mutating " + items.size + " items in families " + items.map(_.cf).toSet[String].mkString(","))
                dynamoDBClient.batchWriteItem(new BatchWriteItemRequest().withRequestItems(batchedItems))
                items.clear()
            } catch {
                case hire: HInvalidRequestException =>
                    error("Error saving batch: " + items)
                    throw hire
            }
        }
    }
}

