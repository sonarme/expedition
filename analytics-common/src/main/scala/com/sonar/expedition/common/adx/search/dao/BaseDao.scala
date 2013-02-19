package com.sonar.expedition.common.adx.search.dao

import collection.JavaConversions._
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.serializers._
import org.joda.time.Duration
import com.amazonaws.services.dynamodb.model._

abstract class BaseDao[K, N >: Null, V](val columnFamily: String,
                                        val hashKey: String,
                                        val rangeKey: String,
                                        val valueAttribute: String = "data",
                                        val keySerializer: Serializer[K],
                                        val nameSerializer: Serializer[N],
                                        val valueSerializer: Serializer[V],
                                        val defaultTtl: Option[Duration] = None
                                               ) extends HectorBatching {
    require(defaultTtl map (x => x.getStandardSeconds > 0 && x.getStandardSeconds <= Int.MaxValue) getOrElse (true), "defaultTTL must be convertible to seconds")

    case class Range(from: N, to: N, reverse: Boolean = false, limit: Int = Int.MaxValue)

    protected def getColumn(key: K, columnName: N) = getColumns(key, List(columnName)).values.headOption

    protected def getColumns(key: K, columnNames: Iterable[N]) = {

        getColumnsForKeys(Seq(key), columnNames).values.headOption.getOrElse(Map.empty[N, V])

    }

    protected def getColumnsForKeys(keys: Iterable[K], columnNames: Iterable[N]) = {
        val keyAttributes = keys.map(key => newAttributeValue(key, keySerializer))
        val columnNameAttributes = columnNames.map(columnName => newAttributeValue(columnName, nameSerializer))
        val dynamoKeys = for (keyAttribute <- keyAttributes;
                              columnNameAttribute <- columnNameAttributes) yield new Key(keyAttribute, columnNameAttribute)
        val q = dynamoDBClient.batchGetItem(new BatchGetItemRequest().withRequestItems(Map(columnFamily -> new KeysAndAttributes().withKeys(dynamoKeys))))
        toRowMap(q.getResponses.values().map(_.getItems).flatten)
    }

    def toRowMap(it: Iterable[java.util.Map[String, AttributeValue]]): Map[K, Map[N, V]] =
        it.map(
            item =>
                (fromAttributeValue(item.get(hashKey), keySerializer),
                        (fromAttributeValue(item.get(rangeKey), nameSerializer), fromAttributeValue(item.get(valueAttribute), valueSerializer)))
        ).groupBy(_._1).mapValues(_.map(_._2).toMap[N, V])

    protected def getRow(key: K, range: Option[Range] = None) =
        getRows(List(key), range).values.headOption.getOrElse(Map.empty[N, V])

    protected def getRows(keys: Iterable[K], range: Option[Range] = None) = {
        toRowMap((for (key <- keys) yield {
            val q = new QueryRequest().withHashKeyValue(newAttributeValue(key, keySerializer)).withTableName(columnFamily)
            range match {
                case Some(Range(from, to, reverse, limit)) =>
                    q.withLimit(limit).withScanIndexForward(!reverse).withRangeKeyCondition(new Condition().withAttributeValueList(newAttributeValue(from, nameSerializer), newAttributeValue(to, nameSerializer)).withComparisonOperator(ComparisonOperator.BETWEEN))

                case _ =>
            }
            dynamoDBClient.query(q).getItems
        }).flatten)

    }

    protected def delete(key: K) {
        withBatch {
            _.delete(columnFamily, newAttributeValue(key, keySerializer))
        }
    }

    protected def delete(key: K, name: N) {
        withBatch {
            _.delete(columnFamily, newAttributeValue(key, keySerializer), Some(newAttributeValue(name, nameSerializer)))
        }
    }

    protected def save(key: K, name: N, ttlOption: Option[Duration] = None) {
        doSave(key, name, null, ttlOption)
    }

    // TODO: hack
    def fromAttributeValue[S](attributeValue: AttributeValue, serializer: Serializer[S]) =
        (serializer match {
            case s: StringSerializer => attributeValue.getS
            case n: IntegerSerializer => attributeValue.getN.toInt
            case n: LongSerializer => attributeValue.getN.toLong
            case n: DateSerializer => new java.util.Date(attributeValue.getN.toLong)
            case other => other.fromByteBuffer(attributeValue.getB)
        }).asInstanceOf[S]


    // TODO: hack
    def newAttributeValue[S](any: Any, serializer: Serializer[S]) =
        any match {
            case s: String => new AttributeValue(s)
            case n: Number => new AttributeValue().withN(n.toString)
            case d: java.util.Date => new AttributeValue().withN(d.getTime.toString)
            case v: S => new AttributeValue().withB(serializer.toByteBuffer(v))
            case _ => new AttributeValue()
        }

    protected def doSave(key: K, name: N, value: AttributeValue, ttlOption: Option[Duration] = None) {

        val columnNameAttribute = Map(rangeKey -> newAttributeValue(name, nameSerializer))
        val keyAttribute = newAttributeValue(key, keySerializer)
        val item =
            Map(hashKey -> keyAttribute, valueAttribute -> value) ++ columnNameAttribute

        val column = new PutRequest().withItem(item)

        withBatch {
            _.insert(columnFamily, keyAttribute, column)
        }
    }

    protected def saveValue(key: K, name: N, value: V, ttlOption: Option[Duration] = None) {
        doSave(key, name, newAttributeValue(value, valueSerializer), ttlOption)
    }

    protected def withBatch(op: (Batch) => Unit) {
        executeBatch({
            currentBatch foreach op
        }, None)
    }

}
