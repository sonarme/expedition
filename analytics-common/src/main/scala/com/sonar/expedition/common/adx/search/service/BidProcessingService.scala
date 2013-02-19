package com.sonar.expedition.common.adx.search.service

import java.io.File

import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.index._
import com.sonar.expedition.common.adx.search.model._

import org.slf4j.LoggerFactory
import com.sonar.expedition.common.segmentation.TimeSegmentation
import java.util.Date
import ch.hsr.geohash.GeoHash
import org.joda.time.{Hours => JTHours}
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.util.BytesRef
import com.sonar.expedition.common.adx.search.rules.BidRequestRules
import com.sonar.expedition.common.adx.search.model.Bid
import com.sonar.expedition.common.adx.search.model.SeatBid
import com.sonar.expedition.common.adx.search.model.BidRequest
import com.sonar.expedition.common.adx.search.model.BidResponse
import org.scala_tools.time.Imports._
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.producer.{Producer => KafkaProducer, ProducerData, ProducerConfig}
import kafka.serializer.Encoder
import kafka.message.Message
import com.sonar.expedition.common.avro.AvroSerialization._
import java.nio.ByteBuffer
import collection.JavaConversions._
import util.Random

object BidProcessingService extends TimeSegmentation {
    val log = LoggerFactory.getLogger("application")
    val clickThroughRate = Map[BytesRef, CTR]() withDefaultValue (new CTR)

    def multiDirectorySearchService(base: String) = {
        val directories = new File(base).listFiles().filter(_.isDirectory)
        log.info("Using directories: " + directories.mkString(", "))
        new SearchService(new MultiReader(directories.map(dir => DirectoryReader.open(new MMapDirectory(dir))): Array[IndexReader], true), null)
    }

    // TODO: ugly
    lazy val searchService = multiDirectorySearchService("/media/ephemeral0/data")
    lazy val searchServiceCohorts = multiDirectorySearchService("/media/ephemeral0/dataCohorts")
    // TODO: shutdown

    val MaxAmountSpentHourly = 10.0f
    var CurrentHourAmountSpent = 0.0f
    val random = new Random
    var epsilon = 0.3f
    val cpc = 1


    val producer = {
        val props = new java.util.Properties
        props.put("zk.connect", "107.22.18.19:2181")
        props.put("serializer.class", classOf[AvroEncoder].getCanonicalName)
        // gzip (kafka 0.8+ will support snappy)
        // props.put("compression.codec", "1")
        // writes in memory until either batch.size or queue.time is reached
        props.put("producer.type", "async")
        // maximum time, in milliseconds, for buffering data on the producer queue. After it elapses, the buffered data in the producer queue is dispatched to the event.handler.
        props.put("queue.time", "5000")
        // the maximum size of the blocking queue for buffering on the kafka.producer.AsyncProducer
        props.put("queue.size", "10000")
        // the number of messages batched at the producer, before being dispatched to the event.handler
        props.put("batch.size", "200")
        new KafkaProducer[String, BidRequestHolder](new ProducerConfig(props))
        //todo: producer.close
    }


    def setCurrentHourAmount(amount: Float) {
        CurrentHourAmountSpent = amount
    }

    /*

      implicit def avroJsonUnmarshaller[T](bidRequest: String) = {
          bidRequest match {
              case x: String =>
                  //                val schema = Schema.parse()
                  val reader = new SpecificDatumReader(classOf[BidRequest])
                  val dataStreamReader = new DataFileReader[BidRequest](new SeekableByteArrayInput(x.getBytes("UTF-8")), reader)
                  var bidRequest: Option[BidRequest] = None
                  while (dataStreamReader.hasNext()) {
                      // Reuse user object by passing it to next(). This saves us from
                      // allocating and garbage collecting many objects for files with
                      // many items.
                      bidRequest = Option(dataStreamReader.next())
                  }
                  bidRequest.getOrElse(throw new RuntimeException("BidRequest not found"))
          }
      }


      implicit def avroJsonMarshaller(bidResponse: BidResponse) = {
          val writer = new SpecificDatumWriter[BidResponse](classOf[BidResponse])
          val dataStreamWriter = new DataFileWriter[BidResponse](writer)
          val os = new ByteArrayOutputStream()
          dataStreamWriter.create(bidResponse.getSchema, os)
          dataStreamWriter.append(bidResponse)
          dataStreamWriter.flush()
          dataStreamWriter.close()
          os.toString("utf-8")
      }
  */
    def addClickThrough(term: BytesRef) =
        clickThroughRate(term).incrementAndGet()

    val testr = org.openrtb.mobile.BidRequest.newBuilder().setId("id").setAt(null).setTmax(null).setImp(Seq.empty[org.openrtb.mobile.BidImpression]).setApp(null).setDevice(null).setRestrictions(null).setSite(null).setUser(org.openrtb.mobile.User.newBuilder().setUid("1").setYob(null).setGender(null).setZip(null).setCountry(null).setKeywords(null).build()).build()

    def sendBidRequestToQueue(bidRequest: org.openrtb.mobile.BidRequest) {
        producer.send(new ProducerData[String, BidRequestHolder]("bidRequests_prod", new BidRequestHolder(bidRequest.user.uid, ByteBuffer.wrap(toByteArray(bidRequest)), DateTime.now.millis)))

    }

    def processBidRequest(bidRequest: BidRequest, currentTime: Date = new Date): Option[BidResponse] = {
        BidRequestRules.execute(bidRequest) match {
            case Some(ruleViolation: String) => {
                log.info("passing on bid. rule violation : " + ruleViolation)
                None
            }
            case None => {
                val bidPrice = if (random.nextFloat() <= epsilon) cpc
                else {

                    val lat = bidRequest.device.geo.lat
                    val lng = bidRequest.device.geo.lon

                    // TODO: ugly copy&paste
                    val timeSegment = hourSegment(lat, lng, currentTime)
                    val geosector = GeoHash.withCharacterPrecision(lat, lng, 7).toBase32
                    val timeWindow = JTHours.hoursBetween(new DateTime(0), new DateTime(currentTime)).getHours

                    val more = searchService.moreLikeThis(Map(
                        IndexField.Ip.toString -> bidRequest.device.ip.toString,
                        IndexField.Geosector.toString -> geosector,
                        IndexField.GeosectorTimesegment.toString -> (geosector + ":" + timeSegment.toIndexableString),
                        IndexField.GeosectorTimewindow.toString -> (geosector + ":" + timeWindow)
                    ).filterNot(_._2 == null))

                    val docTerms = (more.scoreDocs map {
                        scoreDoc: ScoreDoc => scoreDoc -> searchServiceCohorts.terms(scoreDoc.doc, IndexField.SocialCohort)
                    }).toMap[ScoreDoc, Iterable[BytesRef]]

                    val termDocs = (for ((scoreDoc, terms) <- docTerms.toSeq; term <- terms) yield term -> scoreDoc).groupBy(_._1).toMap.mapValues(_.map(_._2))

                    val docFreqs = docTerms.values.flatten.toSet.map {
                        term: BytesRef => term -> searchServiceCohorts.docFreq(new Term(IndexField.SocialCohort.toString, term))
                    }.toMap[BytesRef, Int]

                    val termScores = termDocs.map {
                        case (term, docs) =>
                            term -> docs.map(_.score).sum / docFreqs(term)
                    }
                    val sumTermScores = termScores.values.sum
                    val normalizedTermScores = termScores.mapValues(_ / sumTermScores)
                    val averageClickThrough = normalizedTermScores.map {
                        case (termEnum, normalizedScore) =>
                            clickThroughRate(termEnum).get() * normalizedScore
                    }.sum / normalizedTermScores.size


                    /* val serviceIds = more.scoreDocs.map {
                         case topDoc =>
                             val doc = searchService.doc(topDoc.doc)
                             doc.getField(IndexField.ServiceId.toString).stringValue()
                     }
                     log.info("doc serviceIds: " + serviceIds.mkString(", "))
                     log.info("score : " + totalScore)*/
                    assert(averageClickThrough <= 1f)
                    averageClickThrough * cpc * 1000
                }
                val notifyUrl = "http://sonar.me/notify/win?id=${AUCTION_ID}&bidId=${AUCTION_BID_ID}&impId=${AUCTION_IMP_ID}&seatId=${AUCTION_SEAT_ID}&adId=${AUCTION_AD_ID}&price=${AUCTION_PRICE}&currency=${AUCTION_CURRENCY}"
                val bidId = bidRequest.id //for tracking and debugging. we can probably just use the bidRequest.id since we only handle one impression per bidRequest
                val impId = bidRequest.imp.head.id //we will always have one impression with a bid (based on the rules we specified in BidRequestRules)
                Option(BidResponse(bidId, List(SeatBid(List(Bid(bidId, impId, bidPrice, nurl = notifyUrl, adm = "<html>ad markup</html>"))))))
            }
        }
    }

}

class CTR {
    var current = 1f
    var count = 0
    val reward = 1
    val rwLock = new ReentrantReadWriteLock()

    def get() = {
        rwLock.readLock().lock()
        try {
            current
        } finally {
            rwLock.readLock().unlock()
        }
    }

    /**
     * P_a(t + 1) = P_a(t) + (r(t) - P_a(t)) / (selections[a] + 1)
     * @return
     */
    def incrementAndGet() = {
        rwLock.writeLock().lock()
        try {
            count += 1
            current += (reward - current) / count
            current
        } finally {
            rwLock.writeLock().unlock()
        }
    }
}

class AvroEncoder extends Encoder[Any] {

    def toMessage(data: Any) =
        new Message(toByteArray(data))

}
