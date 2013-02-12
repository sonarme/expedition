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
import scala.Some
import com.sonar.expedition.common.adx.search.model.Bid
import com.sonar.expedition.common.adx.search.model.SeatBid
import com.sonar.expedition.common.adx.search.model.BidRequest
import com.sonar.expedition.common.adx.search.model.BidResponse
import org.scala_tools.time.Imports._
import com.sonar.expedition.common.util.AtomicFloat
import util.Random

object BidProcessingService extends TimeSegmentation {
    val log = LoggerFactory.getLogger("application")
    val clickThroughRate = Map[BytesRef, AtomicFloat]() withDefaultValue (new AtomicFloat(1))

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
                            clickThroughRate(termEnum).get * normalizedScore
                    }.sum / normalizedTermScores.size


                    /* val serviceIds = more.scoreDocs.map {
                         case topDoc =>
                             val doc = searchService.doc(topDoc.doc)
                             doc.getField(IndexField.ServiceId.toString).stringValue()
                     }
                     log.info("doc serviceIds: " + serviceIds.mkString(", "))
                     log.info("score : " + totalScore)*/
                    assert(averageClickThrough <= 1f)
                    averageClickThrough * cpc
                }
                val notifyUrl = "http://sonar.me/notify/win?id=${AUCTION_ID}&bidId=${AUCTION_BID_ID}&impId=${AUCTION_IMP_ID}&seatId=${AUCTION_SEAT_ID}&adId=${AUCTION_AD_ID}&price=${AUCTION_PRICE}&currency=${AUCTION_CURRENCY}"
                val bidId = bidRequest.id //for tracking and debugging. we can probably just use the bidRequest.id since we only handle one impression per bidRequest
                val impId = bidRequest.imp.head.id //we will always have one impression with a bid (based on the rules we specified in BidRequestRules)
                Option(BidResponse(bidId, List(SeatBid(List(Bid(bidId, impId, bidPrice, nurl = notifyUrl, adm = "<html>ad markup</html>"))))))
            }
        }
    }

}
