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
import org.scala_tools.time.Imports._
import java.util.concurrent.locks.ReentrantReadWriteLock
import com.sonar.expedition.common.serialization.Serialization._
import java.nio.ByteBuffer
import collection.JavaConversions._
import util.Random
import org.openrtb._
import java.util.concurrent.atomic.AtomicInteger
import com.yammer.metrics.scala.Instrumented
import scala.Some

object BidProcessingService extends TimeSegmentation with Instrumented {
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

    val requestTimer = metrics.timer("bidProcessor")

    val MaxAmountSpentHourly = 500 * 100
    val CurrentHourAmountSpent = new AtomicInteger(0)
    val random = new Random
    var epsilon = Float.MaxValue
    val cpm = 52
    val notifyUrl = "http://rtb.sonar.me/notify/win?id=${AUCTION_ID}&bidId=${AUCTION_BID_ID}&impId=${AUCTION_IMP_ID}&seatId=${AUCTION_SEAT_ID}&adId=${AUCTION_AD_ID}&price=${AUCTION_PRICE}&currency=${AUCTION_CURRENCY}"

    def spend(amount: Int) = CurrentHourAmountSpent.addAndGet(amount)

    def addClickThrough(term: BytesRef) =
        clickThroughRate(term).incrementAndGet()

    def processBidRequest(bidRequest: BidRequest, currentTime: Date = new Date): Option[BidResponse] = {
        requestTimer.time {
            BidRequestRules.execute(bidRequest) match {
                case Some(ruleViolation: String) => {
                    log.info("passing on bid. rule violation : " + ruleViolation)
                    None
                }
                case None => {
                    val bidPrice = if (random.nextFloat() <= epsilon) cpm
                    else {

                        val (lat, lng) = (bidRequest.getDevice.getGeo.getLat.toDouble, bidRequest.getDevice.getGeo.getLon.toDouble)

                        // TODO: ugly copy&paste
                        val timeSegment = hourSegment(lat, lng, currentTime)
                        val geosector = GeoHash.withCharacterPrecision(lat, lng, 7).toBase32
                        val timeWindow = JTHours.hoursBetween(new DateTime(0), new DateTime(currentTime)).getHours

                        val more = searchService.moreLikeThis(Map(
                            IndexField.Ip.toString -> bidRequest.getDevice.getIp,
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
                        averageClickThrough * cpm
                    }
                    val bidId = bidRequest.getId //for tracking and debugging. we can probably just use the bidRequest.id since we only handle one impression per bidRequest
                    val impId = bidRequest.getImpList.head.getId //we will always have one impression with a bid (based on the rules we specified in BidRequestRules)
                    val response = new BidResponse(bidId)
                    response.setBidid(bidId)
                    response.setCur("USD")
                    response.setSeatbidList(Seq({
                        val sb = new SeatBid
                        sb.setBidList(Seq({
                            val b = new Bid(bidId, impId, (bidPrice / 100f))
                            b.setAdid("") //ID that references the ad to be served if the bid wins.
                            b.setNurl(notifyUrl) //Win notice URL.
                            b.setAdm("") //Actual XHTML ad markup.
                            b.setAdm("") //Advertiser's primary or top-level domain for advertiser checking.
                            //b.setIurl("") //Sample image URL (without cache busting) for content checking.
                            //b.setCid("") //Campaign ID or similar that appears within the ad markup.
                            //b.setCrid("") //Creative ID for reporting content issues or defects.
                            //b.setAttrList(Seq())
                            b
                        }))
                        sb.setGroup(Bool.FALSE)
                        sb
                    }))


                    Option(response)
                }
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
