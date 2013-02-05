package com.sonar.expedition.common.adx.search.service

import java.io.File

import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.index._
import com.sonar.expedition.common.adx.search.model._

import grizzled.slf4j.Logging
import org.slf4j.LoggerFactory
import com.sonar.expedition.common.segmentation.TimeSegmentation
import java.util.Date
import ch.hsr.geohash.GeoHash
import org.joda.time.DateTime
import org.joda.time.{Hours => JTHours}
import com.sonar.expedition.common.adx.search.model.Bid
import com.sonar.expedition.common.adx.search.model.SeatBid
import com.sonar.expedition.common.adx.search.model.BidRequest
import com.sonar.expedition.common.adx.search.model.BidResponse
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.util.BytesRef
import com.sonar.expedition.common.adx.search.rules.BidRequestRules

object BidProcessingService extends TimeSegmentation {
    val log = LoggerFactory.getLogger("application")

    def multiDirectorySearchService(base: String) = {
        val directories = new File(base).listFiles().filter(_.isDirectory)
        log.info("Using directories: " + directories.mkString(", "))
        new SearchService(new MultiReader(directories.map(dir => DirectoryReader.open(new MMapDirectory(dir))): Array[IndexReader], true), null)
    }

    // TODO: ugly
    lazy val searchService = multiDirectorySearchService("/media/ephemeral0/data")
    lazy val searchServiceCohorts = multiDirectorySearchService("/media/ephemeral0/dataCohorts")
    // TODO: shutdown

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


    def processBidRequest(bidRequest: BidRequest, currentTime: Date = new Date) = {
        BidRequestRules.execute(bidRequest) match {
            case Some(ruleViolation: String) => {
                log.info("passing on bid. rule violation : " + ruleViolation)
                BidResponse("id", "bidid", 0, "cur", 1, List(SeatBid("seat", 1, List(Bid("impid", "0.00", "adid", "http://www.sonar.me/adserver/morelikethis", "adm", "sonar.me", "http://sonar.me/adserver/iurl", "cid", "crid", List())))))
            }
            case None => {
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
                ).filterNot(_ == null))

                val docTerms = (more.scoreDocs map {
                    scoreDoc: ScoreDoc => scoreDoc -> searchServiceCohorts.terms(scoreDoc.doc, IndexField.SocialCohort)
                }).toMap[ScoreDoc, Iterable[TermsEnum]]

                val termDocs = (for ((scoreDoc, terms) <- docTerms.toSeq; term <- terms) yield term -> scoreDoc).groupBy(_._1).toMap.mapValues(_.map(_._2))

                val docFreqs = docTerms.values.flatten.toSet.map {
                    term: TermsEnum => term.term() -> searchServiceCohorts.docFreq(new Term(IndexField.SocialCohort.toString, term.term()))
                }.toMap[BytesRef, Int]

                val totalScore = termDocs.map {
                    case (termEnum, docs) =>
                        docs.map(_.score).sum / docFreqs(termEnum.term)
                }.sum



                val serviceIds = more.scoreDocs.map {
                    case topDoc =>
                        val doc = searchService.doc(topDoc.doc)
                        doc.getField(IndexField.ServiceId.toString).stringValue()
                }
                log.info("doc serviceIds: " + serviceIds.mkString(", "))
                log.info("score : " + totalScore)
                BidResponse("id", "bidid", more.totalHits, "cur", 1, List(SeatBid("seat", 1, List(Bid("impid", "1.00", "adid", "http://www.sonar.me/adserver/morelikethis", "adm", "sonar.me", "http://sonar.me/adserver/iurl", "cid", "crid", List("attr1", "attr2"))))))
            }
        }
    }

}
