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

object BidProcessingService extends TimeSegmentation {
    val log = LoggerFactory.getLogger("application")
    lazy val searchService = {
        //        val base = "/develop/expedition/Checkins0118_idx3"
        val base = "/media/ephemeral0/data"
        val directories = new File(base).listFiles().filter(_.isDirectory)
        log.info("Using directories: " + directories.mkString(", "))
        new SearchService(new MultiReader(directories.map(dir => DirectoryReader.open(new MMapDirectory(dir))): Array[IndexReader], true), null)
    }
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
        val Array(lat, lng) = bidRequest.device.loc.toString.split("\\s*,\\s*").map(_.toDouble)

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
            scoreDoc: ScoreDoc => scoreDoc -> searchService.terms(scoreDoc.doc, IndexField.SocialCohort)
        }).toMap[ScoreDoc, Iterable[TermsEnum]]

        val docFreqs = docTerms.values.flatten.toSet.map {
            term: TermsEnum => term.term() -> searchService.docFreq(new Term(IndexField.SocialCohort.toString, term.term()))
        }.toMap[BytesRef, Int]

        val totalScore = docTerms.map {
            case (scoreDoc, termEnums) =>
                scoreDoc.score * termEnums.map(termEnum => docFreqs(termEnum.term)).sum
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