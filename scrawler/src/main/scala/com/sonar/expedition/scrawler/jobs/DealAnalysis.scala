package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Job, TextLine, Tsv, Args}
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.`type`.TypeReference
import java.util
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.pipes.PlacesCorrelation.PlaceCorrelationSectorSize
import DealAnalysis._
import reflect.BeanProperty

class DealAnalysis(args: Args) extends Job(args) {
    val dealsInput = args("dealsInput")

    Tsv(dealsInput, ('dealId, 'successfulDeal, 'merchantName, 'majorCategory, 'minorCategory, 'minPricepoint, 'locationJSON)).map(('merchantName, 'locationJSON) ->('stemmedMerchantName, 'lat, 'lng, 'geosector)) {
        in: (String, String) =>
            val (merchantName, locationJSON) = in
            val dealLocations = DealObjectMapper.readValue[util.List[DealLocation]](locationJSON, new TypeReference[util.List[DealLocation]] {})
            val dealLocation = dealLocations.head
            val stemmedMerchantName = StemAndMetaphoneEmployer.getStemmed(merchantName)
            val geohash = GeoHash.withBitPrecision(dealLocation.latitude, dealLocation.longitude, PlaceCorrelationSectorSize)
            (stemmedMerchantName, dealLocation.latitude, dealLocation.longitude, geohash.longValue())
    }.discard('locationJSON).write(TextLine("dealtest"))
}

object DealAnalysis {
    val DealObjectMapper = new ObjectMapper
}

case class DealLocation(
                               @BeanProperty address: String,
                               @BeanProperty city: String = null,
                               @BeanProperty state: String = null,
                               @BeanProperty zip: String = null,
                               @BeanProperty phone: String = null,
                               @BeanProperty latitude: Double = 0,
                               @BeanProperty longitude: Double = 0
                               ) {
    def this() = this(null)
}
