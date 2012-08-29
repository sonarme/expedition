package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.pipes.{LocationBehaviourAnalysePipe, PlacesCorrelation}
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.SequenceFile
import java.text.SimpleDateFormat
import ch.hsr.geohash.GeoHash
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.SequenceFile
import scala.Some
import com.twitter.scalding.TextLine
import com.sonar.scalding.cassandra.{NarrowRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import com.sonar.expedition.scrawler.util.CommonFunctions

class PlaceClassification(args: Args) extends Job(args) with PlacesCorrelation with CheckinSource {

    val bayestrainingmodel = args("bayestrainingmodelforlocationtype")
    val placesData = args("placesData")
    val output = args("placesOutput")

    val checkinsInputPipe = checkinSource(args, withVenuesOnly = true)

    val placesVenueGoldenId = placeClassification(checkinsInputPipe, bayestrainingmodel, placesData)

            .write(Tsv(output, Fields.ALL))


}

