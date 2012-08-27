package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.pipes.{LocationBehaviourAnalysePipe, DTOPlacesInfoPipe, PlacesCorrelation, CheckinGrouperFunction}
import com.sonar.dossier.dto.CompetitiveVenue
import com.sonar.dossier.dao.cassandra.{JSONSerializer, CompetitiveVenueColumn, CompetitiveVenueColumnSerializer}
import com.sonar.scalding.cassandra.{NarrowRowScheme, WideRowScheme, CassandraSource}
import cascading.tuple.Fields
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{DoubleSerializer, LongSerializer, DateSerializer, StringSerializer}
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.expedition.scrawler.util.Haversine
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.twitter.scalding.{RichDate, Args, SequenceFile, TextLine}
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme


// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222

/*
com.sonar.expedition.scrawler.jobs.CompetitorAnalysisForPlaces --hdfs --checkinData "/tmp/checkinDatatest.txt"
--rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193 --places "/tmp/places_dump_US.geojson.txt" --checkinDatawithVenueId "/tmp/checkinDatawithVenueId.txt"
 */

class CompetitorAnalysisForPlaces(args: Args) extends Job(args) with LocationBehaviourAnalysePipe with PlacesCorrelation with CheckinGrouperFunction {

    val rpcHostArg = args("rpcHost")

    val ppmap = args.getOrElse("ppmap", "")

    val bayestrainingmodel = args("bayestrainingmodelforlocationtype")

    val competitiveAnalysisOutput = args.getOrElse("competitiveAnalysisOutput", "s3n://scrawler/competitiveAnalysisOutput")

    val DEFAULT_NO_DATE = RichDate(0L)
    val NONE_VALUE = "none"

    val checkinsInputPipe = CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier_ben",
        columnFamilyName = "Checkin",
        scheme = NarrowRowScheme(keyField = 'serviceCheckinIdBuffer,
            nameFields = ('userProfileIdBuffer, 'serTypeBuffer, 'serProfileIDBuffer, 'serCheckinIDBuffer,
                    'venNameBuffer, 'venAddressBuffer, 'venIdBuffer, 'chknTimeBuffer,
                    'ghashBuffer, 'latBuffer, 'lngBuffer, 'msgBuffer),
            columnNames = List("userProfileId", "serviceType", "serviceProfileId",
                "serviceCheckinId", "venueName", "venueAddress",
                "venueId", "checkinTime", "geohash", "latitude",
                "longitude", "message"))
    ).map(('serviceCheckinIdBuffer, 'userProfileIdBuffer, 'serTypeBuffer, 'serProfileIDBuffer, 'serCheckinIDBuffer,
            'venNameBuffer, 'venAddressBuffer, 'venIdBuffer, 'chknTimeBuffer,
            'ghashBuffer, 'latBuffer, 'lngBuffer, 'msgBuffer) ->('serviceCheckinId, 'userProfileId, 'serType, 'serProfileID, 'serCheckinID,
            'venName, 'venAddress, 'venId, 'chknTime, 'ghash, 'lat, 'lng, 'msg)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer,
                ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer) => {
            val rowKeyDes = StringSerializer.get().fromByteBuffer(in._1)
            val keyId = Option(in._2).map(StringSerializer.get().fromByteBuffer).getOrElse("missingKeyId")
            val serType = Option(in._3).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val serProfileID = Option(in._4).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val serCheckinID = Option(in._5).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val venName = Option(in._6).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val venAddress = Option(in._7).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val venId = Option(in._8).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)
            val chknTime = Option(in._9).map(DateSerializer.get().fromByteBuffer).getOrElse(DEFAULT_NO_DATE)
            val ghash = Option(in._10).map(LongSerializer.get().fromByteBuffer).orNull
            val lat: Double = Option(in._11).map(DoubleSerializer.get().fromByteBuffer).orNull
            val lng: Double = Option(in._12).map(DoubleSerializer.get().fromByteBuffer).orNull
            val msg = Option(in._13).map(StringSerializer.get().fromByteBuffer).getOrElse(NONE_VALUE)

            (rowKeyDes, keyId, serType, serProfileID, serCheckinID,
                    venName, venAddress, venId, chknTime, ghash, lat, lng, msg)
        }
    }


    val newCheckins = correlationCheckinsFromCassandra(checkinsInputPipe)
    val placesVenueGoldenIdValues = withGoldenId(newCheckins)
    //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)

    // module : start of determining places type from place name

    val placesClassified = classifyPlaceType(bayestrainingmodel, placesVenueGoldenIdValues)
            //.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName,'venTypeFromModel, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
            .mapTo(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
            ->('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)) {
        fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>

            (fields._1, fields._2, fields._3, fields._4, fields._5, fields._6, "", fields._7, fields._8, fields._9, fields._10, fields._11, fields._12, fields._13, fields._14, fields._15, fields._16)

    }


    val placesData = args("placesData")

    val placesPipe = getLocationInfo(TextLine(placesData).read)
            .project(('geometryLatitude, 'geometryLongitude, 'propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory))


    val placesVenueGoldenId = placesVenueGoldenIdValues.leftJoinWithSmaller('venName -> 'propertiesName, placesPipe)
            .project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'classifiersCategory, 'geometryLatitude, 'geometryLongitude, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
            .mapTo(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'classifiersCategory, 'geometryLatitude, 'geometryLongitude, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
            ->
            ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'geometryLatitude, 'geometryLongitude, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId, 'distance)) {

        fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>
            val distance = {
                if (fields._7 != null && fields._8 != null && fields._11 != null && fields._12 != null) {
                    Haversine.haversine(fields._7.toDouble, fields._8.toDouble, fields._11.toDouble, fields._12.toDouble)
                }
                else {
                    -1
                }
            }
            (fields._1, fields._2, fields._3, fields._4, fields._5, "", fields._6, fields._7, fields._8, fields._9, fields._10, fields._11, fields._12, fields._13, fields._14, fields._15, fields._16, fields._17, fields._18, distance)

    }
            .groupBy('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId) {
        _.min('distance)
    }.filter('distance) {
        distance: String => distance != "-1"
    }.discard('distance)
            .++(placesClassified)
            .mapTo(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venTypeFromModel, 'venTypeFromPlacesData, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)
            ->
            ('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venueType, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)) {
        fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) =>

            (fields._1, fields._2, fields._3, fields._4, fields._5, getVenueType(fields._6, fields._7), fields._8, fields._9, fields._10, fields._11, fields._12, fields._13, fields._14, fields._15, fields._16, fields._17)

    }.project('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venueType, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour, 'goldenId, 'venueId)


    //module: end of detrmining places type from venue name

    val similarity = placesVenueGoldenId.groupBy('keyid, 'venName, 'venueType, 'goldenId) {
        _.size
    }.rename('size -> 'rating).project('keyid, 'venName, 'venueType, 'goldenId, 'rating)

    val numRaters = similarity
            .groupBy('goldenId) {
        _.size
    }.rename('size -> 'numRaters)

    //joining based on golden id really does not work , its not perfect.
    val ratingsWithSize = similarity.joinWithSmaller('goldenId -> 'goldenId, numRaters)


    val ratings2 =
        ratingsWithSize
                .rename(('keyid, 'venName, 'venueType, 'goldenId, 'rating, 'numRaters) ->('keyid2, 'venName2, 'venueType2, 'goldenId2, 'rating2, 'numRaters2))


    val ratingPairs =
        ratingsWithSize
                .joinWithSmaller('keyid -> 'keyid2, ratings2)
                // De-dupe so that we don't calculate similarity of both (A, B) and (B, A).
                .filter('venName, 'venName2) {
            venues: (String, String) => venues._1 < venues._2
        }
                .project('venName, 'venueType, 'goldenId, 'rating, 'numRaters, 'venName2, 'venueType2, 'goldenId2, 'rating2, 'numRaters2)


    val vectorCalcs =
        ratingPairs
                // Compute (x*y, x^2, y^2), which we need for dot products and norms.
                .map(('rating, 'rating2) ->('ratingProd, 'ratingSq, 'rating2Sq)) {
            ratings: (Double, Double) =>
                (ratings._1 * ratings._2, math.pow(ratings._1, 2), math.pow(ratings._2, 2))
        }
                .groupBy('venName, 'venueType, 'goldenId, 'venName2, 'venueType2, 'goldenId2) {
            group =>
                group.size // length of each vector
                        .sum('ratingProd -> 'dotProduct)
                        .sum('rating -> 'ratingSum)
                        .sum('rating2 -> 'rating2Sum)
                        .sum('ratingSq -> 'ratingNormSq)
                        .sum('rating2Sq -> 'rating2NormSq)
                        .max('numRaters) // Just an easy way to make sure the numRaters field stays.
                        .max('numRaters2)
        }

    val similarities =
        vectorCalcs
                .map(('size, 'dotProduct, 'ratingSum, 'rating2Sum, 'ratingNormSq, 'rating2NormSq, 'numRaters, 'numRaters2) ->
                ('correlation, 'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity)) {

            fields: (Double, Double, Double, Double, Double, Double, Double, Double) =>

                val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields
                val PRIOR_COUNT = 10
                val PRIOR_CORRELATION = 0

                val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
                val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
                val cosSim = cosineSimilarity(dotProduct, math.sqrt(ratingNormSq), math.sqrt(rating2NormSq))
                val jaccard = jaccardSimilarity(size, numRaters, numRaters2)

                (corr, regCorr, cosSim, jaccard)
        }
                //can also calculate correlation, 'regularizedCorrelation, 'cosineSimilarity
                .project(('venName, 'venueType, 'goldenId, 'venName2, 'venueType2, 'goldenId2, 'jaccardSimilarity))
                .write(
            SequenceFile(competitiveAnalysisOutput, Fields.ALL))


    def correlation(size: Double, dotProduct: Double, ratingSum: Double,
                    rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double): Double = {

        val numerator = size * dotProduct - ratingSum * rating2Sum
        val denominator = math.sqrt(size * ratingNormSq - ratingSum * ratingSum) * math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

        numerator / denominator
    }

    def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double) = {
        val union = totalUsers1 + totalUsers2 - usersInCommon
        usersInCommon / union
    }

    def cosineSimilarity(dotProduct: Double, ratingNorm: Double, rating2Norm: Double) = {
        dotProduct / (ratingNorm * rating2Norm)
    }

    def regularizedCorrelation(size: Double, dotProduct: Double, ratingSum: Double,
                               rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double,
                               virtualCount: Double, priorCorrelation: Double) = {

        val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        val w = size / (size + virtualCount)

        w * unregularizedCorrelation + (1 - w) * priorCorrelation
    }

    def getVenueType(venue1: String, venue2: String): String = {

        if (venue2 != null || venue2 != "")
            venue2
        else
            venue1
    }

}

