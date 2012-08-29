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
import com.twitter.scalding._
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.twitter.scalding.SequenceFile
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.TextLine
import com.sonar.scalding.cassandra.NarrowRowScheme
import cascading.pipe.Pipe


// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222

/*
com.sonar.expedition.scrawler.jobs.CompetitorAnalysisForPlaces --hdfs --checkinData "/tmp/checkinDatatest.txt"
--rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193 --places "/tmp/places_dump_US.geojson.txt" --checkinDatawithVenueId "/tmp/checkinDatawithVenueId.txt"
 */

class CompetitorAnalysisForPlaces(args: Args) extends Job(args) with LocationBehaviourAnalysePipe with PlacesCorrelation with CheckinSource {

    val bayestrainingmodel = args("bayestrainingmodelforvenuetype")

    val competitiveAnalysisOutput = args.getOrElse("competitiveAnalysisOutput", "s3n://scrawler/competitiveAnalysisOutput")
    val placesData = args("placesData")

    val checkins = checkinSource(args, true)
    val checkinsWithExtra = correlationCheckinsFromCassandra(checkins)
    val allCheckins = checkinsWithExtra.project('keyid, 'venId)
    val placesVenueGoldenId =
        placeClassification(checkins, bayestrainingmodel, placesData)
                .unique('goldenId, 'venueId)
                .joinWithLarger('venueId -> 'venId, allCheckins)
                .project('keyid, 'goldenId)

    //module: end of detrmining places type from venue name
    // keyid is servicetype:serviceprofileid
    val similarity = placesVenueGoldenId.groupBy('keyid, 'goldenId) {
        _.size
    }.rename('size -> 'rating).project('keyid, 'goldenId, 'rating)

    val numRaters = similarity
            .groupBy('goldenId) {
        _.size
    }.rename('size -> 'numRaters)

    //joining based on golden id really does not work , its not perfect.
    val ratingsWithSize = similarity.joinWithSmaller('goldenId -> 'goldenId, numRaters)


    val ratings2 =
        ratingsWithSize
                .rename(('keyid, 'goldenId, 'rating, 'numRaters) ->('keyid2, 'goldenId2, 'rating2, 'numRaters2))


    val ratingPairs =
        ratingsWithSize
                .joinWithSmaller('keyid -> 'keyid2, ratings2)
                // De-dupe so that we don't calculate similarity of both (A, B) and (B, A).
                .filter('goldenId, 'goldenId2) {
            venues: (String, String) => venues._1 < venues._2
        }
                .project('goldenId, 'rating, 'numRaters, 'goldenId2, 'rating2, 'numRaters2)


    val vectorCalcs =
        ratingPairs
                // Compute (x*y, x^2, y^2), which we need for dot products and norms.
                .map(('rating, 'rating2) ->('ratingProd, 'ratingSq, 'rating2Sq)) {
            ratings: (Double, Double) =>
                (ratings._1 * ratings._2, math.pow(ratings._1, 2), math.pow(ratings._2, 2))
        }
                .groupBy('goldenId, 'goldenId2) {
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
                val priorCount = 10
                val priorCorrelation = 0

                val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
                val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, priorCount, priorCorrelation)
                val cosSim = cosineSimilarity(dotProduct, math.sqrt(ratingNormSq), math.sqrt(rating2NormSq))
                val jaccard = jaccardSimilarity(size, numRaters, numRaters2)

                (corr, regCorr, cosSim, jaccard)
        }
                //can also calculate correlation, 'regularizedCorrelation, 'cosineSimilarity
                .project('goldenId, 'goldenId2, 'jaccardSimilarity)
                .write(
            SequenceFile(competitiveAnalysisOutput, Fields.ALL))


    def correlation(size: Double, dotProduct: Double, ratingSum: Double,
                    rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double) = {

        val numerator = size * dotProduct - ratingSum * rating2Sum
        val denominator = math.sqrt(size * ratingNormSq - ratingSum * ratingSum) * math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

        numerator / denominator
    }

    def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double) = usersInCommon / (totalUsers1 + totalUsers2 - usersInCommon)

    def cosineSimilarity(dotProduct: Double, ratingNorm: Double, rating2Norm: Double) = dotProduct / (ratingNorm * rating2Norm)

    def regularizedCorrelation(size: Double, dotProduct: Double, ratingSum: Double,
                               rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double,
                               virtualCount: Double, priorCorrelation: Double) = {

        val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        val w = size / (size + virtualCount)

        w * unregularizedCorrelation + (1 - w) * priorCorrelation
    }


}

