package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.CheckinGrouperFunction
import com.sonar.dossier.dto.CompetitiveVenue
import com.sonar.dossier.dao.cassandra.{JSONSerializer, CompetitiveVenueColumn, CompetitiveVenueColumnSerializer}
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222

class CompetitorAnalysisForPlaces(args: Args) extends Job(args) {

    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")

    val chkininputData = args("checkinData")
    val checkinGrouperPipe = new CheckinGrouperFunction(args)

    val chkindata = checkinGrouperPipe.unfilteredCheckinsLatLon(TextLine(chkininputData).read)

    val similarity = chkindata.groupBy('keyid, 'venName) {
        _.size
    }.rename('size -> 'rating).project('keyid, 'venName, 'rating)

    val numRaters = similarity
            .groupBy('venName) {
        _.size
    }.rename('size -> 'numRaters)

    val ratingsWithSize = similarity.joinWithSmaller('venName -> 'venName, numRaters)


    val ratings2 =
        ratingsWithSize
                .rename(('keyid, 'venName, 'rating, 'numRaters) ->('keyid2, 'venName2, 'rating2, 'numRaters2))


    val ratingPairs =
        ratingsWithSize
                .joinWithSmaller('keyid -> 'keyid2, ratings2)
                // De-dupe so that we don't calculate similarity of both (A, B) and (B, A).
                .filter('venName, 'venName2) {
            movies: (String, String) => movies._1 < movies._2
        }
                .project('venName, 'rating, 'numRaters, 'venName2, 'rating2, 'numRaters2)


    val vectorCalcs =
        ratingPairs
                // Compute (x*y, x^2, y^2), which we need for dot products and norms.
                .map(('rating, 'rating2) ->('ratingProd, 'ratingSq, 'rating2Sq)) {
            ratings: (Double, Double) =>
                (ratings._1 * ratings._2, math.pow(ratings._1, 2), math.pow(ratings._2, 2))
        }
                .groupBy('venName, 'venName2) {
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
                .map(('venName, 'venName2, 'jaccardSimilarity) ->('rowKey, 'columnName, 'columnValue)) {
            in: (String, String, Double) =>
                var (venueGoldenId, venueName, similarityIndex) = in

                println("similarityIndex +" + similarityIndex)
                val analysisType = com.sonar.dossier.dto.CompetitiveAnalysisType.competitor
                val targetVenueGoldenId = "id_" + venueName
                val column = CompetitiveVenueColumn(venueGoldenId = targetVenueGoldenId, frequency = similarityIndex)
                val dto = new CompetitiveVenue(
                    analysisType = analysisType,
                    venueId = targetVenueGoldenId,
                    venueName = venueName,
                    venueType = "undefined",
                    frequency = similarityIndex
                )
                val columnB = CompetitiveVenueColumnSerializer toByteBuffer (column)
                val dtoB = new JSONSerializer(classOf[CompetitiveVenue]) toByteBuffer (dto)

                (venueGoldenId + "-" + analysisType.name, columnB, dtoB)

        }.project(('rowKey, 'columnName, 'columnValue))
                .write(CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueCompetitiveAnalysis",
            scheme = WideRowScheme(keyField = 'rowKey)
        ))


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

}


//('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat,'lon, 'dayOfYear, 'dayOfWeek, 'hour)

/*val chkindatarenamepipe = chkindata.rename(('keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'lat, 'lng, 'dayOfYear, 'dayOfWeek, 'hour) ->
        ('keyid2, 'serType2, 'serProfileID2, 'serCheckinID2, 'venName2, 'venAddress2, 'chknTime2, 'ghash2, 'lat2, 'lng2, 'dayOfYear2, 'dayOfWeek2, 'hour2))

val joinedcorrelation = chkindatarenamepipe.joinWithLarger('keyid2 -> 'keyid, chkindata)
        .project(('keyid,'venName, 'lat, 'lng, 'venName2, 'lat2, 'lng2))
        .filter('venName, 'venName2) {
    places: (String, String) => (places._1.trim != places._2.trim && places._1.trim != "" && places._2.trim != "")
}
        .groupBy('keyid,'venName, 'venName2) {
    _.size
}*/
