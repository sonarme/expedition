package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.pipes.{LocationBehaviourAnalysePipe, PlacesCorrelation}
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.SequenceFile

/**
 * This class computes similarities between places
 * by representing each places as a vector of checkins and
 * computing similarity scores over these vectors.
 *
 * Similarity measures include correlation, cosine similarity,
 * and Jaccard similarity.
 *
 */
class CompetitorAnalysisForPlaces(args: Args) extends Job(args) with LocationBehaviourAnalysePipe with PlacesCorrelation with CheckinSource {

    val competitiveAnalysisOutput = args("competitiveAnalysisOutput")
    val placeClassification = args("placeClassification")

    val (checkins, _) = checkinSource(args, true, false)
    val allCheckins = checkins.project('keyid, 'venId)

    /**
     * Place is a tuple ('goldenId, 'venueId, 'venueLat, 'venueLng, 'venName, 'venAddress, 'venuePhone, 'venueType)
     */
    val places = SequenceFile(placeClassification, PlaceClassification.PlaceClassificationOutputTuple).read
    val placesVenueGoldenId =
        places
                .unique('goldenId, 'venueId)
                .joinWithLarger('venueId -> 'venId, allCheckins)
                .project('keyid, 'goldenId)

    // keyid is serviceType:serviceProfileId
    val checkinsAtPlace = placesVenueGoldenId.groupBy('keyid, 'goldenId) {
        _.size
    }.rename('size -> 'numCheckins).project('keyid, 'goldenId, 'numCheckins)

    val numPeople = checkinsAtPlace
            // Put the size of each group in a field called "numPeople".
            .groupBy('goldenId) {
        _.size
    } // Rename, since Scalding currently requires both sides of a join to have distinctly named fields.
            .rename('size -> 'numPeople)

    /**
     * Join with number of people who checked in at a place.
     */
    val numCheckinsWithSize = checkinsAtPlace.joinWithSmaller('goldenId -> 'goldenId, numPeople)


    /**
     * Make a dummy copy of the numCheckins, so we can do a self-join.
     */
    val numCheckins2 =
        numCheckinsWithSize
                .rename(('keyid, 'goldenId, 'numCheckins, 'numPeople) ->('keyid2, 'goldenId2, 'numCheckins2, 'numPeople2))


    /**
     * Join the two checkins streams on their user fields,
     * in order to find all pairs of places that a user has checked in.
     */
    val numCheckinsPairs =
        numCheckinsWithSize
                .joinWithSmaller('keyid -> 'keyid2, numCheckins2)
                // De-dupe so that we don't calculate similarity of both (A, B) and (B, A).
                .filter('goldenId, 'goldenId2) {
            venues: (String, String) => venues._1 < venues._2
        }
                .project('goldenId, 'numCheckins, 'numPeople, 'goldenId2, 'numCheckins2, 'numPeople2)

    /**
     * Compute dot products, norms, sums, and sizes of the checkins vectors.
     */
    val vectorCalcs =
        numCheckinsPairs
                // Compute (x*y, x^2, y^2), which we need for dot products and norms.
                .map(('numCheckins, 'numCheckins2) ->('numCheckinsProd, 'numCheckinsSq, 'numCheckins2Sq)) {
            numCheckins: (Double, Double) =>
                (numCheckins._1 * numCheckins._2, math.pow(numCheckins._1, 2), math.pow(numCheckins._2, 2))
        }
                .groupBy('goldenId, 'goldenId2) {
            group =>
                group.size // length of each vector
                        .sum('numCheckinsProd -> 'dotProduct)
                        .sum('numCheckins -> 'numCheckinsSum)
                        .sum('numCheckins2 -> 'numCheckins2Sum)
                        .sum('numCheckinsSq -> 'numCheckinsNormSq)
                        .sum('numCheckins2Sq -> 'numCheckins2NormSq)
                        .max('numPeople) // Just an easy way to make sure the numPeople field stays.
                        .max('numPeople2)
        }
    val placesNames = places.project('goldenId, 'venName, 'venueType).rename('goldenId -> 'goldenIdForName)

    /**
     * Calculate similarity between numCheckins vectors using similarity measures
     * like correlation, cosine similarity, and Jaccard similarity.
     */
    val similarities =
        vectorCalcs
                .map(('size, 'dotProduct, 'numCheckinsSum, 'numCheckins2Sum, 'numCheckinsNormSq, 'numCheckins2NormSq, 'numPeople, 'numPeople2) ->
                ('correlation, 'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity)) {

            fields: (Double, Double, Double, Double, Double, Double, Double, Double) =>

                val (size, dotProduct, numCheckinsSum, numCheckins2Sum, numCheckinsNormSq, numCheckins2NormSq, numPeople, numPeople2) = fields
                val priorCount = 10
                val priorCorrelation = 0

                val corr = correlation(size, dotProduct, numCheckinsSum, numCheckins2Sum, numCheckinsNormSq, numCheckins2NormSq)
                val regCorr = regularizedCorrelation(size, dotProduct, numCheckinsSum, numCheckins2Sum, numCheckinsNormSq, numCheckins2NormSq, priorCount, priorCorrelation)
                val cosSim = cosineSimilarity(dotProduct, math.sqrt(numCheckinsNormSq), math.sqrt(numCheckins2NormSq))
                val jaccard = jaccardSimilarity(size, numPeople, numPeople2)
                //can also calculate correlation, 'regularizedCorrelation, 'cosineSimilarity

                (corr, regCorr, cosSim, jaccard)
        }
                // join venue name and type back in
                .joinWithLarger('goldenId -> 'goldenIdForName, placesNames)
                .discard('goldenIdForName)
                .joinWithLarger('goldenId2 -> 'goldenIdForName, placesNames.rename(('venName, 'venueType) ->('venName2, 'venueType2)))
                .project(('venName, 'venueType, 'goldenId, 'venName2, 'venueType2, 'goldenId2, 'jaccardSimilarity))

                /**
                 * Output all similarities to a Sequence and TSV file.
                 */
                .write(SequenceFile(competitiveAnalysisOutput, Fields.ALL))
                .write(Tsv(competitiveAnalysisOutput + "_tsv", Fields.ALL))


    /**
     * The correlation between two vectors A, B is
     * cov(A, B) / (stdDev(A) * stdDev(B))
     *
     * This is equivalent to
     * [n * dotProduct(A, B) - sum(A) * sum(B)] /
     * sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
     */
    def correlation(size: Double, dotProduct: Double, numCheckinsSum: Double,
                    numCheckins2Sum: Double, numCheckinsNormSq: Double, numCheckins2NormSq: Double) = {

        val numerator = size * dotProduct - numCheckinsSum * numCheckins2Sum
        val denominator = math.sqrt(size * numCheckinsNormSq - numCheckinsSum * numCheckinsSum) * math.sqrt(size * numCheckins2NormSq - numCheckins2Sum * numCheckins2Sum)

        numerator / denominator
    }


    /**
     * The Jaccard Similarity between two sets A, B is
     * |Intersection(A, B)| / |Union(A, B)|
     */
    def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double) = usersInCommon / (totalUsers1 + totalUsers2 - usersInCommon)

    /**
     * The cosine similarity between two vectors A, B is
     * dotProduct(A, B) / (norm(A) * norm(B))
     */
    def cosineSimilarity(dotProduct: Double, numCheckinsNorm: Double, numCheckins2Norm: Double) = dotProduct / (numCheckinsNorm * numCheckins2Norm)


    /**
     * Regularize correlation by adding virtual pseudocounts over a prior:
     * RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
     * where w = # actualPairs / (# actualPairs + # virtualPairs).
     */
    def regularizedCorrelation(size: Double, dotProduct: Double, numCheckinsSum: Double,
                               numCheckins2Sum: Double, numCheckinsNormSq: Double, numCheckins2NormSq: Double,
                               virtualCount: Double, priorCorrelation: Double) = {

        val unregularizedCorrelation = correlation(size, dotProduct, numCheckinsSum, numCheckins2Sum, numCheckinsNormSq, numCheckins2NormSq)
        val w = size / (size + virtualCount)

        w * unregularizedCorrelation + (1 - w) * priorCorrelation
    }


}

