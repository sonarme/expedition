package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding.{IterableSource, Tsv, Job, Args}
import com.twitter.scalding.mathematics.Matrix
import com.sonar.expedition.scrawler.util.Tuples

class SimilarUsersJob(args: Args) extends Job(args) {

    import Matrix._

    val userIpMatrix = IterableSource(Seq(
            ("roger", "ip1", 3),
            ("roger", "ip2", 2),
            ("roger", "ip3", 0),
            ("paul", "ip1", 2),
            ("paul", "ip2", 0),
            ("paul", "ip3", 1),
            ("ben", "ip1", 0),
            ("ben", "ip2", 0),
            ("ben", "ip3", 2) ,
            ("brett", "ip1", 1),
            ("brett", "ip2", 2),
            ("brett", "ip3", 0)
        ), Tuples.Matrix).read.toMatrix[String, String, Double]('x, 'y, 'val)


    // compute the overall user frequency of each row
    val userFreq = userIpMatrix.sumRowVectors

    // compute the inverse user frequency vector
    val invUserFreqVct = userFreq.toMatrix(1).rowL1Normalize.mapValues(x => log2(1 / x))

    // zip the row vector along the entire user - ip matrix
    val invUserFreqMat = userIpMatrix.zip(invUserFreqVct.getRow(1)).mapValues(pair => pair._2)

    val tfidf = invUserFreqMat.hProd(userIpMatrix)

    val similarityMatrix = tfidf * userIpMatrix.transpose

    //remove diagonals which signify same users
    val cleanedSimilarityMatrix = similarityMatrix - similarityMatrix.diagonal
    cleanedSimilarityMatrix.topRowElems(args("nrUsers").toInt).write(Tsv(args("output")))

    def log2(x: Double) = scala.math.log(x) / scala.math.log(2.0)
}