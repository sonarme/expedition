package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding.{Tsv, Job, Args}
import com.twitter.scalding.mathematics.Matrix

class SimilarUsersJob(args: Args) extends Job(args) {

    import Matrix._

    val docWordMatrix = Tsv(args("input"), ('user, 'ip, 'count))
            .read
            .toMatrix[String, String, Double]('user, 'ip, 'count)

    // compute the overall document frequency of each row
    val docFreq = docWordMatrix.sumRowVectors

    // compute the inverse document frequency vector
    val invDocFreqVct = docFreq.toMatrix(1).rowL1Normalize.mapValues(x => log2(1 / x))

    // zip the row vector along the entire document - word matrix
    val invDocFreqMat = docWordMatrix.zip(invDocFreqVct.getRow(1)).mapValues(pair => pair._2)

    // multiply the term frequency with the inverse document frequency and keep the top nrWords
//    docWordMatrix.hProd(invDocFreqMat).topRowElems(args("nrWords").toInt).write(Tsv(args("output")))

    val tfidf = invDocFreqMat.hProd(docWordMatrix)

    val similarityMatrix = tfidf * docWordMatrix.transpose
    val cleanedSimilarityMatrix = similarityMatrix - similarityMatrix.diagonal //remove diagonals which signify same users
    cleanedSimilarityMatrix.write(Tsv(args("output")))

    def log2(x: Double) = scala.math.log(x) / scala.math.log(2.0)
}