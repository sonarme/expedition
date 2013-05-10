package com.sonar.expedition.scrawler.jobs.behavior

import com.twitter.scalding.{IterableSource, Tsv, Job, Args}
import com.twitter.scalding.mathematics.Matrix
import com.sonar.expedition.scrawler.util.Tuples

/**
 * based on http://www.quuxlabs.com/blog/2010/09/matrix-factorization-a-simple-tutorial-and-implementation-in-python/
 * @param args
 */
class FindIndependentFeaturesJob(args: Args) extends Job(args) {

    import Matrix._

    /*
    val matrixA = Tsv(args("matrixA"), ('feature, 'item, 'count))
                .read
                .toMatrix[String, String, Double]('feature, 'item, 'count)

    val matrixB = Tsv(args("matrixB"), ('feature, 'item, 'count))
                    .read
                    .toMatrix[String, String, Double]('feature, 'item, 'count)
    */

    val origMatrix = IterableSource(Seq(
        (1, 1, 29),
        (1, 2, 29),
        (2, 1, 43),
        (2, 2, 33),
        (3, 1, 15),
        (3, 2, 25),
        (4, 1, 40),
        (4, 2, 28),
        (5, 1, 24),
        (5, 2, 11),
        (6, 1, 29),
        (6, 2, 29),
        (7, 1, 37),
        (7, 2, 23),
        (8, 1, 21),
        (8, 2, 6)
    ), Tuples.Matrix)


    val weightsMatrix = IterableSource(Seq(
        (1, 1, 2),
        (1, 2, 9),
        (2, 1, 3),
        (2, 2, 3),
        (3, 1, 5),
        (3, 2, 2),
        (4, 1, 4),
        (4, 2, 8),
        (5, 1, 4),
        (5, 2, 1),
        (6, 1, 2),
        (6, 2, 9),
        (7, 1, 7),
        (7, 2, 3),
        (8, 1, 1),
        (8, 2, 6)
    ), Tuples.Matrix)

    val featuresMatrix = IterableSource(Seq(
        (1, 1, 9),
        (1, 2, 2),
        (2, 1, 4),
        (2, 2, 3)
    ), Tuples.Matrix)


    def difcost(a: Matrix[String, String, Double], b: Matrix[String, String, Double]) = {
        val sum = a.elemWiseOp(b)((x, y) => math.pow((x - y), 2)).sum
        sum
    }

    def factorize(matrix: Matrix[String, String, Double], features: Int, iterations: Int) {
        val wm = weightsMatrix.read.toMatrix[String, String, Double]('x, 'y, 'val)
        val fm = featuresMatrix.read.toMatrix[String, String, Double]('x, 'y, 'val)
        val wf = wm * fm

        val cost = difcost(matrix, wf)
        //        cost.write(Tsv(args("output")))

        //update features matrix
        val fn = wm.transpose * matrix
        val fd = wm.transpose * wm * fm

        //        featuresMatrix = (featuresMatrix * fn).elemWiseOp(fd)((x,y) => x/y)

        //update weights matrix
        val wn = matrix * fm.transpose
        val wd = wm * fm * fm.transpose

        //        weightsMatrix = (weightsMatrix * wn).elemWiseOp(wd)((x,y) => x/y)

        wf.write(Tsv(args("weightsMatrix")))
        //        featuresMatrix.write(Tsv(args("featuresMatrix")))
    }

    //    factorize(origMatrix.read.toMatrix[String, String, Double]('x, 'y, 'val), 2, 10)

    weightsMatrix.read.toMatrix[String, String, Double]('x, 'y, 'val).write(Tsv(args("weightsMatrix")))
    featuresMatrix.read.toMatrix[String, String, Double]('x, 'y, 'val).write(Tsv(args("featuresMatrix")))
}