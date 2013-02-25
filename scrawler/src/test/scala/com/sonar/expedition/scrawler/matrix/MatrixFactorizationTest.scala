package com.sonar.expedition.scrawler.matrix

//import breeze.linalg._
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import scala.math._
import util.Random

class MatrixFactorizationTest extends FlatSpec with ShouldMatchers {

    "A Matrix" should "be created" in {
        /*
        val R = DenseMatrix(
            (5, 3, 0, 1),
            (4, 0, 0, 1),
            (1, 1, 0, 5),
            (1, 0, 0, 4),
            (0, 1, 5, 4)
        )

        val K = 2
        val N = R.rows
        val M = R.cols
        val P = DenseMatrix.rand(N, K)
        val Q = DenseMatrix.rand(M, K).t

        /*
        val P = DenseMatrix(
            (1.0, 2.0),
            (2.0, 3.0),
            (3.0, 4.0),
            (4.0, 5.0),
            (5.0, 6.0)
        )
        val Q = DenseMatrix(
            (1.0, 2.0),
            (2.0, 3.0),
            (3.0, 4.0),
            (4.0, 5.0)
        ).t
        */

        val alpha = 0.0002
        val beta = 0.02
        println(P)
        println("")
        println(Q)

        val dotprod = P * Q
        println("dot:\n" + dotprod)

        println("original:\n" + R)

        val steps = 5000
        for (step <- 0 until steps) {
            for (n <- 0 until N;
                 m <- 0 until M) {
                if (R(n, m) > 0) {
                    val dot: DenseVector[Double] = P(n, ::) * Q(::, m)
                    val enm = R(n, m) - dot(0)
                    for (k <- 0 until K) {
                        P(n, k) = P(n, k) + alpha * (2 * enm * Q(k, m) - beta * P(n, k))
                        Q(k, m) = Q(k, m) + alpha * (2 * enm * P(n, k) - beta * Q(k, m))
                    }
                }
            }
            val eR = P * Q
            var e = 0.0
            for (n <- 0 until N if e < 0.001;
                 m <- 0 until M) {
                if (R(n, m) > 0) {
                    val dp: DenseVector[Double] = P(n, ::) * Q(::, m)
                    e = e + pow(R(n, m) - dp(0), 2)
                    for (k <- 0 until K) {
                        e = e + (beta/2) * (pow(P(n,k),2) + pow(Q(k, m), 2))
                    }
                }
            }
        }
        println("final:\n" + P * Q)
        */
    }
}