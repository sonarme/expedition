import org.apache.commons.math3.stat.inference.TestUtils

object Test extends App {
    val matrix = breeze.linalg.DenseMatrix((30, 29, 16), (12, 33, 5))
    println(matrix)
    val scaled1 = matrix * 2
    println(scaled1)
    println(TestUtils.chiSquareTest(Array(Array(30, 29, 16), Array(12, 33, 5))))
    Fisher.test()
    println(Fisher.pValue(3, 1, 1, 3))
    println(Fisher.pValue(2, 10, 15, 3))
}
