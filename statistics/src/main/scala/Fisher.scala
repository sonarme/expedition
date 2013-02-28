import org.apache.commons.math3.distribution.HypergeometricDistribution

object Fisher {

    def pValue(aTrue: Int, aFalse: Int, bTrue: Int, bFalse: Int, epsilon: Double = 1e-10) = {
        // convert the a/b groups to study vs population.
        val k = aTrue
        val n = aFalse + aTrue // total in study
        val C = aTrue + bTrue
        val N = C + aFalse + bFalse

        val um = math.min(n, C)
        val lm = math.max(0, n + C - N)
        if (um == lm) (1d, 1d, 1d)
        else {
            val distribution = new HypergeometricDistribution(N, C, n)
            val cutoff = distribution.probability(k)
            var leftTail = 0d
            var rightTail = 0d
            var twoTail = 0d
            for (i <- lm to um + 1) {
                val p = distribution.probability(i)
                if (i <= k) leftTail += p
                if (i >= k) rightTail += p
                if (p < cutoff + epsilon) twoTail += p
            }
            (math.min(leftTail, 1d), math.min(rightTail, 1d), math.min(twoTail, 1d))
        }
    }

    def test() {
        // found these tests on this ticket. (thanks).
        // http://projects.scipy.org/scipy/ticket/956
        // these values were taken from R as a means to test the code in that ticket.
        val tablist = Seq(
            (Array(Array(100, 2), Array(1000, 5)), (2.505583993422285e-001, 1.300759363430016e-001)),
            (Array(Array(2, 100), Array(5, 1000)), (2.505583993422285e-001, 1.300759363430016e-001)),
            (Array(Array(2, 7), Array(8, 2)), (8.586235135736206e-002, 2.301413756522114e-002)),
            (Array(Array(5, 1), Array(10, 10)), (4.725646047336584e+000, 1.973244147157190e-001)),
            (Array(Array(5, 15), Array(20, 20)), (3.394396617440852e-001, 9.580440012477637e-002)),
            (Array(Array(5, 16), Array(20, 25)), (3.960558326183334e-001, 1.725864953812994e-001)),
            (Array(Array(10, 5), Array(10, 1)), (2.116112781158483e-001, 1.973244147157190e-001)),
            (Array(Array(10, 5), Array(10, 0)), (0.000000000000000e+000, 6.126482213438734e-002)),
            (Array(Array(5, 0), Array(1, 4)), (Double.PositiveInfinity, 4.761904761904762e-002)),
            (Array(Array(0, 5), Array(1, 4)), (0.000000000000000e+000, 1.000000000000000e+000)),
            (Array(Array(5, 1), Array(0, 4)), (Double.PositiveInfinity, 4.761904761904758e-002)),
            (Array(Array(0, 1), Array(3, 2)), (0.000000000000000e+000, 1.000000000000000e+000))
        )


        for ((table, ab) <- tablist) {
            val (leftTail, rightTail, twoTail) = pValue(table(0)(0), table(0)(1), table(1)(0), table(1)(1))
            assert(math.abs(twoTail - ab._2) < 0.1)
        }
    }
}
