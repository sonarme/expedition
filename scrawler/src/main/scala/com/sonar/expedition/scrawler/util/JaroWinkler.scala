package com.sonar.expedition.scrawler.util

object JaroWinkler {
    // maximum prefix length to use
    private final val MINPREFIXTESTLENGTH: Int = 4 //using value from lingpipe (was 6)

    // prefix adjustment scale
    private final val PREFIXADUSTMENTSCALE: Double = 0.1

    def jaro(string1: String, string2: String): Double = {
        //get half the length of the string rounded up - (this is the distance used for acceptable transpositions)
        val halflen: Int = ((math.min(string1.length, string2.length)) / 2) + ((math.min(string1.length, string2.length)) % 2)

        //get common characters
        val common1 = getCommonCharacters(string1, string2, halflen)
        val common2 = getCommonCharacters(string2, string1, halflen)

        //check for zero in common
        if (common1.length == 0 || common2.length == 0) {
            return 0.0
        }
        /*
        //check for same length common strings returning 0.0 is not the same
        if (common1.length != common2.length) {
            return 0.0
        }
        */

        //get the number of transpositions
        var transpositions: Int = 0

        for (i <- 0 to math.min(common1.length - 1, common2.length - 1)) {
            if (common1.charAt(i) != common2.charAt(i)) {
                transpositions += 1
            }
        }

        transpositions = transpositions / 2

        //calculate jaro metric
        (common1.length / (string1.length.toDouble) + common2.length / (string2.length.toDouble) + (common1.length - transpositions) / (common1.length.toDouble)) / 3.0
    }


    /**
     * returns a string buffer of characters from string1 within string2 if they are of a given
     * distance seperation from the position in string1.
     *
     * @param string1
     * @param string2
     * @param distanceSep
     * @return a string buffer of characters from string1 within string2 if they are of a given
     *         distance seperation from the position in string1
     */
    private def getCommonCharacters(string1: String, string2: String, distanceSep: Int): StringBuilder = {
        val returnCommons = new StringBuilder()
        val copy = new StringBuilder(string2)

        var i = 0
        for (string1Char <- string1) {
            var foundIt = false
            var j = math.max(0, i - distanceSep)
            while (!foundIt && j < math.min(i + distanceSep + 1, string2.length)) {
                if (copy.charAt(j) == string1Char) {
                    foundIt = true
                    returnCommons.append(string1Char)
                    copy.setCharAt(j, 0.toChar)
                }
                j += 1
            }
            i += 1
        }

        returnCommons
    }

    /**
     * gets the similarity measure of the JaroWinkler metric for the given strings.
     *
     * @param string1
     * @param string2
     * @return 0 -1 similarity measure of the JaroWinkler metric
     */
    def evaluateDistance(string1: String, string2: String): Double = {
        val dist = jaro(string1, string2)
        val prefixLength: Int = getPrefixLength(string1, string2)
        dist + (prefixLength.asInstanceOf[Double] * PREFIXADUSTMENTSCALE * (1.0 - dist))
    }

    /**
     * gets the prefix length found of common characters at the begining of the strings.
     *
     * @param string1
     * @param string2
     * @return the prefix length found of common characters at the begining of the strings
     */
    private def getPrefixLength(string1: String, string2: String): Int = {
        val max: Int = math.min(MINPREFIXTESTLENGTH, math.min(string1.length, string2.length))

        var i: Int = 0
        while (i < max) {
            if (string1.charAt(i) != string2.charAt(i)) {
                return i
            }
            i += 1
        }
        max
    }

}
