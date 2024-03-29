package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer


trait BayesModelPipe extends ScaldingImplicits {
    val specialWordSet1 = Set("has", "was", "does", "goes", "dies", "yes", "gets", "its")
    val specialWordSet2 = Set("sses", "xes", "hes")
    val specialWordSet3 = Set("ies")
    val specialWordSet4 = Set("s", "ss", "is", "us", "pos", "ses")

    def trainBayesModel(input: RichPipe): RichPipe = {

        // input has 'key, 'token, and 'doc

        // turn all to lower case, strip plurals and remove punctuation from token
        val reading = input.map('token -> 'token2) {
            token: String => StemAndMetaphoneEmployer.removeStopWords(stripEnglishPlural(token.replaceAll( """\p{P}""", " ")))
        }.discard('token).rename('token2 -> 'token)

        // input has 'key, 'token, and 'doc
        // *************
        // featuredriver
        // *************

        // feature count, number of times feature appears with a key
        val fcPipe = reading.groupBy(('key, 'token)) {
            _.size('featureCount)
        }

        // rms across keys, can change to be across documents
        val rmsPipe = fcPipe.groupBy('key) {
            _.mapList[Int, Double]('featureCount -> 'rms) {
                list: List[Int] => normaliseList(list)
            }
        }

        // term doc count, number of docs term appears in
        val tdcPipe = reading.unique(('doc, 'token)).groupBy('token) {
            _.size('termDocCount)
        }

        // doc count, number of docs total
        val numDocs = reading.unique('doc).groupAll {
            _.size('docCount)
        }

        // vocab count, number of unique key/value pairs summed across keys
        val vocab = reading.unique(('key, 'token)).groupBy('key) {
            _.size('vocabCount)
        }

        // val docPipe = reading.crossWithTiny(numDocs)

        // wordFreq, normalized frequency 'normSize
        val counter = reading.groupBy(('key, 'token)) {
            _.size
        }

        val sumOfSizeSquared = counter.groupAll {
            _.toList[Int]('size -> 'sizeList)
        }

                .map('sizeList -> 'sumSize) {
            sizeList: List[Int] => {
                sizeList.foldLeft[Int](0)((a, b) => a + (b * b))
            }
        }.discard('sizeList)

        val normalized = counter.crossWithTiny(sumOfSizeSquared).map(('size, 'sumSize) -> 'weight) {
            fields: (Int, Int) => {
                val (size, sumSize) = fields
                math.log(1.0 + size) / math.sqrt(sumSize)
            }
        }

        val totalPipe = normalized
                .joinWithSmaller('token -> 'token, tdcPipe)
                .joinWithSmaller(('key, 'token) ->('key, 'token), fcPipe)
                //.joinWithSmaller(('key, 'token) ->('key, 'token), docPipe)
                .crossWithTiny(numDocs)
                .joinWithSmaller('key -> 'key, rmsPipe)
                .joinWithSmaller('key -> 'key, vocab)

        // ***********
        // tf-idf
        // ***********

        // idf
        val tfidfPipe = totalPipe.map(('termDocCount, 'docCount, 'featureCount, 'rms) -> 'logTFIDF) {
            fields: (Int, Int, Int, Double) => {
                val (tdc, dc, fc, rms) = fields
                val logIDF = math.log(dc * 1.0 / tdc)
                val tf = if (fc == 0) 0.0 else fc / rms
                logIDF * tf

            }
        }

        // sigma_k, sum of tf-idf across the label
        val tfidfSummerPipe = tfidfPipe.groupBy('key) {
            _.sum(('logTFIDF) -> ('sigmak))
        }

        // tf-idf
        val normTFIDFPipe = tfidfPipe.joinWithSmaller('key -> 'key, tfidfSummerPipe)
                .map(('logTFIDF, 'sigmak, 'vocabCount) -> 'normTFIDF) {
            fields: (Double, Double, Int) => {
                val (tfidf, sk, vc) = fields
                (tfidf + 1) / (sk + vc)
            }
        }
        normTFIDFPipe.project(('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak))
    }


    def normaliseList(freq: List[Int]): Double = math.sqrt(freq reduce {
        (acc, elem) => acc + (elem * elem)
    })

    def stripEnglishPlural(word: String): String = {

        // too small?
        if (word.length < 1) {
            word
        }
        // special cases
        else if (specialWordSet1(word)) {
            word
        }
        else if (specialWordSet2(word)) {
            // remove 'es'
            word.substring(0, word.length() - 2);
        }
        else if (specialWordSet3(word)) {
            // remove 'ies', replace with 'y'
            word.substring(0, word.length() - 3) + 'y'
        }
        else if (specialWordSet4(word)) {
            // remove 's'
            word.substring(0, word.length - 1)
        } else {
            word
        }
    }

    def calcProb(model: RichPipe, data: RichPipe) =
        data.flatMap('data -> 'token) {
            line: String => StemAndMetaphoneEmployer.extractTokens(line).toSet
        }.joinWithSmaller('token -> 'token, model).groupBy('data, 'key) {
            _.sum('normTFIDF -> 'weight)
        }.groupBy('data) {
            _.max('weight, 'key)
        }.filter('data) {
            data: String => data != null // TODO: is this necessary?
        }.project('data, 'key, 'weight)

}
