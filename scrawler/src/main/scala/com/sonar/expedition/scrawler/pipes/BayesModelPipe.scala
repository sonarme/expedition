package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Job}
import com.twitter.scalding.{Job, Args}

class BayesModelPipe(args: Args) extends Job(args) {


    def trainBayesModel(reading: RichPipe): RichPipe = {

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
            _.toList[Int]('featureCount -> 'featureList)
        }
                .map('featureList -> 'rms) {
            list: List[Int] => {
                val sumSq = list.foldLeft[Double](0.0)((a, b) => a + (b * b))
                Math.sqrt(sumSq)
            }
        }.discard('featureList)

        // term doc count, number of docs term appears in
        val tdcPipe = reading.unique(('doc, 'token)).groupBy('token) {
            _.size('termDocCount)
        }

        // doc count, number of docs total
        val numDocs = reading.unique(('doc)).groupAll {
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
                Math.log(1.0 + size) / Math.sqrt(sumSize)
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
        val tfidfPipe = totalPipe.map(('termDocCount, 'docCount) -> 'logIDF) {
            fields: (Int, Int) => {
                val (tdc, dc) = fields
                (Math.log(dc * 1.0 / tdc))
            }
        }
                // tf
                .map(('featureCount, 'rms) -> 'logTF) {
            fields: (Int, Double) => {
                val (fc, rms) = fields
                if (fc == 0)
                    0.0
                else
                    fc / rms
                //Math.log(1 + fc / rms)
            }
        }
                .map(('logIDF, 'logTF) -> ('logTFIDF)) {
            fields: (Double, Double) => {
                val (idf, tf) = fields
                idf * tf
            }
        }

        // sigma_k, sum of tf-idf across the label
        val tfidfSummerPipe = tfidfPipe.groupBy('key) {
            _.toList[Double]('logTFIDF -> 'tfidfList)
        }
                .map('tfidfList -> 'sigmak) {
            list: List[Double] => {
                list.foldLeft[Double](0.0)((a, b) => a + b)
            }
        }
                .discard('tfidfList)

        // tf-idf
        val normTFIDFPipe = tfidfPipe.joinWithSmaller('key -> 'key, tfidfSummerPipe)
                .map(('logTFIDF, 'sigmak, 'vocabCount) -> 'normTFIDF) {
            fields: (Double, Double, Int) => {
                val (tfidf, sk, vc) = fields
                (tfidf + 1) / (sk + vc)
            }
        }
        normTFIDFPipe.project('key, 'token, 'featureCount, 'termDocCount, 'docCount, 'logTF, 'logIDF, 'logTFIDF, 'normTFIDF, 'rms, 'sigmak)
    }

    def calcProb(model: RichPipe, data: RichPipe): RichPipe = {
        data.map('data -> 'value) {
            line: String => {
                line.split("\\s+")
            }
        }
                .flatMap('value -> 'token) {
            value: Array[String] =>
                value
        }
                .joinWithLarger(('token -> 'token), model)
                .groupBy(('data, 'key)){
            _.toList[Double]('normTFIDF -> 'weightList)
        }
                .map('weightList -> 'weight){
            list: List[Double] => {
                list.foldLeft[Double](0.0)((a, b) => a + b)
            }
        }.groupBy('data){
            _.toList[(Double, String)](('weight, 'key) -> 'weightKeyList)
        }
                .map(('weightKeyList) -> ('weight, 'key)) {
            fields: (List[(Double, String)]) =>
                val (weightKeyList) = fields
                val weightKey = weightKeyList.max
                (weightKey._1, weightKey._2)
        }
        .project('data, 'key, 'weight)
    }




}
