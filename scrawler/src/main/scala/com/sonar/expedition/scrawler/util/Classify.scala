package com.sonar.expedition.scrawler.util

import java.util.ArrayList
import java.util.Collection
import java.util.List
import org.apache.mahout.classifier.ClassifierResult
import org.apache.mahout.classifier.ResultAnalyzer
import org.apache.mahout.classifier.bayes.algorithm.BayesAlgorithm
import org.apache.mahout.classifier.bayes.common.BayesParameters
import org.apache.mahout.classifier.bayes.datastore.InMemoryBayesDatastore
import org.apache.mahout.classifier.bayes.interfaces.Algorithm
import org.apache.mahout.classifier.bayes.interfaces.Datastore
import org.apache.mahout.classifier.bayes.model.ClassifierContext
import org.apache.mahout.common.nlp.NGrams

object Classify {
    def setParams: BayesParameters = {
        val params: BayesParameters = new BayesParameters(2)
        params.set("verbose", "false")
        params.set("classifierType", "cbayes")
        params.set("dataSource", "hdfs")
        params.set("defaultCat", "unknown")
        params.set("encoding", "UTF-8")
        params.set("alpha_i", "1.0")
        return params
    }

    def getClassifer(modString: String): ClassifierContext = {
        val params: BayesParameters = setParams
        params.set("basePath", modString)
        val algorithm: Algorithm = new BayesAlgorithm
        val datastore: Datastore = new InMemoryBayesDatastore(params)
        val classifier: ClassifierContext = new ClassifierContext(algorithm, datastore)
        classifier.initialize
        return classifier
    }

    def getClassFordoc(fileContent: String, classifier: ClassifierContext): ClassifierResult = {
        val params: BayesParameters = setParams
        val myClassifir: ClassifierContext = classifier
        val labels: Collection[String] = myClassifir.getLabels
        val labelTest: ArrayList[String] = new ArrayList[String](labels)
        val resultAnalyzer: ResultAnalyzer = new ResultAnalyzer(classifier.getLabels, params.get("defaultCat"))
        val document: List[String] = new NGrams(fileContent, Integer.parseInt(params.get("gramSize"))).generateNGramsWithoutLabel
        val result: ClassifierResult = classifier.classifyDocument(document.toArray(new Array[String](document.size)), params.get("defaultCat"))
        return result
    }

    /**
     * @param args
     * @throws Exception
     */
    def main(args: Array[String]) {
        val test: String = "from wtm@uhura.neoucom.edu bill mayhew subject re mysterious tv problem source organization northeastern ohio universities college medicine lines 19 set direct line powered try checking likely hybrid regulator module down stream from 170 volt supply several sets i've looked use 135 volt regulator regulators have tendency short out making safety circuits shut down eht supply section try putting set variac adjustable transformer lower ac input voltage set about 90 volts set operates nromally you know you've got shorted regulator myriad other areas problems i've seen one above several times also set uses one trippler module may shot fairly common bill mayhew neoucom computer services department rootstown oh 44272-9995 usa phone 216-325-2511 wtm@uhura.neoucom.edu 140.220.1.1 146.580 n8wed "
        val modelDir: String = "/home/jaganadhg/backup/Desktop/mahout-distribution-0.4/20cbayesn"
        val bck9cl: ClassifierContext = getClassifer(modelDir)
        val res: ClassifierResult = getClassFordoc(test, bck9cl)
        System.out.println("Label: " + res.getLabel + " Score: " + res.getScore)
    }
}