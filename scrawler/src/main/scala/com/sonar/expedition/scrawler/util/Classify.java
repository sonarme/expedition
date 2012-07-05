package com.sonar.expedition.scrawler.util;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.classifier.bayes.algorithm.BayesAlgorithm;
import org.apache.mahout.classifier.bayes.common.BayesParameters;
import org.apache.mahout.classifier.bayes.datastore.InMemoryBayesDatastore;
import org.apache.mahout.classifier.bayes.interfaces.Algorithm;
import org.apache.mahout.classifier.bayes.interfaces.Datastore;
import org.apache.mahout.classifier.bayes.model.ClassifierContext;
import org.apache.mahout.common.nlp.NGrams;

public class Classify {

    public static BayesParameters setParams() {
        BayesParameters params = new BayesParameters(2);
        params.set("verbose", "false");
        // params.set("basePath", modString);// modelDir is the dir where the
        //params.set("classifierType", "bayes");
        params.set("classifierType", "cbayes");
        //Interchange the values for swap between byaes and cbayes classifier
        params.set("dataSource", "hdfs");
        params.set("defaultCat", "unknown");
        params.set("encoding", "UTF-8");
        params.set("alpha_i", "1.0");

        return params;

    }

    public static ClassifierContext getClassifer(String modString)
            throws Exception {
        BayesParameters params = setParams();
        params.set("basePath", modString);
        Algorithm algorithm = new BayesAlgorithm();// Creating the instance of
        Datastore datastore = new InMemoryBayesDatastore(params);// Creating
        ClassifierContext classifier = new ClassifierContext(algorithm,
                datastore);
        classifier.initialize();

        return classifier;

    }

    public static ClassifierResult getClassFordoc(String fileContent,
                                                  ClassifierContext classifier) throws Exception {
        BayesParameters params = setParams();
        ClassifierContext myClassifir = classifier;
        // Object sentRes[] = new Object[3];
        Collection<String> labels = myClassifir.getLabels();
        ArrayList<String> labelTest = new ArrayList<String>(labels);
        ResultAnalyzer resultAnalyzer = new ResultAnalyzer(
                classifier.getLabels(), params.get("defaultCat"));

        List<String> document = new NGrams(fileContent, Integer.parseInt(params
                .get("gramSize"))).generateNGramsWithoutLabel();

        ClassifierResult result = classifier.classifyDocument(
                document.toArray(new String[document.size()]),
                params.get("defaultCat"));

        /*
        * ClassifierResult[] resNew = classifier.classifyDocument(document
        * .toArray(new String[document.size()]), params.get("defaultCat"), 2);
        *
        * Hashtable categVals = new Hashtable();
        *
        * for (ClassifierResult re : resNew) { categVals.put(re.getLabel(), new
        * Double(re.getScore())); } double approve = ((Double)
        * categVals.get("positive")).doubleValue(); double reject = ((Double)
        * categVals.get("negative")).doubleValue();
        */

        // String sentRes = result.getLabel().toString();

        // sentRes[1] = approve;
        // sentRes[2] = reject;

        return result;

    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String test = "from wtm@uhura.neoucom.edu bill mayhew subject re mysterious tv problem source organization northeastern ohio universities college medicine lines 19 set direct line powered try checking likely hybrid regulator module down stream from 170 volt supply several sets i've looked use 135 volt regulator regulators have tendency short out making safety circuits shut down eht supply section try putting set variac adjustable transformer lower ac input voltage set about 90 volts set operates nromally you know you've got shorted regulator myriad other areas problems i've seen one above several times also set uses one trippler module may shot fairly common bill mayhew neoucom computer services department rootstown oh 44272-9995 usa phone 216-325-2511 wtm@uhura.neoucom.edu 140.220.1.1 146.580 n8wed ";
        String modelDir = "/home/jaganadhg/backup/Desktop/mahout-distribution-0.4/20cbayesn";
        //String modelDir = "/home/jaganadhg/backup/Desktop/mahout-distribution-0.4/20bayes";
        //Interchange the values for swap between byaes and cbayes classifier
        // for bayes
        ClassifierContext bck9cl = getClassifer(modelDir);


        ClassifierResult res = getClassFordoc(test, bck9cl);

        System.out.println("Label: " + res.getLabel() + " Score: "
                + res.getScore());

    }
}
