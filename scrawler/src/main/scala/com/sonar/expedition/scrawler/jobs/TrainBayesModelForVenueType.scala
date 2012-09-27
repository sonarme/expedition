package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine

/*
com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysisBayesModel --hdfs --placesData "/tmp/places_dump_US.geojson.txt" --bayesmodelforvenuetype "/tmp/bayesmodelforvenuetype"

 */
class TrainBayesModelForVenueType(args: Args) extends Job(args) with LocationBehaviourAnalysePipe with BayesModelPipe {

    val trainingmodel = args("bayesmodelforvenuetype")
    val placesData = args("placesData")

    val docs = placesPipe(TextLine(placesData).read)
            //propertiesTags    classifiersCategory  classifiersType  classifiersSubcategory
            .flatMapTo(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum) ->('key, 'token, 'doc)) {
        fields: (String, List[String], List[String], List[String], List[String], String) =>
            val (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, docid) = fields
            val tokens = propertiesName :: (propertiesTags ++ classifiersCategory ++ classifiersType ++ classifiersSubcategory)
            for (key <- classifiersCategory;
                 token <- tokens) yield (key.toLowerCase, token, docid)

    }.project(('key, 'token, 'doc))

    val model = trainBayesModel(docs)
    model.write(SequenceFile(trainingmodel, Fields.ALL))
            .write(Tsv(trainingmodel + "_tsv", Fields.ALL))


}
