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
            .flatMapTo(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum) ->('key, 'token, 'doc)) {
        fields: (String, String, String, String, String, String) =>
            val (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, docid) = fields
            val key = classifiersCategory.toLowerCase
            val tokens = List(propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory)
            for ((token, idx) <- tokens.zipWithIndex) yield (key, token, docid + "_" + idx)

    }.project(('key, 'token, 'doc))

    val model = trainBayesModel(docs)
    model.write(SequenceFile(trainingmodel, Fields.ALL))


}
