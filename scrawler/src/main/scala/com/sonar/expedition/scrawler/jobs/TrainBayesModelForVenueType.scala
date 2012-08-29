package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine

/*
com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysisBayesModel --hdfs --placesData "/tmp/places_dump_US.geojson.txt" --bayestrainingmodelforvenuetype "/tmp/bayestrainingmodelforvenuetype"

 */
class TrainBayesModelForVenueType(args: Args) extends Job(args) with LocationBehaviourAnalysePipe with BayesModelPipe {

    val trainingmodel = args("bayestrainingmodelforvenuetype")
    val placesData = args("placesData")

    val placesPipe = getLocationInfo(TextLine(placesData).read)
            .project(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum))
            .flatMapTo(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum) ->('key, 'token, 'doc)) {
        fields: (String, String, String, String, String, Int) =>
            val (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, docid) = fields

            List(propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory) map {
                key => (classifiersCategory.toLowerCase, key, docid)
            }

    }.project(('key, 'token, 'doc))

    val model = trainBayesModel(placesPipe)
    model.write(SequenceFile(trainingmodel, Fields.ALL))


}
