package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer

/*
com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysisBayesModel --hdfs --placesData "/tmp/places_dump_US.geojson.txt" --bayestrainingmodelforlocationtype "/tmp/bayestrainingmodelforlocationtype"

 */
class LocationBehaviourAnalyseBayesModel(args: Args) extends LocationBehaviourAnalysePipe(args) {

    val trainingmodel = args("bayestrainingmodelforlocationtype")
    val placesData = args("placesData")

    val placesPipe = getLocationInfo(TextLine(placesData).read)
            .project(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum))
            .flatMapTo(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum) ->('key, 'token, 'doc)) {
        fields: (String, String, String, String, String, Int) =>
            val (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, docid) = fields
            val tokens = (StemAndMetaphoneEmployer.getStemmed(propertiesName) + " " + StemAndMetaphoneEmployer.getStemmed(propertiesTags) + " " + StemAndMetaphoneEmployer.getStemmed(classifiersCategory) + " "
                    + StemAndMetaphoneEmployer.getStemmed(classifiersType) + " " + StemAndMetaphoneEmployer.getStemmed(classifiersSubcategory)).split(" ");
            for (key <- tokens) yield (classifiersCategory.toLowerCase, key.trim, docid)

    }.project(('key, 'token, 'doc))

    val trainer = new BayesModelPipe(args)
    val model = trainer.trainBayesModel(placesPipe);
    model.write(SequenceFile(trainingmodel, Fields.ALL))


}
