package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, TextLine, Job, Args}
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer
import com.sonar.expedition.scrawler.pipes.{LocationBehaviourAnalysePipe, BayesModelPipe}
import cascading.tuple.Fields

/*
com.sonar.expedition.scrawler.jobs.CategorisePlaceTypesFromPlaceNamesBuildBayesModel --hdfs
 --placesData "/tmp/places_dump_US.geojson.txt"
 --bayestrainingmodelforplacetype "/tmp/bayestrainingmodelforplacetype"
 */
class LocationBehaviourAnalyseBayesModel(args: Args) extends LocationBehaviourAnalysePipe(args) {


    val trainingmodel = args("bayestrainingmodelforplacetype")
    val placesData = args("placesData")

    val placesPipe = getLocationInfo(TextLine(placesData).read)
            .project(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum))
            .flatMapTo(('propertiesName, 'propertiesTags, 'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'linenum) ->('key, 'token, 'doc)) {
        fields: (String, String, String, String, String, Int) =>
            val (propertiesName, propertiesTags, classifiersCategory, classifiersType, classifiersSubcategory, docid) = fields
            val tokens = (StemAndMetaphoneEmployer.getStemmed(propertiesName) + " " + StemAndMetaphoneEmployer.getStemmed(propertiesTags) + " " + StemAndMetaphoneEmployer.getStemmed(classifiersCategory) + " "
                    + StemAndMetaphoneEmployer.getStemmed(classifiersType) + " " + StemAndMetaphoneEmployer.getStemmed(classifiersSubcategory)).split(" ");
            for (i <- 0 until tokens.size)
            yield (classifiersCategory.toLowerCase, tokens(i), docid + i)


    }.project(('key, 'token, 'doc))


    val trainer = new BayesModelPipe(args)
    val model = trainer.trainBayesModel(placesPipe);
    model.write(SequenceFile(trainingmodel, Fields.ALL))


}

