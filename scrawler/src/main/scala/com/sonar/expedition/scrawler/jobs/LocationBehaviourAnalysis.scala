package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Job, RichPipe, Args, TextLine}
import com.sonar.expedition.scrawler.pipes._
import cascading.tuple.Fields
import java.util.{Calendar, Date}
import java.util
import com.sonar.expedition.scrawler.util.{Levenshtein, EmployerCheckinMatch, StemAndMetaphoneEmployer, LocationScorer}
import scala.Some
import scala.Some
import com.sonar.expedition.scrawler.objs.serializable.LuceneIndex
import scala.Some
import com.sonar.dossier.service.PrecomputationSettings
import ch.hsr.geohash.GeoHash
import java.text.DecimalFormat

/*
 BEFORE RUNING THIS MAKE SURE TO build the bayes model by running this
 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysisBayesModel --hdfs --placesData "/tmp/places_dump_US.geojson.txt" --bayesmodelforvenuetype "/tmp/bayesmodelforvenuetype"


 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysis --hdfs --timedifference "6" --checkindata "/tmp/checkin_nomsg.txt" --



  "/tmp/output.txt" --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox" --bayesmodel "/tmp/bayesmodel" --training "/tmp/training" --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"  --placesData "/tmp/places_dump_US.geojson.txt --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis""

 // need to integrate the data from cloudmade and cross join with the output of dataanalyser to get the male and user profile info

 com.sonar.expedition.scrawler.jobs.LocationBehaviourAnalysis --hdfs --checkindata "/tmp/checkinDatatest.txt" --output "/tmp/output.txt"
 --chkinop "/tmp/chkinop" --chkinoptimebox "/tmp/chkinoptimebox"
 --bayesmodelforvenuetype "/tmp/bayesmodelforvenuetype" --training "/tmp/training"
 --trainingclassified "/tmp/trainingclassified" --trainingclassifiedfinal "/tmp/trainingclassifiedfinal"
 --placesData "/tmp/places_dump_US.geojson.txt" --locationBehaviourAnalysis "/tmp/locationBehaviourAnalysis"
 --timedifference "24" --geohashsectorsize "20"
*/
// not finished, find type of places that people come from and go to
class LocationBehaviourAnalysis(args: Args) extends Job(args) with LocationBehaviourAnalysePipe with CheckinGrouperFunction {

    val chkindata = TextLine(args("checkindata"))
    val chkindataoutput = TextLine(args("output"))
    val bayesmodel = args("bayesmodelforvenuetype")
    val training = args("training")
    val trainingclassified = args("trainingclassified")
    val trainingclassifiedfinal = args("trainingclassifiedfinal")
    val locationBehaviourAnalysis = args("locationBehaviourAnalysis")
    val timedifference = args("timedifference")
    val geohashsectorsize = args("geohashsectorsize")
    val prodtest = args.getOrElse("prodtest", "0").toInt
    val placesData = args("placesData")

    val chkinpipe = unfilteredCheckinsLatLon(chkindata).filter('venName) {
        fields: (String) =>
            val (venname) = fields
            (venname != None || venname != null || venname.trim != "")
    }.project(Fields.ALL)
    //'keyid, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime,'ghash, 'latitude, 'longitude, 'dayOfYear, 'hour

    val chkinpipe1 = chkinpipe.project(('keyid, 'venName, 'chknTime, 'lat, 'lng)).rename(('lat, 'lng) ->('latitude, 'longitude))
    val chkinpipe2 = chkinpipe.project(('venName, 'keyid, 'chknTime, 'lat, 'lng)).rename(('venName, 'keyid, 'chknTime, 'lat, 'lng) ->('venName1, 'keyid1, 'chknTime1, 'latitude1, 'longitude1))

    var chkinpipefileterdtime = filterTime(chkinpipe1, chkinpipe2, timedifference, geohashsectorsize, prodtest)
            .project(Fields.ALL)
            .write(chkindataoutput)

    // do also for ghash2 and output none for no matching and do classify later
    /*val classificationByBayesModel = classifyTFIDF(bayesmodel, chkinpipefileterdtime)
          .write(TextLine(locationBehaviourAnalysis))
*/

}
