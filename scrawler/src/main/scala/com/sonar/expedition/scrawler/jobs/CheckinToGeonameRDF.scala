package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, SequenceFile, Args, Job}
import com.sonar.expedition.scrawler.util.Tuples
import collection.JavaConversions._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.hp.hpl.jena.rdf.model.ModelFactory

class CheckinToGeonameRDF(args: Args) extends Job(args) {
    val input = args("input")
    val outputDir = args("output")

    val checkins = SequenceFile(input, Tuples.Place)
    val geonameRDF = Tsv(outputDir + "/geonameRdf_tsv")

    val checkinsSmall = Tsv(outputDir + "checkinsSmall_tsv")


    val gn = "http://www.geonames.org/ontology#"
    val wgs84_pos = "http://www.w3.org/2003/01/geo/wgs84_pos#"

    val model = ModelFactory.createDefaultModel()
    model.setNsPrefixes(Map[String, String](
                            "gn" -> gn,
                            "wgs84_pos" -> wgs84_pos)
                        )

    model.createResource()
    checkins
        .read
        .write(checkinsSmall)
}