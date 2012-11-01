package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.util.{RdfFormat, Tuples}
import collection.JavaConversions._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.vocabulary.RDF
import com.sonar.expedition.scrawler.util.RDFNamespaces._
import java.io.StringWriter
import com.hp.hpl.jena.shared.CannotEncodeCharacterException
import com.sonar.expedition.scrawler.rdf.RDFConversion
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv

class PlacesToRDF(args: Args) extends Job(args) with RDFConversion {
    val input = args("iPlaces")
    val output = args("oPlacesRDF")

    SequenceFile(input, Tuples.Place)
            .read
            .mapTo(('serType, 'venId, 'venName, 'venAddress, 'lat, 'lng) -> 'model) {
        in: (String, String, String, String, Double, Double) =>
            val (serType, venId, venName, venAddress, lat, lng) = in
            placeToRDF(serType, venId, venName, venAddress, lat, lng)
    }.write(TextLine(output))

}