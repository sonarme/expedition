package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, SequenceFile, Args, Job}
import com.sonar.expedition.scrawler.util.Tuples
import collection.JavaConversions._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.vocabulary.RDF
import com.sonar.expedition.scrawler.util.RDFNamespaces._
import java.io.StringWriter
import com.hp.hpl.jena.shared.CannotEncodeCharacterException

class CheckinToGeonameRDF(args: Args) extends Job(args) {
    val input = args("input")
    val outputDir = args("output")

    val checkins = SequenceFile(input, Tuples.Place)
//    val checkins = Tsv(input, Tuples.Place)
    val geonameRDF = Tsv(outputDir + "/geonameRdf_tsv")
    val geonameRDFSequence = SequenceFile(outputDir + "/geonameRdf_sequence")

    val checkinsSmall = Tsv(outputDir + "/checkinsSmall_tsv")

    val models = checkins
            .read
            .flatMapTo(('serType, 'venId, 'venName, 'venAddress, 'lat, 'lng) -> 'model) {
        x: (String, String, String, String, Double, Double) =>
            val (serType, venId, venName, venAddress, lat, lng) = x
            val model = ModelFactory.createDefaultModel()
            model.setNsPrefixes(Map[String, String](
                "gn" -> Gn,
                "wgs84_pos" -> Wgs84_pos,
                "foaf" -> Foaf,
                "owl" -> Owl,
                "rdf" -> Rdf,
                "rdfs" -> Rdfs,
                "v" -> Vcard,
                "sonar" -> Sonar)
            )

            model.createResource(Gn + venId)
                    .addProperty(RDF.`type`, model.createResource(Gn + "Feature"))
                    .addProperty(model.createProperty(Gn, "name"), venName)
                    .addProperty(model.createProperty(Wgs84_pos, "lat"), lat.toString)
                    .addProperty(model.createProperty(Wgs84_pos, "lng"), lng.toString)
                    .addProperty(model.createProperty(Vcard, "VCard"), model.createResource()
                        .addProperty(model.createProperty(Vcard, "adr"), model.createResource(Sonar + venId)
                            .addProperty(model.createProperty(Vcard, "street-address"), venAddress)))


            val strWriter = new StringWriter
            try {
                model.write(strWriter, "RDF/XML-ABBREV")
                val str = strWriter.toString
                //we strip the root element so that we can create one big document.  should put it back in somewhere
                val strippedString = strWriter.toString.substring(str.indexOf("<gn:Feature "), str.lastIndexOf("</rdf:RDF>"))
                Some("  " + strippedString.trim)
            } catch {
                case cece: CannotEncodeCharacterException => throw new RuntimeException("Failed creating model for " + venId, cece)
            } finally {
                strWriter.close()
            }
    }

    models.write(geonameRDF)
    models.write(geonameRDFSequence)
}