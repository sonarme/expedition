package com.sonar.expedition.scrawler.rdf

import com.hp.hpl.jena.rdf.model.ModelFactory
import com.sonar.expedition.scrawler.util.RDFNamespaces._
import scala.{Double, Some}
import com.hp.hpl.jena.vocabulary.RDF
import java.io.StringWriter
import com.sonar.expedition.scrawler.util.RdfFormat
import com.hp.hpl.jena.shared.CannotEncodeCharacterException
import scala.Predef._
import collection.JavaConversions._

trait RDFConversion {
    def placeToRDF(serType: String, venId: String, venName: String, venAddress: String, lat: Double, lng: Double, rdfFormat: RdfFormat.Value = RdfFormat.Ntriple) = {
        val model = ModelFactory.createDefaultModel()
        model.setNsPrefixes(
            LinkedGeo.NS ++
                    Map("sonarven" -> SonarVenue,
                        "sonarsvc" -> SonarService)
        )

        val canonicalVenueId = serType + "_" + venId
        model.createResource(SonarVenue + canonicalVenueId)
                .addProperty(RDF.`type`, LinkedGeo.Amenity)
                // TODO: not ideal
                .addProperty(LinkedGeo.contributor, model.createResource(SonarService + serType))
                .addLiteral(LinkedGeo.lat, lat)
                .addLiteral(LinkedGeo.long, lng)
                .addLiteral(LinkedGeo.point, lat.toString + " " + lng.toString)
                // TODO: parse the address into housenumber, street, etc.
                .addLiteral(LinkedGeo.street, venAddress)
                .addLiteral(LinkedGeo.label, venName)

        val strWriter = new StringWriter
        try {
            model.write(strWriter, rdfFormat.toString)
            strWriter.toString
        } catch {
            case cece: CannotEncodeCharacterException => throw new RuntimeException("Failed creating model for " + venId, cece)
        } finally {
            strWriter.close()
        }

    }
}
