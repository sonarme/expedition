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
    def placeToRDF(serType: String, venId: String, venName: String, venAddress: String, lat: Double, lng: Double) = {
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

        model.createResource(Vcard + venId)
                .addProperty(RDF.`type`, model.createResource(Vcard + "VCard"))
                .addProperty(model.createProperty(Vcard, "geo"), model.createResource()
                .addProperty(model.createProperty(Vcard + "latitude"), lat.toString)
                .addProperty(model.createProperty(Vcard + "longitude"), lng.toString))
                .addProperty(model.createProperty(Vcard, "adr"), model.createResource()
                .addProperty(model.createProperty(Vcard, "street-address"), venAddress)
                .addProperty(model.createProperty(Vcard, "locality"), "") //todo: get city from cassandra
                .addProperty(model.createProperty(Vcard, "postal-code"), "") //todo: get postal code from cassandra
                .addProperty(model.createProperty(Vcard, "country-name"), "")) //todo: get country from cassandra

        model.createResource(Gn + venId)
                .addProperty(RDF.`type`, model.createResource(Gn + "Feature"))
                .addProperty(model.createProperty(Owl + "sameAs"), model.createResource(Vcard + venId))
                .addProperty(model.createProperty(Gn, "name"), venName)
                .addProperty(model.createProperty(Wgs84_pos, "lat"), lat.toString)
                .addProperty(model.createProperty(Wgs84_pos, "lng"), lng.toString)


        val strWriter = new StringWriter
        try {
            model.write(strWriter, RdfFormat.Ntriple)
            strWriter.toString
        } catch {
            case cece: CannotEncodeCharacterException => throw new RuntimeException("Failed creating model for " + venId, cece)
        } finally {
            strWriter.close()
        }

    }
}
