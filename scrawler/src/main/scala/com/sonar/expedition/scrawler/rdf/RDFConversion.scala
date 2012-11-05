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
import org.openrdf.elmo.ElmoModule
import org.openrdf.elmo.sesame.SesameManagerFactory
import org.openrdf.rio.RDFFormat
import javax.xml.namespace.QName
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.ontologies.foaf.{Image, Person}
import org.openrdf.rio.ntriples.NTriplesWriter
import org.openrdf.rio.rdfxml.RDFXMLWriter
import org.openrdf.rio.turtle.TurtleWriter

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

    def profileToRDF(serviceProfile: ServiceProfileDTO) = {
        val module = new ElmoModule()
        module.addConcept(classOf[Person])
        module.addConcept(classOf[Image])
        val factory = new SesameManagerFactory(module)
        val manager = factory.createElmoManager()

        val id = new QName("http://xmlns.com/foaf/0.1/Person/", serviceProfile.serviceType.toString + ":" + serviceProfile.userId)
        val person = manager.designate(id, classOf[Person])

        person.setFoafName(Set[String](serviceProfile.fullName))


        if (serviceProfile.fullName != null) {
            person.setFoafName(Set[String](serviceProfile.fullName))
        }
        if (serviceProfile.gender != null) {
            person.setFoafGender(serviceProfile.gender.toString)
        }
        if (serviceProfile.birthday != null) {
            person.setFoafBirthday(serviceProfile.birthday)
        }
        if (serviceProfile.aliases != null && serviceProfile.aliases.email != null) {
            person.setFoafMbox(Set[String](serviceProfile.aliases.email))
        }
        if (serviceProfile.photoUrl != null) {
            val photoId = new QName(serviceProfile.photoUrl)
            val photo = manager.designate(photoId, classOf[Image])
            person.setFoafDepiction(Set[Image](photo))
        }

//        val file = new File("/Users/rogchang/Desktop/test.rdf")
//        val writer = new FileWriter(file)
        val writer = new StringWriter()
        try {
            //        manager.getConnection.export(new NTriplesWriter(writer))
            manager.getConnection.export(new RDFXMLWriter(writer))
            //        manager.getConnection.export(new TurtleWriter(writer))
            writer.toString
        } catch {
            case e: Exception => throw new RuntimeException("Failed writing rdf", e)
        } finally {
            writer.close()
        }
    }
}
