package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.hp.hpl.jena.rdf.model.{ResourceFactory, Model, ModelFactory}
import collection.JavaConversions._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import java.io.{StringWriter, ByteArrayOutputStream}
import com.hp.hpl.jena.ontology.OntModel
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper
import com.sonar.dossier.dto.{ServiceType, ServiceProfileDTO}
import com.sonar.dossier.ScalaGoodies._
import com.hp.hpl.jena.shared.CannotEncodeCharacterException
import com.sonar.expedition.scrawler.util.RDFNamespaces._
import com.sonar.expedition.scrawler.jobs.ServiceProfileToRDFJob._
import com.hp.hpl.jena.sparql.vocabulary.FOAF


//import org.apache.clerezza.rdf.ontologies.FOAF

import com.hp.hpl.jena.vocabulary._

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class ServiceProfileToRDFJob(args: Args) extends Job(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val outputDir = args("output")
    val profileInput = args("profiles")
    val friendshipInput = args("friendships")

        val profiles = Tsv(profileInput, ('userProfileId, 'serviceType, 'json))
//    val profiles = SequenceFile(input, ('userProfileId, 'serviceType, 'json))
        val friendships = SequenceFile(friendshipInput, FriendTuple)

    val profileRdf = Tsv(outputDir + "/profileRdfTsv")
    val profileRdfSequence = SequenceFile(outputDir + "/profileRdfSequence")
    //  val profilesSmall = Tsv(outputDir + "/profilesSmall.tsv")

    //   val profilesSmall2 = Tsv(input, ('userProfileId, 'serviceType, 'json))
    val friendshipsSmall = Tsv(outputDir + "/frienshipsSmall.tsv")

    val models = profiles
            .read
            .filter('json) {
        json: String => json.startsWith("{")
    }
            .groupBy('userProfileId, 'serviceType) {
        _.head('json)
    }
            .flatMapTo('json -> 'model) {
        json: String => {
            ScrawlerObjectMapper.parseJson[ServiceProfileDTO](json) flatMap {
                serviceProfile =>
                    val model = ModelFactory.createDefaultModel()
                    //todo: find SIOC library
                    model.setNsPrefixes(Map[String, String](
                        "foaf" -> Foaf,
                        "sioc" -> Sioc,
                        "opo" -> Opo,
                        "owl" -> Owl,
                        "sonar" -> Sonar)
                    )

                    val resource = model.createResource(Foaf + serviceProfile.serviceType + ":" + serviceProfile.userId)
                            .addProperty(RDF.`type`, FOAF.Person)
                            .addProperty(model.createProperty(Foaf + "account"), createUserAccount(model, serviceProfile.serviceType, serviceProfile.userId))

                    if (serviceProfile.fullName != null) {
                        resource.addProperty(FOAF.name, serviceProfile.fullName)
                    }
                    if (serviceProfile.gender != null) {
                        resource.addProperty(FOAF.gender, serviceProfile.gender.toString)
                    }
                    if (serviceProfile.birthday != null) {
                        resource.addProperty(FOAF.birthday, serviceProfile.birthday.toString)
                    }
                    if (serviceProfile.aliases != null && serviceProfile.aliases.email != null) {
                        resource.addProperty(FOAF.mbox, model.createResource("mailto:" + serviceProfile.aliases.email))
                    }
                    if (serviceProfile.photoUrl != null) {
                        resource.addProperty(FOAF.depiction, model.createResource(serviceProfile.photoUrl))
                    }
                    //add sameAs
                    if (serviceProfile.aliases != null && serviceProfile.aliases.username != null) {
                        resource.addProperty(model.createProperty(Owl + "sameAs"), model.createResource(Foaf + serviceProfile.serviceType + ":" + serviceProfile.aliases.username))
                    }
                    //add known correlations
                    if (serviceProfile.aliases != null && serviceProfile.aliases.facebook != null && serviceProfile.serviceType != ServiceType.facebook) {
                        resource.addProperty(model.createProperty(Foaf + "account"), createUserAccount(model, ServiceType.facebook, serviceProfile.aliases.facebook))
                    }
                    if (serviceProfile.aliases != null && serviceProfile.aliases.twitter != null && serviceProfile.serviceType != ServiceType.twitter) {
                        resource.addProperty(model.createProperty(Foaf + "account"), createUserAccount(model, ServiceType.twitter, serviceProfile.aliases.twitter))
                    }

                    //todo: add a seeAlso for id or username version of accountName?

                    val strWriter = new StringWriter
                    try {
                        model.write(strWriter, rdfOutputFormat)
                        val str = stripHeader(rdfOutputFormat, strWriter.toString, serviceProfile)
                        Some(str)
                    } catch {
                        case cece: CannotEncodeCharacterException => throw new RuntimeException("Failed creating model for " + json, cece)
                    } finally {
                        strWriter.close()
                    }

            }

        }
    }
    models.write(profileRdf)
//    models.write(profileRdfSequence)


    def createUserAccount(model: Model, serviceType: ServiceType, serviceId: String) = {
        model.createResource(Sioc + serviceType.toString + ":" + serviceId)
            .addProperty(RDF.`type`, model.createResource(Sioc + "UserAccount"))
            .addProperty(model.createProperty(Sioc + "follows"), model.createResource(Sioc + serviceType.toString + ":rogchang")
                .addProperty(RDF.`type`, model.createResource(Sioc + "UserAccount")))
            .addProperty(FOAF.accountServiceHomepage, model.createResource(getServiceHome(serviceType)))
            .addProperty(FOAF.accountName, serviceId)
    }

    def getServiceHome(serviceType: ServiceType) = {
        serviceType match {
            case ServiceType.facebook => FacebookHome
            case ServiceType.twitter => TwitterHome
            case ServiceType.foursquare => FoursquareHome
            case ServiceType.linkedin => LinkedinHome
            case _ => SonarHome
        }
    }

    /**
     *
     * @param format
     * @param strToStrip
     * @param serviceProfile
     * @return
     */
    def stripHeader(format: String, strToStrip: String, serviceProfile: ServiceProfileDTO) = {
       /*
        * <rdf:RDF
        *      xmlns:sonar="http://sonar.me/ns#"
        *      xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        *      xmlns:foaf="http://xmlns.com/foaf/0.1/"
        *      xmlns:opo="http://ggg.milanstankovic.org/opo/ns#"
        *      xmlns:sioc="http://rdfs.org/sioc/ns#">
        *  </rdf:RDF>
        *
        *  @prefix sonar:   <http://sonar.me/#> .
        *  @prefix sioc:    <http://rdfs.org/sioc/ns#> .
        *  @prefix opo:     <http://ggg.milanstankovic.org/opo/ns#> .
        *  @prefix foaf:    <http://xmlns.com/foaf/0.1/> .
        *  @prefix owl:     <http://www.w3.org/2002/07/owl#> .
        */
        format match {
            case "N3" => {
                val lastPrefix = strToStrip.lastIndexOf("@prefix")
                strToStrip.substring(strToStrip.indexOf("\n\n", lastPrefix)).trim
            }
            case "RDF/XML-ABBREV" => strToStrip.substring(strToStrip.indexOf("<foaf:Person"), strToStrip.lastIndexOf("</rdf:RDF>"))
            case _ => strToStrip
        }
    }

}

object ServiceProfileToRDFJob {
    val TwitterHome = "http://www.twitter.com"
    val FacebookHome = "http://www.facebook.com"
    val FoursquareHome = "http://www.foursquare.com"
    val LinkedinHome = "http://www.linkedin.com"
    val SonarHome = "http://www.sonar.me"

    val rdfOutputFormat = "N3" // RDF/XML-ABBREV | N3
}