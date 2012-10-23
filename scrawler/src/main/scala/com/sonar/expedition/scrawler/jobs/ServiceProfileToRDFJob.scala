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
    val input = args("input")

        val profiles = Tsv(input, ('userProfileId, 'serviceType, 'json))
//    val profiles = SequenceFile(input, ('userProfileId, 'serviceType, 'json))
    //    val friendships = SequenceFile("/Users/rogchang/Desktop/rdf/friendship_prod_0905_small", FriendTuple)


    //    val input = SequenceFile(src, ProfileTuple)
    /*

        val testOutput = Tsv(outputDir + "/test.tsv")
        val testOutput2 = Tsv(outputDir + "/test2.tsv")
    */

    val profileRdf = Tsv(outputDir + "/profileRdfTsv")
    val profileRdfSequence = SequenceFile(outputDir + "/profileRdfSequence")
    //  val profilesSmall = Tsv(outputDir + "/profilesSmall.tsv")

    //   val profilesSmall2 = Tsv(input, ('userProfileId, 'serviceType, 'json))

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
                            .addProperty(model.createProperty(Foaf + "account"), model.createResource()
                                .addProperty(model.createProperty(Foaf + "OnlineAccount"), model.createResource(serviceProfile.serviceType + ":" + serviceProfile.userId)
                                        .addProperty(RDF.`type`, model.createResource(Sioc + "UserAccount"))
                                        .addProperty(model.createProperty(Sioc + "follows"), model.createResource("http://www.linkedin.com/sGiYOiEnd1")
                                            .addProperty(RDF.`type`, model.createResource(Sioc + "UserAccount")))
                                        .addProperty(FOAF.accountServiceHomepage, model.createResource(getServiceHome(serviceProfile.serviceType)))
                                        .addProperty(FOAF.accountName, serviceProfile.userId)))

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
                        resource.addProperty(model.createProperty(Foaf + "account"), model.createResource()
                            .addProperty(FOAF.accountServiceHomepage, model.createResource(getServiceHome(serviceProfile.serviceType)))
                            .addProperty(FOAF.accountName, serviceProfile.aliases.facebook))
                    }
                    if (serviceProfile.aliases != null && serviceProfile.aliases.twitter != null && serviceProfile.serviceType != ServiceType.twitter) {
                        resource.addProperty(model.createProperty(Foaf + "account"), model.createResource()
                            .addProperty(FOAF.accountServiceHomepage, model.createResource(getServiceHome(serviceProfile.serviceType)))
                            .addProperty(FOAF.accountName, serviceProfile.aliases.twitter))
                    }

                    //todo: add a seeAlso for id or username version of accountName?

                    val strWriter = new StringWriter
                    try {
                        model.write(strWriter, "RDF/XML-ABBREV")
                        val str = strWriter.toString
                        //we strip the root element so that we can create one big document.  should put it back in somewhere
//                        <rdf:RDF
//                            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
//                            xmlns:sonar="http://sonar.me/#"
//                            xmlns:foaf="http://xmlns.com/foaf/0.1/"
//                            xmlns:opo="http://ggg.milanstankovic.org/opo/ns#"
//                            xmlns:sioc="http://rdfs.org/sioc/ns#">
//                        </rdf:RDF>
                        val strippedString = strWriter.toString.substring(str.indexOf("<foaf:Person"), str.lastIndexOf("</rdf:RDF>"))
                        Some("  " + strippedString.trim)
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

    def getServiceHome(serviceType: ServiceType) = {
        serviceType match {
            case ServiceType.facebook => FacebookHome
            case ServiceType.twitter => TwitterHome
            case ServiceType.foursquare => FoursquareHome
            case ServiceType.linkedin => LinkedinHome
            case _ => SonarHome
        }
    }

}

object ServiceProfileToRDFJob {
    val TwitterHome = "http://www.twitter.com"
    val FacebookHome = "http://www.facebook.com"
    val FoursquareHome = "http://www.foursquare.com"
    val LinkedinHome = "http://www.linkedin.com"
    val SonarHome = "http://www.sonar.me"
}