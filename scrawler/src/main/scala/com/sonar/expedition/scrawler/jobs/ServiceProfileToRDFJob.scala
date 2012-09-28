package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.hp.hpl.jena.rdf.model.{ResourceFactory, Model, ModelFactory}
import ServiceProfileToRDFJob._
import collection.JavaConversions._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import java.io.ByteArrayOutputStream
import com.hp.hpl.jena.ontology.OntModel
import com.sonar.expedition.scrawler.jena.vocabulary.OPO
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper
import com.sonar.dossier.dto.ServiceProfileDTO
import com.hp.hpl.jena.sparql.vocabulary.FOAF
import thewebsemantic.vocabulary.Sioc
import com.sonar.dossier.ScalaGoodies._

//import org.apache.clerezza.rdf.ontologies.FOAF
import com.hp.hpl.jena.vocabulary._

// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class ServiceProfileToRDFJob(args: Args) extends Job(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val outputDir = args("output")
    val input = args("input")

    val profilesTsv = Tsv("/Users/rogchang/Desktop/rdf/serviceProfiles.tsv", ProfileTuple) //('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
    val profiles = SequenceFile(input, ('userProfileId, 'serviceType, 'json))
    //    val friendships = SequenceFile("/Users/rogchang/Desktop/rdf/friendship_prod_0905_small", FriendTuple)


    //    val input = SequenceFile(src, ProfileTuple)

    val testOutput = Tsv(outputDir + "/test.tsv")
    val testOutput2 = Tsv(outputDir + "/test2.tsv")

    val profileRdf = Tsv(outputDir + "/profileRdfTsv")
    val profileRdfSequence = SequenceFile(outputDir + "/profileRdfSequence")
    val profilesSmall = Tsv(outputDir + "/profilesSmall.tsv")

    val profilesSmall2 = Tsv(input, ('userProfileId, 'serviceType, 'json))

    val foaf = "http://xmlns.com/foaf/0.1/"
    val sioc = "http://rdfs.org/sioc/ns#"
    val opo = OPO.NS
    val sonar = "http://sonar.me/ns#"


    val serviceProfiles = profiles
        .read
        .unique('userProfileId, 'serviceType, 'json)
        .map('json -> 'serviceProfileDTO) {
            json: String => {
                val serviceProfileDTO = try { ScrawlerObjectMapper.mapper().readValue[ServiceProfileDTO](json, classOf[ServiceProfileDTO]) } catch {
                    case e: Exception => println(json); null
                }
                serviceProfileDTO
            }
        }.filter('serviceProfileDTO){sp: ServiceProfileDTO => sp != null && sp.viewingUser == null}
        .discard('json)

//    serviceProfiles
//        .write(profileRdf)


    val models = serviceProfiles
            .map('userProfileId -> 'userProfileIdHashMod) {
        userProfileId: String => {
            userProfileId.hashCode % NumGroupChunks
        }
    }.groupBy('userProfileIdHashMod) {
        groupBuilder => {
            groupBuilder.mapReduceMap(('userProfileId, 'serviceType, 'serviceProfileDTO) -> ('model)) { //map1
                in: (String, String, ServiceProfileDTO) => {
                    val (userProfileId, serviceType, serviceProfile) = in

                    val model = ModelFactory.createDefaultModel()
                    //todo: find SIOC library
                    model.setNsPrefixes(Map[String, String](
                        "foaf" -> foaf,
                        "sioc" -> sioc,
                        "opo" -> opo,
                        "sonar" -> sonar)
                    )


                    model.createResource()
                                                .addProperty(RDF.`type`, FOAF.Person)
                                                .addProperty(ResourceFactory.createProperty(foaf + "account"), model.createResource(sioc + serviceType + ":" + serviceProfile.userId)
                                                                                                                    .addProperty(RDF.`type`, ResourceFactory.createProperty(sioc + "UserAccount"))
                                                                                                                    .addProperty(FOAF.name, ??(serviceProfile.fullName).getOrElse(""))
                                                                                                                    .addProperty(FOAF.gender, ??(serviceProfile.gender).getOrElse("").toString)
                                                                                                                    .addProperty(FOAF.birthday, ??(serviceProfile.birthday).getOrElse("").toString)
                                                                                                                    .addProperty(FOAF.accountName, ??(serviceProfile.userId).getOrElse(""))
                                                                                                                    .addProperty(FOAF.mbox, ??(serviceProfile.aliases.email).getOrElse(""))
                                                                                                                    .addProperty(model.createProperty(foaf + "twitterId"), ??(serviceProfile.aliases.twitter).getOrElse(""))
                                                                                                                    .addProperty(model.createProperty(foaf + "facebookId"), ??(serviceProfile.aliases.facebook).getOrElse(""))
                                                                                                                    .addProperty(FOAF.homepage, ??(serviceProfile.url).getOrElse("")))

                                  model

                }
            } { //reduce
                (model1: Model, model2: Model) => model1.add(model2)
            } { //map2
                (modelOut: Model) => {
                    val outputStream = new ByteArrayOutputStream()
                    modelOut.write(outputStream, "RDF/XML-ABBREV")
//                    modelOut.write(outputStream, "TURTLE")
                    outputStream.toString
                }
            }

        }
    }.discard('userProfileIdHashMod)

    models
        .write(profileRdf)

    models
        .write(profileRdfSequence)

}


object ServiceProfileToRDFJob {
    val NumGroupChunks = 100
}
