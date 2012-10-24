package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.sonar.expedition.scrawler.jena.vocabulary.OPO
import com.hp.hpl.jena.sparql.vocabulary.FOAF
import com.sonar.dossier.ScalaGoodies._
import com.sonar.expedition.scrawler.util.Tuples
import com.hp.hpl.jena.update.UpdateExecutionFactory
import com.hp.hpl.jena.sparql.modify.request.UpdateDeleteInsert
import java.text.SimpleDateFormat
import com.sonar.dossier.dto.ServiceProfileDTO
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.TextLine
import com.hp.hpl.jena.rdf.model.{StmtIterator, ResourceFactory, ModelFactory}
import com.hp.hpl.jena.vocabulary._
import com.hp.hpl.jena.sparql.core.DatasetImpl


// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class ServiceProfileToRDFJob(args: Args) extends Job(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val outputDir = args("output")
    val input = args("input")

    //    val profilesTsv = Tsv("/Users/rogchang/Desktop/rdf/serviceProfiles.tsv", ProfileTuple) //('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
    val profiles = SequenceFile(input, Tuples.Profile)
    //    val friendships = SequenceFile("/Users/rogchang/Desktop/rdf/friendship_prod_0905_small", FriendTuple)


    //    val input = SequenceFile(src, ProfileTuple)
    /*

        val testOutput = Tsv(outputDir + "/test.tsv")
        val testOutput2 = Tsv(outputDir + "/test2.tsv")
    */

    val profileRdf = TextLine(outputDir + "/profileRdf")
    //  val profilesSmall = Tsv(outputDir + "/profilesSmall.tsv")

    //   val profilesSmall2 = Tsv(input, ('userProfileId, 'serviceType, 'json))

    val foaf = "http://xmlns.com/foaf/0.1/"
    val sioc = "http://rdfs.org/sioc/ns#"
    val opo = OPO.NS
    val sonar = "http://sonar.me/ns#"


    val models = profiles
            .read
            .mapTo('profile -> 'model) {
        serviceProfile: ServiceProfileDTO => {
            val model = ModelFactory.createDefaultModel()
            //todo: find SIOC library

            import collection.JavaConversions._
            model.setNsPrefixes(Map(
                "foaf" -> foaf,
                "sioc" -> sioc,
                "opo" -> opo,
                "sonar" -> sonar): java.util.Map[String, String]
            )

            val resource = model.createResource()
                    .addProperty(RDF.`type`, FOAF.Person)
                    .addProperty(ResourceFactory.createProperty(foaf + "account"), model.createResource(sioc + serviceProfile.serviceType + ":" + serviceProfile.userId))
                    .addProperty(RDF.`type`, ResourceFactory.createProperty(sioc + "UserAccount"))
            // hacky
            val dateFormat = new SimpleDateFormat("MM-dd")
            Map(
                FOAF.name -> serviceProfile.fullName,
                FOAF.gender -> serviceProfile.gender.name(),
                FOAF.birthday -> ??(serviceProfile.birthday).map(dateFormat.format).orNull,
                FOAF.accountName -> serviceProfile.userId,
                FOAF.mbox -> serviceProfile.aliases.email,
                FOAF.homepage -> serviceProfile.url,
                model.createProperty(foaf + "twitterId") -> serviceProfile.aliases.twitter,
                model.createProperty(foaf + "facebookId") -> serviceProfile.aliases.facebook
            ) foreach {
                case (p, value) if value != null =>
                    resource.addProperty(p, value)
            }
            val update = updateFromStatements(model.listStatements)
            val ds = new DatasetImpl(model)
            // ds.asDatasetGraph().find()
            // ...
            UpdateExecutionFactory.createRemote(update, "http://ec2-107-22-231-224.compute-1.amazonaws.com:3030/data/update").execute()
            /*
                       val strWriter = new StringWriter

           try {
                model.write(strWriter, "RDF/XML-ABBREV")
                Some(strWriter.toString)
            } catch {
                case cece: CannotEncodeCharacterException => throw new RuntimeException("Failed creating model for " + serviceProfile, cece)
            } finally {
                strWriter.close()
            }*/
            ""
        }
    }
    models
            .write(profileRdf)


}

