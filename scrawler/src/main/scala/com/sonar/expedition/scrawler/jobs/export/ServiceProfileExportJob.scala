package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.{GenderFromNameProbability, DTOProfileInfoPipe}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.SequenceFile
import java.nio.ByteBuffer
import com.sonar.dossier.dto.{ServiceType, Gender, ServiceProfileLink, ServiceProfileDTO}
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import ServiceProfileExportJob._
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.jobs.DefaultJob

class ServiceProfileExportJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe {
    val rpcHostArg = args("rpcHost")
    val output = args("canonicalProfilesOut")

    val export = args.optional("export").map(_.toBoolean).getOrElse(true)
    val profileViews = args.optional("profileViews").map(_.toBoolean).getOrElse(true)

    val serviceProfileFile = SequenceFile(output + "_ServiceProfile", Tuples.ProfileIdDTO)
    val profileViewFile = SequenceFile(output + "_ProfileView", Tuples.ProfileIdDTO)

    def parseProfiles(pipe: RichPipe) = pipe.flatMapTo('jsondataBuffer -> Tuples.ProfileIdDTO) {
        jsondataBuffer: ByteBuffer =>
            if (jsondataBuffer == null || !jsondataBuffer.hasRemaining) None
            else {
                val profile = try {
                    ProfileSerializer.fromByteBuffer(jsondataBuffer)
                } catch {
                    case jpe: com.fasterxml.jackson.core.JsonParseException =>
                        throw new RuntimeException("reading profile: " + StringSerializer.get().fromByteBuffer(jsondataBuffer), jpe)
                }
                require(profile.serviceType != null)
                require(profile.userId != null)
                Some((profile.link, profile, profile.serviceType))
            }
    }

    if (export) {
        if (!profileViews) {
            // Load ServiceProfile CF
            val serviceProfiles = CassandraSource(
                rpcHost = rpcHostArg,
                additionalConfig = ppmap(args),
                keyspaceName = "dossier",
                columnFamilyName = "ServiceProfile",
                scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
                    nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
            ).read.filter('columnNameBuffer) {
                columnNameBuffer: ByteBuffer =>
                    val columnName = StringSerializer.get().fromByteBuffer(columnNameBuffer)
                    // filter out id and link columns
                    columnName.startsWith("data:") || columnName.contains(":data:")
            }
            parseProfiles(serviceProfiles).write(serviceProfileFile)
        } else {

            // Load ProfileView CF
            val profileViews = CassandraSource(
                rpcHost = rpcHostArg,
                additionalConfig = ppmap(args),
                keyspaceName = "dossier",
                columnFamilyName = "ProfileView",
                scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
                    nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
            ).read
            parseProfiles(profileViews).write(profileViewFile)
        }
    }
    else {

        // Combine profile pipes
        val allProfiles =
            IterableSource(Seq(
                (ServiceProfileLink(ServiceType.foursquare, "x"), ServiceProfileDTO(ServiceType.foursquare, "sonar"), ServiceType.sonar)
            ), Tuples.ProfileIdDTO).read

        //serviceProfileFile.read ++ profileViewFile.read

        // Read Correlation CF
        val correlation = IterableSource(Seq(("x", ServiceProfileLink(ServiceType.foursquare, "x"))), Tuples.Correlation).read
        //SequenceFile(args("correlationIn"), Tuples.Correlation).read

        val correlatedProfiles =
        // join correlation
            allProfiles.leftJoinWithSmaller('profileId -> 'correlationSPL, correlation).discard('correlationSPL)
                    // replace non-existing correlation with profile id temporary correlation id
                    .map(('profileId, 'correlationId) -> 'correlationId) {
                in: (ServiceProfileLink, String) =>
                    val (profileId, correlationId) = in
                    if (correlationId == null) profileId.profileId else correlationId
            }
                    // aggregate profiles in the correlation
                    .groupBy('correlationId) {
                _.reduce('profileDto -> 'combinedProfile) {
                    (profileAgg: ServiceProfileDTO, profile: ServiceProfileDTO) =>
                        populateNonEmpty(profileAgg, profile)
                }
            }.rename('combinedProfile -> 'profileDto)
                    .map('profileDto -> 'profileDto) {
                profile: ServiceProfileDTO =>
                // infer gender from name if unknown
                    if (profile.gender == Gender.unknown)
                        profile.gender = GenderFromNameProbability.gender(profile.fullName)._1
                    profile
            }

        // expand correlation
        val expandedProfiles =
            correlatedProfiles.leftJoinWithSmaller('correlationId -> '_correlationId, correlation.rename('correlationId -> '_correlationId)).discard('_correlationId)
                    .map(('profileDto, 'correlationSPL) ->('profileId, 'serviceType)) {
                in: (ServiceProfileDTO, ServiceProfileLink) =>
                    val (profile, correlationSPL) = in
                    val spl = if (correlationSPL == null) profile.link else correlationSPL
                    (spl, spl.serviceType)
            }


        expandedProfiles.write(SequenceFile(output, Tuples.ProfileIdDTO))

        // Statistics about profiles
        val numCorrelated =
            correlatedProfiles.groupAll {
                _.size
            }.map(() -> 'statName) {
                u: Unit => "num_correlated"
            }.project('statName, 'size)
        val numServiceType =
            allProfiles.unique('profileId, 'serviceType).groupBy('serviceType) {
                _.size
            }.map('serviceType -> 'statName) {
                serviceType: ServiceType => "num_" + serviceType.name()
            }.project('statName, 'size)
        (numCorrelated ++ numServiceType).write(Tsv(output + "_stats", ('statName, 'size)))
    }
}

object ServiceProfileExportJob {
    val ProfileSerializer = JsonSerializer.get[ServiceProfileDTO](None)
}
