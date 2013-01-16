package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.SequenceFile
import java.nio.ByteBuffer
import com.sonar.dossier.dto.{ServiceProfileLink, ServiceProfileDTO}
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import ServiceProfileExportJob._

class ServiceProfileExportJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe {
    val rpcHostArg = args("rpcHost")
    val output = args("canonicalProfilesOut")
    val serviceProfiles = CassandraSource(
        rpcHost = rpcHostArg,
        additionalConfig = ppmap(args),
        keyspaceName = "dossier",
        columnFamilyName = "ServiceProfile",
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    val profileViews = CassandraSource(
        rpcHost = rpcHostArg,
        additionalConfig = ppmap(args),
        keyspaceName = "dossier",
        columnFamilyName = "ProfileView",
        scheme = WideRowScheme(keyField = 'userProfileIdBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    val jsonData = serviceProfiles ++ profileViews

    val allProfiles =
        jsonData.flatMapTo('jsondataBuffer ->('profileId, 'profile)) {
            jsondataBuffer: ByteBuffer =>
                if (jsondataBuffer == null || !jsondataBuffer.hasRemaining) None
                else {
                    val profile = ProfileSerializer.fromByteBuffer(jsondataBuffer)
                    require(profile.serviceType != null)
                    require(profile.userId != null)
                    Some((profile.link, profile))
                }

        }

    val correlation = SequenceFile(args("correlationIn"), Tuples.Correlation).read

    val correlatedProfiles =
    // join correlation
        allProfiles.leftJoinWithSmaller('profileId -> 'correlationSPL, correlation).discard('correlationSPL)
                // replace non-existing correlation with profile id temporary correlation id
                .map(('profileId, 'correlationId) -> 'correlationId) {
            in: (ServiceProfileLink, ServiceProfileLink) =>
                val (profileId, correlationId) = in
                if (correlationId == null) profileId else correlationId
        }
                // aggregate profiles in the correlation
                .groupBy('correlationId) {
            _.reduce('profile -> 'combinedProfile) {
                (profileAgg: ServiceProfileDTO, profile: ServiceProfileDTO) =>
                    populateNonEmpty(profileAgg, profile)
            }
        }.rename('combinedProfile -> 'profile)

    // expand correlation
    val expandedProfiles =
        correlatedProfiles.leftJoinWithSmaller('correlationId -> '_correlationId, correlation.rename('correlationId -> '_correlationId)).discard('_correlationId)
                .map(('correlationId, 'correlationSPL) -> 'profileId) {
            in: (ServiceProfileLink, ServiceProfileLink) =>
                val (profileId, correlationProfileId) = in
                val spl = if (correlationProfileId == null) profileId else correlationProfileId
                spl
        }


    expandedProfiles.write(SequenceFile(output, Tuples.Profile))

    correlatedProfiles.groupAll {
        _.size
    }.write(Tsv(output + "_stats", ('size)))

}

object ServiceProfileExportJob {
    val ProfileSerializer = JsonSerializer.get[ServiceProfileDTO](None)
}
