package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import com.sonar.scalding.cassandra._
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.scalding.cassandra.WideRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.twitter.scalding.SequenceFile
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper
import com.sonar.dossier.dto.{RecentCheckin, ServiceProfileDTO}
import com.sonar.dossier.jackson.CassandraObjectMapperJava
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dao.cassandra.JSONSerializer
import ServiceProfileExportJob._

class ServiceProfileExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe {
    val columnFamily = args("columnFamily")
    val rpcHostArg = args("rpcHost")
    val output = args("output")
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

    val reducedProfiles =
        jsonData.flatMapTo('jsondataBuffer ->('profileId, 'profile)) {
            jsondataBuffer: ByteBuffer =>
                if (jsondataBuffer == null || !jsondataBuffer.hasRemaining) None
                else {
                    val profile = ProfileSerializer.fromByteBuffer(jsondataBuffer)
                    require(profile.serviceType != null)
                    require(profile.userId != null)
                    Some((profile.profileId, profile))
                }

        }.groupBy('profileId) {
            _.reduce('profile -> 'combinedProfile) {
                (profileAgg: ServiceProfileDTO, profile: ServiceProfileDTO) =>
                    populateNonEmpty(profileAgg, profile)
            }
        }.rename('combinedProfile -> 'profile)

    reducedProfiles.write(SequenceFile(output, Tuples.Profile))
            .limit(180000).write(SequenceFile(output + "_small", Tuples.Profile))
}

object ServiceProfileExportJob {
    val ProfileSerializer = new JSONSerializer(classOf[ServiceProfileDTO])
}
