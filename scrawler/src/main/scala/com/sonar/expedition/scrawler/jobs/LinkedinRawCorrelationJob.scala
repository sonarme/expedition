package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Job, Tsv, Args}
import com.sonar.scalding.cassandra._
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{LongSerializer, StringSerializer}
import com.sonar.dossier.ScalaGoodies._
import com.sonar.scalding.cassandra.NarrowRowScheme
import com.sonar.scalding.cassandra.CassandraSource
import com.sonar.dossier.dto.{ServiceProfileDTO, ServiceType, ServiceProfileLink}
import com.sonar.dossier.dao.cassandra.{RawCorrelation, ServiceProfileLinkSerializer}
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import org.apache.cassandra.utils.ByteBufferUtil
import collection.JavaConversions._

class LinkedinRawCorrelationJob(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")

    CassandraSource(
        rpcHost = rpcHostArg,
        keyspaceName = "dossier",
        columnFamilyName = "ProfileView",
        scheme = WideRowScheme(keyField = 'privacySPLB,
            nameField = ('splB, 'profile))
    )
            .flatMapTo(('privacySPLB, 'splB, 'profile) ->('rawCorrelationRowKey, 'rawCorrelationData, 'empty)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) => {
            val (_, splB, profileB) = in
            // target user's service profile link
            val spl = ServiceProfileLinkSerializer.fromByteBuffer(splB)
            // if the target user is from linkedin
            if (spl.serviceType == ServiceType.linkedin) {
                // deserialize the profile
                val profile = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(profileB)
                // if the profile has a correlation for tiwtter
                if (profile.aliases.twitter == null) None
                else {
                    val rawCorrelationRowKey = "linkedin-" + profile.userId
                    val rawCorrelation = new RawCorrelation(List(profile.link, ServiceProfileLink(ServiceType.twitter, profile.aliases.twitter)))
                    val rawCorrelationB = JsonSerializer.get[RawCorrelation]().toByteBuffer(rawCorrelation)
                    Some((rawCorrelationRowKey, rawCorrelationB, ByteBufferUtil.EMPTY_BYTE_BUFFER))
                }
            } else None
        }
    }.project(('rawCorrelationRowKey, 'rawCorrelationData, 'empty)).write(
        CassandraSource(
            rpcHost = rpcHostArg,
            keyspaceName = "dossier",
            columnFamilyName = "RawCorrelation",
            scheme = WideRowScheme(keyField = 'rawCorrelationRowKey)
        )
    )
}
