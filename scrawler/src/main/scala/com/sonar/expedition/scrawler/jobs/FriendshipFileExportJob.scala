package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, Args, Job, SequenceFile}
import com.sonar.scalding.cassandra._
import com.sonar.expedition.scrawler.pipes.{FriendGrouperFunction, DTOProfileInfoPipe}
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import com.sonar.expedition.scrawler.util.CommonFunctions._
import FriendshipFileExportJob._

class FriendshipFileExportJob(args: Args) extends Job(args) with DTOProfileInfoPipe with FriendGrouperFunction {
    val output = args("output")
    TextLine(args("input")).read.flatMapTo('line -> FriendTuple) {
        line: String =>
            line match {
                // change when we use prod data
                case FriendProdExtractLine(id, serviceType, serviceId, _) => Some((id, serviceType, hashed(serviceId)))
                case FriendExtractLine(id, other2, serviceId, serviceType, _, other) => Some((id, serviceType, hashed(serviceId)))
                case _ => None
            }
    }.unique(FriendTuple).write(SequenceFile(output, FriendTuple))
            .limit(180000).write(SequenceFile(output + "_small", FriendTuple))
}

object FriendshipFileExportJob {
    val FriendExtractLine = """([a-zA-Z\d\-]+):(.*?)"id":"(.*?)","service_type":"(.*?)","name":"(.*?)","photo(.*)""".r
    val FriendProdExtractLine = """([a-zA-Z\d\-]+)::(twitter|facebook|foursquare|linkedin|sonar)::([a-zA-Z\d\-]+)::(.*)""".r

}
