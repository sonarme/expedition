package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, TextLine, Job, Args}
import cascading.tuple.Fields
import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sonar.expedition.scrawler.util.CommonFunctions._


// Use args:
// STAG while local testing: --rpcHost 184.73.11.214 --ppmap 10.4.103.222:184.73.11.214,10.96.143.88:50.16.106.193
// STAG deploy: --rpcHost 10.4.103.222
class BuzzToCassandra(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val buzzTextFile = args("buzzScore")

    val buzz = TextLine(buzzTextFile).read
    .flatMapTo('line ->('venName, 'buzzCount, 'buzzScore, 'goldenId)) {
        line: String => {
            line match {
                case BuzzFromText(venName, buzzCount, buzzScore, goldenId) => {
                    Some((venName, buzzCount, buzzScore, goldenId))
                }
                case _ => {
                    println("Coudn't extract line using regex: " + line)
                    None
                }
            }
        }
    }

    val count = buzz.map(('buzzCount, 'goldenId) -> ('rowKey, 'columnName, 'columnValue)) {
        fields: (String, String) =>
            val (buzzcount, golden) = fields
            val row = golden + "_normalizedBuzz"
            val colName = "buzzCount"
            val colVal = buzzcount.toDouble
            (row, colName, colVal)
    }.project('rowKey, 'columnName, 'columnValue)

    val score = buzz.map(('buzzScore, 'goldenId) -> ('rowKey, 'columnName, 'columnValue)) {
        fields: (String, String) =>
            val (buzzscore, golden) = fields
            val row = golden + "_normalizedBuzz"
            val colName = "buzzScore"
            val colVal = buzzscore.toDouble
            (row, colName, colVal)
    }.project('rowKey, 'columnName, 'columnValue)

    val output = count ++ score
    val timeSeriesCassandra = output
            .write(
        CassandraSource(
            rpcHost = rpcHostArg,
            privatePublicIpMap = ppmap,
            keyspaceName = "dossier",
            columnFamilyName = "MetricsVenueStatic",
            scheme = WideRowScheme(keyField = 'rowKey)
        )
    )
}
