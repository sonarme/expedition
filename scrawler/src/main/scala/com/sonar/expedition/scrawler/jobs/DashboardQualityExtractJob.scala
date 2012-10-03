package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import ch.hsr.geohash.GeoHash
import me.prettyprint.hector.api.beans.Composite
import com.sonar.dossier.service.PrecomputationSettings
import cascading.tuple.Fields

class DashboardQualityExtractJob(args: Args) extends Job(args) {
    val rpcHostArg = args("rpcHost")
    val ppmap = args.getOrElse("ppmap", "")
    val placeClassification = SequenceFile(args("placeClassification"), PlaceClassification.PlaceClassificationOutputTuple)
    val sequenceInputStatic = args("inputStatic")
    val output = args("output")
    val venueIds = SequenceFile(sequenceInputStatic, ('rowKey, 'columnName, 'columnValue)).read.flatMapTo(('rowKey, 'columnName, 'columnValue) -> ('venueId)) {
        in: (String, String, Double) =>
            val (rowKey, columnName, columnValue) = in
            val Array(venueId, metric) = rowKey.split("_", 2)
            if (metric == "numCheckins" && columnName == "withProfile" && columnValue >= 500) Some(venueId)
            else None
    }
    venueIds.joinWithSmaller('venueId -> 'venueId, placeClassification).map(('venueLat, 'venueLng) -> ('geosector)) {
        in: (Double, Double) =>
            val (venueLat, venueLng) = in
            GeoHash.withCharacterPrecision(venueLat, venueLng, 4).toBase32
    }.write(Tsv(output, Fields.ALL, true, true))

    /*.write(CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "MetricsVenues",
        scheme = WideRowScheme(keyField = 'rowKey)
    ))*/

}
