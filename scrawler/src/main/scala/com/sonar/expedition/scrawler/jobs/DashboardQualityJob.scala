package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{SequenceFile, Tsv, Job, Args}
import ch.hsr.geohash.GeoHash
import me.prettyprint.hector.api.beans.Composite
import com.sonar.dossier.service.PrecomputationSettings
import com.sonar.scalding.cassandra.{CassandraSource, WideRowScheme}

class DashboardQualityJob(args: Args) extends Job(args) {
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
    venueIds.joinWithSmaller('venueId -> 'venueId, placeClassification).unique('venueId, 'venName, 'venueLat, 'venueLng).map(('venueId, 'venName, 'venueLat, 'venueLng) ->('rowKey, 'columnName, 'columnValue)) {
        in: (String, String, Double, Double) =>
            val (venueId, venueName, venueLat, venueLng) = in
            (
                    GeoHash.withCharacterPrecision(venueLat, venueLng, 3).toBase32,
                    new Composite(GeoHash.withBitPrecision(venueLat, venueLng, PrecomputationSettings.GEOHASH_BIT_LENGTH).longValue(): java.lang.Long, venueId),
                    venueName
                    )


    }.write(CassandraSource(
        rpcHost = rpcHostArg,
        privatePublicIpMap = ppmap,
        keyspaceName = "dossier",
        columnFamilyName = "MetricsVenues",
        scheme = WideRowScheme(keyField = 'rowKey)
    ))

}
