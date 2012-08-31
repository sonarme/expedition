package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, SequenceFile, Args, Job}
import cascading.tuple.Fields

class AggregateMetricsJob(args: Args) extends Job(args) {
    val sequenceOutputStatic = args("sequenceOutputStatic")
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val metrics = SequenceFile(sequenceOutputStatic, ('rowKey, 'columnName, 'columnValue)).read
            .mapTo(('rowKey, 'columnName, 'columnValue) ->('venueId, 'metric, 'value)) {
        in: (String, String, Double) =>
            val (rowKey, columnName, columnValue) = in
            val Array(venueId, metricPrefix) = rowKey.split("_", 2)
            (venueId, metricPrefix + "_" + columnName, columnValue)
    }

    SequenceFile(dealsOutput, DealAnalysis.DealsOutputTuple).joinWithLarger('goldenId -> 'venueId, metrics)
            .groupBy(DealAnalysis.DealsOutputTuple) {
        _.toList[(String, String)](('metric, 'value) -> 'metricValues)
    }.write(Tsv(metricsOut, Fields.ALL))

}
