package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding._
import cascading.tuple.Fields
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv

class AggregateMetricsJob(args: Args) extends Job(args) {
    val staticInputFiles = args("staticInputFiles").split(',')
    val dealsOutput = args("dealsOutput")
    val metricsOut = args("metricsOut")
    val allPipes = staticInputFiles.map(file => SequenceFile(file, ('rowKey, 'columnName, 'columnValue)).read).reduce(_ ++ _)
    val metrics = allPipes.mapTo(('rowKey, 'columnName, 'columnValue) ->('venueId, 'metric, 'value)) {
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
