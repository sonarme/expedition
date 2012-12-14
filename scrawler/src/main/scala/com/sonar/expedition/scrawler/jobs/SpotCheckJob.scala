package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Tsv, Args, Job, SequenceFile}
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.{ByteBufferSerializer, StringSerializer}
import com.sonar.expedition.scrawler.util.Tuples
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.HSlicePredicate
import java.util.concurrent.atomic.AtomicInteger

class SpotCheckJob(args: Args) extends Job(args) with CheckinSource {
    val input = args("input")
    SequenceFile(input, Tuples.Profile).read.project('serType, 'serCheckinID).filter(
        'serType, 'serCheckinID) {
        in: (String, String) =>
            val id = in._1 + ":" + in._2
            val columns = SpotCheckJob.Template.queryColumns(id, new HSlicePredicate(ByteBufferSerializer.get()) setStartOn (null) setEndOn (null) setCount (1))
            val notFound = !columns.hasResults
            if (notFound) {
                println("Not found: " + id)
                SpotCheckJob.notfound.incrementAndGet()
            }
            val newCount = SpotCheckJob.count.incrementAndGet()
            if (newCount % 256 == 0) {
                println("Not found: " + SpotCheckJob.notfound.get() + "   Count: " + newCount)
            }

            notFound
    }.write(Tsv("notfound", ('serType, 'serCheckinID)))

}

object SpotCheckJob {
    var count = new AtomicInteger(0)
    var notfound = new AtomicInteger(0)
    lazy val Template = new ThriftColumnFamilyTemplate[String, ByteBuffer](HFactory.createKeyspace("dossier", HFactory.getOrCreateCluster("SonarProd2", "50.19.128.170:9160")),
        "Checkin",
        StringSerializer.get(),
        ByteBufferSerializer.get())
}
