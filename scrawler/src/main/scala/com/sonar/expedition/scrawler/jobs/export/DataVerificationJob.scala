package com.sonar.expedition.scrawler.jobs.export

import com.twitter.scalding.{Tsv, Args, SequenceFile}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.expedition.scrawler.jobs.{CheckinSource, DefaultJob}
import com.sonar.dossier.dto.{CheckinDTO, ServiceProfileDTO, ServiceProfileLink}
import cascading.tuple.Fields
import cascading.kryo.KryoFactory
import org.scala_tools.time.Imports._
import com.twitter.scalding.SequenceFile
import com.twitter.scalding.Tsv
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import com.sonar.expedition.scrawler.serializer.{HashMapSerializer, HashSetSerializer, ArrayListSerializer}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Output, Input}

class DataVerificationJob(args: Args) extends DefaultJob(args) with CheckinSource {
    val newIn = args("newIn")
    val oldIn = args("oldIn")

    val dataType = args("dataType")

    override def config =
        super.config ++
                Map(KryoFactory.KRYO_REGISTRATIONS ->
                        Seq(classOf[DateTime].getCanonicalName + "," + classOf[JodaDateTimeSerializer].getCanonicalName,
                            "scala.collection.JavaConversions$SeqWrapper" + "," + classOf[ArrayListSerializer].getCanonicalName,
                            "scala.collection.JavaConversions$SetWrapper" + "," + classOf[HashSetSerializer].getCanonicalName,
                            "scala.collection.JavaConversions$MapWrapper" + "," + classOf[HashMapSerializer].getCanonicalName,
                            classOf[ServiceProfileDTO].getCanonicalName + "," + classOf[DummySerializer].getCanonicalName,
                            classOf[CheckinDTO].getCanonicalName + "," + classOf[DummySerializer].getCanonicalName
                        ).mkString(":"))

    val oldPipe =
        (dataType match {
            case "checkin" =>
                SequenceFile(oldIn, Tuples.Checkin).read.mapTo(('serType, 'serCheckinID) -> 'id) {
                    in: (String, String) => in._1 + ":" + in._2
                }
            case "profile" =>
                SequenceFile(oldIn, Tuples.Profile).read.project('profileId).rename('profileId -> 'id)
        }).unique('id)
    val newPipe =
        (dataType match {
            case "checkin" =>
                SequenceFile(newIn, Tuples.CheckinIdDTO).read.project('id)
            case "profile" =>
                SequenceFile(newIn, Tuples.ProfileIdDTO).read.project('profileId).mapTo('profileId -> 'id) {
                    profileId: ServiceProfileLink => profileId.profileId
                }
        }).unique('id)

    val oldStat = oldPipe.groupAll {
        _.size
    }.map(() -> 'statName) {
        _: Unit => "old"
    }.project('statName, 'size)
    val newStat = oldPipe.joinWithLarger('id -> 'id, newPipe).groupAll {
        _.size
    }.map(() -> 'statName) {
        _: Unit => "new"
    }.project('statName, 'size)
    (oldStat ++ newStat).write(Tsv(newIn + "_compare_stats", ('statName, 'size)))

}

class DummySerializer extends Serializer[Any] {
    setImmutable(true)
    setAcceptsNull(true)

    def write(p1: Kryo, p2: Output, p3: Any) {}

    def read(p1: Kryo, p2: Input, p3: Class[Any]) = p2
}
