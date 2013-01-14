package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import cascading.kryo.KryoFactory
import org.scala_tools.time.Imports._
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import com.sonar.expedition.scrawler.serializer.ArrayListSerializer

class DefaultJob(args: Args) extends Job(args) {

    override def config =
        super.config ++
                Map(KryoFactory.KRYO_REGISTRATIONS ->
                        Seq(classOf[DateTime].getCanonicalName + "," + classOf[JodaDateTimeSerializer].getCanonicalName,
                            "scala.collection.JavaConversions$SeqWrapper" + "," + classOf[ArrayListSerializer].getCanonicalName).mkString(":"))

}
