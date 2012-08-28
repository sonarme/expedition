package com.twitter.scalding

/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import cascading.flow.{Flow, FlowDef, FlowProps}
import cascading.pipe.Pipe
import com.sonar.expedition.scrawler.pipes.JobImplicits


//For java -> scala implicits on collections

import scala.collection.JavaConversions._

import java.util.Calendar
import java.util.{Map => JMap}

@serializable
class Job(val args: Args) extends JobImplicits with TupleConversions with FieldConversions {


    // Use reflection to copy this job:
    def clone(nextargs: Args): Job = {
        this.getClass
                .getConstructor(classOf[Args])
                .newInstance(nextargs)
                .asInstanceOf[Job]
    }

    /**
     * Implement this method if you want some other jobs to run after the current
     * job. These will not execute until the current job has run successfully.
     */
    def next: Option[Job] = None

    // Only very different styles of Jobs should override this.
    def buildFlow(implicit mode: Mode) = {
        validateSources(mode)
        // Sources are good, now connect the flow:
        mode.newFlowConnector(config).connect(flowDef)
    }

    /**
     * By default we only set two keys:
     * io.serializations
     * cascading.tuple.element.comparator.default
     * Override this class, call base and ++ your additional
     * map to set more options
     */
    def config: Map[AnyRef, AnyRef] = {
        val ioserVals = (ioSerializations ++
                List("com.twitter.scalding.serialization.KryoHadoop")).mkString(",")
        Map("io.serializations" -> ioserVals) ++
                (defaultComparator match {
                    case Some(defcomp) => Map(FlowProps.DEFAULT_ELEMENT_COMPARATOR -> defcomp)
                    case None => Map[String, String]()
                }) ++
                Map("cascading.spill.threshold" -> "100000", //Tune these for better performance
                    "cascading.spillmap.threshold" -> "100000") ++
                Map("scalding.version" -> "0.7.3",
                    "scalding.flow.class.name" -> getClass.getName,
                    "scalding.job.args" -> args.toString,
                    "scalding.flow.submitted.timestamp" ->
                            Calendar.getInstance().getTimeInMillis().toString
                )
    }

    //Override this if you need to do some extra processing other than complete the flow
    def run(implicit mode: Mode) = {
        val flow = buildFlow(mode)
        flow.complete
        flow.getFlowStats.isSuccessful
    }

    // Add any serializations you need to deal with here (after these)
    def ioSerializations = List[String](
        "org.apache.hadoop.io.serializer.WritableSerialization",
        "cascading.tuple.hadoop.TupleSerialization"
    )

    // Override this if you want to customize comparisons/hashing for your job
    def defaultComparator: Option[String] = {
        Some("com.twitter.scalding.IntegralComparator")
    }

    def validateSources(mode: Mode) {
        flowDef.getSources()
                .asInstanceOf[JMap[String, AnyRef]]
                // this is a map of (name, Tap)
                .foreach {
            nameTap =>
            // Each named source must be present:
                mode.getSourceNamed(nameTap._1)
                        .get
                        // This can throw a InvalidSourceException
                        .validateTaps(mode)
        }
    }
}
