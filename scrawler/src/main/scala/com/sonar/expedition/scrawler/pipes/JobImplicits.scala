package com.sonar.expedition.scrawler.pipes

import cascading.flow.{Flow, FlowDef, FlowProps}
import cascading.pipe.Pipe
import com.twitter.scalding._


//For java -> scala implicits on collections

import scala.collection.JavaConversions._

import java.util.Calendar
import java.util.{Map => JMap}

trait JobImplicits {

    /**
     * you should never call these directly, there are here to make
     * the DSL work.  Just know, you can treat a Pipe as a RichPipe and
     * vice-versa within a Job
     */
    implicit def p2rp(pipe: Pipe) = new RichPipe(pipe)

    implicit def rp2p(rp: RichPipe) = rp.pipe

    implicit def source2rp(src: Source): RichPipe = RichPipe(src.read)

    // This converts an interable into a Source with index (int-based) fields
    implicit def iterToSource[T](iter: Iterable[T])(implicit set: TupleSetter[T], conv: TupleConverter[T]): Source = {
        IterableSource[T](iter)(set, conv)
    }

    //
    implicit def iterToPipe[T](iter: Iterable[T])(implicit set: TupleSetter[T], conv: TupleConverter[T]): Pipe = {
        iterToSource(iter)(set, conv).read
    }

    implicit def iterToRichPipe[T](iter: Iterable[T])
                                  (implicit set: TupleSetter[T], conv: TupleConverter[T]): RichPipe = {
        RichPipe(iterToPipe(iter)(set, conv))
    }

    // Override this if you want change how the mapred.job.name is written in Hadoop
    def name: String = getClass.getName

    //This is the FlowDef used by all Sources this job creates
    @transient
    implicit val flowDef = {
        val fd = new FlowDef
        fd.setName(name)
        fd
    }

    //Largely for the benefit of Java jobs
    implicit def read(src: Source): Pipe = src.read

    def write(pipe: Pipe, src: Source) {
        src.writeFrom(pipe)
    }


}
