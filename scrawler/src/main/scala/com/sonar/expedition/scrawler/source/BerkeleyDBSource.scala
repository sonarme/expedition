package com.sonar.expedition.scrawler.source

import com.twitter.scalding._
import cascading.tap.Tap
import elephantdb.cascading.ElephantDBTap
import elephantdb.DomainSpec
import elephantdb.cascading.ElephantDBTap.{Args => EArgs}
import elephantdb.persistence.JavaBerkDB
import elephantdb.partition.HashModScheme

case class BerkeleyDBSource(dir: String, spec: DomainSpec = new DomainSpec(new JavaBerkDB, new HashModScheme, 1), args: EArgs = new EArgs) extends Source {

    override def createTap(readOrWrite: AccessMode)(implicit mode: Mode) = new ElephantDBTap(dir, spec, args).asInstanceOf[Tap[_, _, _]]
}
