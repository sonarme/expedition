package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{TupleConversions, FieldConversions}
import JobImplicits._

trait ScaldingImplicits extends TupleConversions with FieldConversions
