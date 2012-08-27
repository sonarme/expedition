package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{TupleConversions, FieldConversions}

trait ScaldingImplicits extends JobImplicits with TupleConversions with FieldConversions
