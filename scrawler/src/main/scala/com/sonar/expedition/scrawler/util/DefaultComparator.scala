package com.sonar.expedition.scrawler.util

import com.twitter.scalding.IntegralComparator
import org.slf4j.LoggerFactory

class DefaultComparator extends IntegralComparator {
    val LOG = LoggerFactory.getLogger(classOf[DefaultComparator])

    override def hashCode(obj: AnyRef) = {
        LOG.info("Found " + obj)
        if (obj == null) 0
        else if (obj.getClass.isEnum) {
            // fix enum hashing
            val enumObj = obj.asInstanceOf[Enum[_]]
            LOG.info("Found enum " + enumObj)
            (obj.getClass, enumObj.name()).hashCode()
        }
        else super.hashCode(obj)
    }
}
