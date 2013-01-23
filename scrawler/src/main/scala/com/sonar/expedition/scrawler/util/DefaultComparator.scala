package com.sonar.expedition.scrawler.util

import com.twitter.scalding.IntegralComparator

class DefaultComparator extends IntegralComparator {

    override def hashCode(obj: AnyRef) =
        if (obj == null) 0
        else if (obj.getClass.isEnum) {
            // fix enum hashing
            val enumObj = obj.asInstanceOf[Enum[_]]
            (obj.getClass, enumObj.name()).hashCode()
        }
        else super.hashCode(obj)
}
