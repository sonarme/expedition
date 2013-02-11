package com.sonar.expedition.common.util

import java.util.concurrent.atomic.AtomicInteger
import java.lang.Float._

class AtomicFloat(initialValue: Float = 0f) extends Number {
    private val bits = new AtomicInteger(floatToIntBits(initialValue))


    def compareAndSet(expect: Float, update: Float) =
        bits.compareAndSet(floatToIntBits(expect), floatToIntBits(update))


    def set(newValue: Float) {
        bits.set(floatToIntBits(newValue))
    }

    def get = intBitsToFloat(bits.get)

    def floatValue = get

    def getAndSet(newValue: Float) = intBitsToFloat(bits.getAndSet(floatToIntBits(newValue)))

    def weakCompareAndSet(expect: Float, update: Float) = bits.weakCompareAndSet(floatToIntBits(expect), floatToIntBits(update))

    def doubleValue = floatValue.asInstanceOf[Double]

    def intValue = get.toInt

    def longValue = get.toLong

}
