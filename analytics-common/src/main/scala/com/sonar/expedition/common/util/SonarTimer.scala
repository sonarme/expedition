package com.sonar.expedition.common.util

import com.yammer.metrics.scala.Timer
import java.util.concurrent.atomic.AtomicInteger

object SonarTimer {
    implicit def timer_to_skipTimer(timer: Timer) = new SkipTimer(timer)

    class SkipTimer(timer: Timer, n: Int = 3) {
        val countDown = new AtomicInteger(n)
        var counting = n > 0

        def toSkipTimer = this

        def offsetTime[A](f: => A): A = {
            if (counting) {
                if (countDown.decrementAndGet() <= 0)
                    counting = false
                f
            }
            else timer.time(f)
        }
    }

}
