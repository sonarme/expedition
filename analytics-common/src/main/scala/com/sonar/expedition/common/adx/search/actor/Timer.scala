package com.sonar.expedition.common.adx.search.actor

import actors.{TIMEOUT, Actor}
import org.scala_tools.time.Imports._

abstract class TimerMessage
case object Reset extends TimerMessage

class Timer(timeout: Duration, who: Actor) extends Actor {

    def act {
        loop {
            reactWithin(timeout.millis) {
                case TIMEOUT => who ! Reset
            }
        }
    }
}