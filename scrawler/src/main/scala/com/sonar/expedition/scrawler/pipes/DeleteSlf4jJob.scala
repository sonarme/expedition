package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{Args, Job}
import java.io.File

class DeleteSlf4jJob(args:Args) extends Job(args) {

    val slf4jVersion = args.getOrElse("version", "1.4.3")

    val localLog4jFile = new File("/home/hadoop/lib/slf4j-log4j12-" + slf4jVersion + ".jar")
    val localApiFile = new File("/home/hadoop/lib/slf4j-api-" + slf4jVersion + ".jar")

    val successfulLog4jDelete:Boolean = if (localLog4jFile.exists()) {
        localLog4jFile.delete()
    } else {
        false
    }

    val successfulApiDelete:Boolean = if (localApiFile.exists()) {
        localApiFile.delete()
    } else {
        false
    }
    println("deleted slf4j files locally: " + (if (successfulApiDelete) "yes" else "no") + " " + (if (successfulLog4jDelete) "yes" else "no"))

}
