package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.dossier.dto.CheckinDTO
import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.util.{LocationClusterer, Tuples}
import com.sonar.dossier.Normalizers
import com.sonar.expedition.scrawler.checkins.CheckinInference
import com.sonar.expedition.scrawler.util.CommonFunctions._
import org.joda.time.{LocalTime, LocalDateTime}

class PlaceInferenceJob(args: Args) extends Job(args) with Normalizers with CheckinInference {
    val argsCheckin = args("checkins")
    val argsVenues = args("venues")
    val argsStats = args("stats")
    val segments = Seq(0 -> 7, 6 -> 11, 11 -> 14, 13 -> 17, 16 -> 20, 19 -> 0) map {
        case (fromHr, toHr) => Segment(from = new LocalTime(fromHr, 0, 0), to = new LocalTime(toHr, 0, 0), name = toHr)
    }
    SequenceFile(argsCheckin, Tuples.CheckinIdDTO).read.flatMap(('checkinDto) ->('weekDay, 'timeSegment)) {
        dto: CheckinDTO =>
            val ldt = localDateTime(dto.latitude, dto.longitude, dto.checkinTime.toDate)
            val weekDay = isWeekDay(ldt)

            createSegments(ldt.toLocalTime, segments) map {
                segment => (weekDay, segment.name)
            }
    }.groupBy('weekDay, 'timeSegment) {
        _.mapList(('checkinDto) -> ('c)) {
            checkins: List[CheckinDTO] =>
                val clusters = LocationClusterer.cluster(checkins.map(c => (c.latitude, c.longitude)))
                null
        }
    }


}
