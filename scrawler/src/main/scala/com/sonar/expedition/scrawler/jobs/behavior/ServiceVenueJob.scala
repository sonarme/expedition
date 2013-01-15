package com.sonar.expedition.scrawler.jobs.behavior

import com.sonar.dossier.dto.CheckinDTO
import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.dossier.Normalizers

class ServiceVenueJob(args: Args) extends Job(args) with Normalizers {
    val argsCheckin = args("checkinsIn")
    val argsVenues = args("venuesOut")
    val argsStats = args("statsOut")
    SequenceFile(argsCheckin, Tuples.CheckinIdDTO).read.flatMapTo(('checkinDto) ->('venueId, 'venueDto, 'nonRaw)) {
        dto: CheckinDTO =>
            if (dto.venueId == null) None
            else {
                val rawNormalizedVenue = normalizeVenueFromCheckin(deserializeFromRaw(dto.serviceType, dto.raw))
                val venue = rawNormalizedVenue getOrElse dto.serviceVenue
                Some((venue.canonicalId, venue, rawNormalizedVenue.isEmpty))
            }
    }.groupBy('venueId) {
        // pick the venue resulting from raw data first, because it contains categories etc.
        _.sortBy('nonRaw).head('nonRaw, 'venueDto)
    }.write(SequenceFile(argsVenues, Tuples.VenueIdDTO)).groupBy('nonRaw) {
        // write some stats so we know how many venues from raw we have
        _.size
    }.write(Tsv(argsStats, ('nonRaw, 'size)))


}
