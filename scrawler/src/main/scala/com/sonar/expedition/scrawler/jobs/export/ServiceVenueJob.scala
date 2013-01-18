package com.sonar.expedition.scrawler.jobs.export

import com.sonar.dossier.dto.{ServiceVenueDTO, ServiceType, CheckinDTO}
import com.twitter.scalding.{Tsv, SequenceFile, Job, Args}
import com.sonar.expedition.scrawler.util.Tuples
import com.sonar.dossier.Normalizers
import com.sonar.expedition.scrawler.jobs.DefaultJob

class ServiceVenueJob(args: Args) extends DefaultJob(args) with Normalizers {
    val argsCheckin = args("checkinsIn")
    val argsVenues = args("venuesOut")
    val argsStats = args("statsOut")
    val venues = SequenceFile(argsCheckin, Tuples.CheckinIdDTO).read.flatMapTo(('checkinDto) ->('venueId, 'venueDto, 'raw)) {
        dto: CheckinDTO =>
            if (dto.venueId == null || dto.serviceType != ServiceType.foursquare) None
            else {

                val rawNormalizedVenue =
                    try {
                        deserializeFromRaw(dto.serviceType, dto.raw) flatMap normalizeVenueFromCheckin
                    } catch {
                        case npe: NullPointerException => throw new RuntimeException("from raw: " + dto.raw, npe)
                    }
                val venue = rawNormalizedVenue getOrElse dto.serviceVenue
                Some((venue.canonicalId, venue, rawNormalizedVenue.isDefined))
            }
    }.groupBy('venueId) {
        // pick the venue resulting from raw data first, because it contains categories etc.
        _.reduce('raw, 'venueDto) {
            (left: (Boolean, ServiceVenueDTO), right: (Boolean, ServiceVenueDTO)) =>
                if (left._1) left else right
        }
    }
    venues.write(SequenceFile(argsVenues, Tuples.VenueIdDTO))
    venues.discard('venueDto).groupBy('raw) {
        // write some stats so we know how many venues from raw we have
        _.size.reducers(1)
    }.write(Tsv(argsStats, ('raw, 'size)))


}
