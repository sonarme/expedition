package com.sonar.expedition.scrawler.pipes


import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.{RichPipe, Args}
import util.matching.Regex


trait FriendGrouperFunction extends ScaldingImplicits {

    def groupFriends(input: RichPipe) =

        input.flatMapTo('line ->('userProfileId, 'serviceType, 'serviceProfileId)) {
            line: String =>
                line match {
                    // change when we use prod data
                    case FriendProdExtractLine(id, serviceType, serviceId, _) => Some((id, serviceType, hashed(serviceId)))
                    case FriendExtractLine(id, other2, serviceId, serviceType, _, other) => Some((id, serviceType, hashed(serviceId)))
                    case _ => None
                }
        }.unique('userProfileId, 'serviceType, 'serviceProfileId)


}

