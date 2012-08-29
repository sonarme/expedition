package com.sonar.expedition.scrawler.pipes


import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding.{RichPipe, Args}
import util.matching.Regex


trait FriendGrouperFunction extends ScaldingImplicits {

    def groupFriends(input: RichPipe): RichPipe = {


        val data = input.flatMapTo(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)) {
            line: String => {
                line match {
                    // change when we use prod data
                    case FriendProdExtractLine(id, serviceType, serviceId, friendName) => Some((id, serviceType, hashed(serviceId), friendName))
                    case FriendExtractLine(id, other2, serviceId, serviceType, friendName, other) => Some((id, serviceType, hashed(serviceId), friendName))
                    case _ => None
                }
            }
        }

                .unique(('userProfileId, 'serviceType, 'serviceProfileId, 'friendName))

        data
    }


}

