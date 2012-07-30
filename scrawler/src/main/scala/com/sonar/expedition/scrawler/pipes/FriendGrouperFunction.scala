package com.sonar.expedition.scrawler.pipes


import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.twitter.scalding._
import util.matching.Regex

class FriendGrouperFunction(args: Args) extends Job(args) {

    def groupFriends(input: RichPipe): RichPipe = {


        val data = input.mapTo(('line) ->('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)) {
            line: String => {
                line match {
                    // change when we use prod data
                    case FriendProdExtractLine(id, other2, serviceId, serviceType, friendName, other) => (id, serviceType, serviceId, friendName)
                    case _ => ("None", "None", "None", "None")
                }
            }
        }

                .project('userProfileId, 'serviceType, 'serviceProfileId, 'friendName)

        data
    }


}

object FriendGrouperFunction {

}



