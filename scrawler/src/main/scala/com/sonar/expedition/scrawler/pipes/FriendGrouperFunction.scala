package com.sonar.expedition.scrawler.pipes

import cascading.tuple.Fields


trait FriendGrouperFunction extends ScaldingImplicits {
    val FriendTuple: Fields = ('userProfileId, 'serviceType, 'serviceProfileId)

}

