package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Job, Args}
import java.util.Calendar

class LocationBehaviourAnalysePipe(args: Args) extends DTOPlacesInfoPipe(args) {

    def getLocationInfo(placesData: RichPipe): RichPipe = {

        val parsedPlaces = placesPipe(placesData).project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
                'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode, 'linenum))

        parsedPlaces
    }

    def deltatime(chkintime1: String, chkintime2: String): Boolean = {

        val timeFilter1 = Calendar.getInstance()
        val checkinDate1 = CheckinTimeFilter.parseDateTime(chkintime1)
        timeFilter1.setTime(checkinDate1)
        val date1 = timeFilter1.get(Calendar.DAY_OF_YEAR)
        val time1 = timeFilter1.get(Calendar.HOUR_OF_DAY) + timeFilter1.get(Calendar.MINUTE) / 60.0
        val timeFilter2 = Calendar.getInstance()
        val checkinDate2 = CheckinTimeFilter.parseDateTime(chkintime2)
        timeFilter2.setTime(checkinDate2)
        val date2 = timeFilter2.get(Calendar.DAY_OF_YEAR)
        val time2 = timeFilter2.get(Calendar.HOUR_OF_DAY) + timeFilter2.get(Calendar.MINUTE) / 60.0

        if (date1.equals(date2)) {
            // need to include the timing too, which simple, if same date, check diff in time, normally we dont want checkins in border timings like 12 am.
            if ((time2.toDouble - time1.toDouble) < 4)
                true
            else
                false
        }
        else
            false

    }


}
