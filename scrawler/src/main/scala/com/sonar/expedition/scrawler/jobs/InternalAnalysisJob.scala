package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{TextLine, RichPipe, Job, Args}
import com.sonar.expedition.scrawler.util.StemAndMetaphoneEmployer

class InternalAnalysisJob(args: Args) extends Job(args) {


    def internalAnalysisGroupByServiceType(data: RichPipe): RichPipe = {

        data.groupBy('serviceType) {
            _.size
        }
        data

    }

    def internalAnalysisUniqueProfiles(data: RichPipe): RichPipe = {
        data.unique('id).groupAll {
            _.size
        }
        data

    }

    def internalAnalysisGroupByCity(joinedProfiles: RichPipe): RichPipe = {
        val returnpipe = joinedProfiles.map('city -> 'cityCleaned) {
            city: String => {
                StemAndMetaphoneEmployer.removeStopWords(city)
            }
        }
                .project(('key, 'cityCleaned))
                .groupBy('cityCleaned) {
            _.size
        }
                .filter('size) {
            size: Int => {
                size > 1
            }
        }

        returnpipe
    }


}
