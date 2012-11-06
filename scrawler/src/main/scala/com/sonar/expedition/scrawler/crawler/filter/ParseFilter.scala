package com.sonar.expedition.scrawler.crawler.filter

import com.sonar.expedition.scrawler.crawler.Sites

/**
 * urls to be included when parsing content
 */
trait ParseFilter {
    def isUrlIncluded(url: String) = {
        url match {
            case u: String if u.indexOf(Sites.Yelp) > -1 => if (url.contains(Sites.Yelp + "/biz/")) true else false
            case u: String if u.indexOf(Sites.CitySearch) > -1 => if (url.contains(Sites.CitySearch + "/profile/")) true else false
            case _ => true
        }
    }
}