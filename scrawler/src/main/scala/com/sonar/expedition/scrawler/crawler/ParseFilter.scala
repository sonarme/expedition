package com.sonar.expedition.scrawler.crawler


/**
 * urls to be included when parsing content
 */
trait ParseFilter {
    def isUrlIncluded(url: String) = {
        url match {
            case u: String if u.contains(Sites.Yelp) => if (url.contains(Sites.Yelp + "/biz/")) true else false
            case u: String if u.contains(Sites.CitySearch) => if (url.contains(Sites.CitySearch + "/profile/")) true else false
            case _ => true
        }
    }
}