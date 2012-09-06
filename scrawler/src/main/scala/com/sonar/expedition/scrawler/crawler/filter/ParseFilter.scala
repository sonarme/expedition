package com.sonar.expedition.scrawler.crawler.filter

import com.sonar.expedition.scrawler.crawler.Sites

/**
 * urls to be included when parsing content
 */
class ParseFilter {
    def isIncluded(url: String) = true
}

case class YelpParseFilter() extends ParseFilter {
    override def isIncluded(url: String) = if (url.contains(Sites.Yelp + "/biz/")) true else false
}

case class CitySearchParseFilter() extends ParseFilter {
    override def isIncluded(url: String) = if (url.contains(Sites.CitySearch + "/profile/")) true else false
}