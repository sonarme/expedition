package com.sonar.expedition.scrawler.crawler.filter

import com.sonar.expedition.scrawler.crawler.Sites

object ParseFilterFactory {
    def getParseFilter(url: String) = {
        url match {
            case u: String if u.indexOf(Sites.Yelp) > -1 => new YelpParseFilter()
            case u: String if u.indexOf(Sites.CitySearch) > -1 => new CitySearchParseFilter()
            case _ => new ParseFilter()
        }
    }
}