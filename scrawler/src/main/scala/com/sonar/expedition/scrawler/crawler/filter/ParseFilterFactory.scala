package com.sonar.expedition.scrawler.crawler.filter

import com.sonar.expedition.scrawler.crawler.Sites

object ParseFilterFactory {
    def getParseFilter(domain: String) = {
        domain match {
            case d: String if d.indexOf(Sites.Yelp) > -1 => new YelpParseFilter()
            case d: String if d.indexOf(Sites.CitySearch) > -1 => new CitySearchParseFilter()
            case _ => new ParseFilter()
        }
    }
}