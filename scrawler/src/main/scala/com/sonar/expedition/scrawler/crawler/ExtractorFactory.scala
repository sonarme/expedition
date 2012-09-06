package com.sonar.expedition.scrawler.crawler

object ExtractorFactory {
    def getExtractor(domain: String, content: String) = {
        domain match {
            case d: String if d.indexOf(Sites.Yelp) > -1 => new YelpExtractor(content)
            case d: String if d.indexOf(Sites.CitySearch) > -1 => new CitySearchExtractor(content)
            case _ => new Extractor(content)
        }
    }
}