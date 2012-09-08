package com.sonar.expedition.scrawler.crawler

object ExtractorFactory {
    def getExtractor(domain: String, content: String) = {
        domain match {
            case d: String if d.indexOf(Sites.Yelp) > -1 => new YelpExtractor(content)
            case d: String if d.indexOf(Sites.CitySearch) > -1 => new CitySearchExtractor(content)
            case d: String if d.indexOf(Sites.Foursquare) > -1 => new FoursquareExtractor(content)
            case d: String if d.indexOf(Sites.Facebook) > -1 => new FacebookExtractor(content)
            case d: String if d.indexOf(Sites.Twitter) > -1 => new TwitterExtractor(content)
            case d: String if d.indexOf(Sites.LivingSocial) > -1 => new LivingSocialExtractor(content)
            case _ => new Extractor(content)
        }
    }
}