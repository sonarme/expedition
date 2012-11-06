package com.sonar.expedition.scrawler.crawler

object ExtractorFactory {
    def getExtractor(url: String, content: String) = {
        url match {
            case u: String if u.contains(Sites.Yelp) => new YelpExtractor(content)
            case u: String if u.contains(Sites.CitySearch) => new CitySearchExtractor(content)
            case u: String if u.contains(Sites.Foursquare) => new FoursquareExtractor(content)
            case u: String if u.contains(Sites.Facebook) => new FacebookExtractor(content)
            case u: String if u.contains(Sites.Twitter) => new TwitterExtractor(content)
            case u: String if u.contains(Sites.LivingSocial) => new LivingSocialExtractor(content)
            case _ => new Extractor(content)
        }
    }
}