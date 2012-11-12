package com.sonar.expedition.scrawler.crawler

trait SitemapFilter {
    def isSiteMapIncluded(siteMapUrl: String) = {
        siteMapUrl match {
            case sm: String if sm.contains(Sites.Meetup) => if ( (!sm.contains("group")) || (sm.endsWith("kmz")) ) false else true
            case _ => true
        }
    }
}