package com.sonar.expedition.scrawler.crawler

trait LinkPostProcessor {
    def processUrl(url: String) = {
        url match {
            case u: String if u.contains(Sites.LivingSocial) => u + "?show_missed=true"
            case _ => url
        }
    }
}