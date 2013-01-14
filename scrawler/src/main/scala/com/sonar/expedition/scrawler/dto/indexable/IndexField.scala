package com.sonar.expedition.scrawler.dto.indexable

object IndexField extends Enumeration {
    type IndexField = Value
    val Key = Value("key")
    val Name = Value("name")
    val Content = Value("content")
    val Categories = Value("categories")
    val Geohash = Value("geohash")
    val Ip = Value("ip")
}