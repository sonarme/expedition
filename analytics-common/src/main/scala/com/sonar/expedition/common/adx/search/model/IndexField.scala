package com.sonar.expedition.common.adx.search.model

// TODO: we need a -common project to connect between scalding and rtb
object IndexField extends Enumeration {
    type IndexField = Value
    val Key = Value("key")
    val Name = Value("name")
    val Categories = Value("categories")
    val Geosector = Value("geosector")
    val GeosectorTimesegment = Value("geosectorTimesegment")
    val GeosectorTimewindow = Value("geosectorTimewindow")
    val Ip = Value("ip")
    val ServiceId = Value("serviceId")
}
