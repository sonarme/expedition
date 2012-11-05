package com.sonar.expedition.scrawler.rdf

import com.hp.hpl.jena.rdf.model._

object LinkedGeo {
    def resource(uri: String, local: String) = ResourceFactory.createResource(uri + local)

    def property(uri: String, local: String) = ResourceFactory.createProperty(uri, local)

    val LGNS = "http://linkedgeodata.org/ontology/"

    val Amenity = resource(LGNS, "Amenity")

    val J1NS = "http://linkedgeodata.org/ontology/addr%3A"

    val street = property(J1NS, "street")

    val GeoNS = "http://www.w3.org/2003/01/geo/wgs84_pos#"

    val long = property(GeoNS, "long")

    val lat = property(GeoNS, "lat")

    val DCTermsNS = "http://purl.org/dc/terms/"

    val contributor = property(DCTermsNS, "contributor")

    val RDFSNS = "http://www.w3.org/2000/01/rdf-schema#"

    val label = property(RDFSNS, "label")

    val GEORSSNS = "http://www.georss.org/georss/"

    val point = property(GEORSSNS, "point")

    val NS = Map(
        "lgdo" -> LGNS,
        "j.1" -> J1NS,
        "geo" -> GeoNS,
        "dcterms" -> DCTermsNS,
        "rdfs" -> RDFSNS,
        "georss" -> GEORSSNS)

}


