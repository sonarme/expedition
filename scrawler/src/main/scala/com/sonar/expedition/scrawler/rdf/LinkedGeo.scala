package com.sonar.expedition.scrawler.rdf

import com.hp.hpl.jena.rdf.model._

object LinkedGeo {
    def resource(uri: String, local: String) = ResourceFactory.createResource(uri + local)

    def property(uri: String, local: String) = ResourceFactory.createProperty(uri, local)

    val LGNS = "http://linkedgeodata.org/ontology/"

    def Amenity = resource(LGNS, "Amenity")

    val J1NS = "http://linkedgeodata.org/ontology/addr%3A"

    def street = property(J1NS, "street")

    val GeoNS = "http://www.w3.org/2003/01/geo/wgs84_pos#"

    def long = property(GeoNS, "long")

    def lat = property(GeoNS, "lat")

    val DCTermsNS = "http://purl.org/dc/terms/"

    def contributor = property(DCTermsNS, "contributor")

    val RDFSNS = "http://www.w3.org/2000/01/rdf-schema#"

    def label = property(RDFSNS, "label")

    val NS = Map(
        "lgdo" -> LGNS,
        "j.1" -> J1NS,
        "geo" -> GeoNS,
        "dcterms" -> DCTermsNS,
        "rdfs" -> RDFSNS)
}


