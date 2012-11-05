package com.sonar.expedition.scrawler.rdf

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.sonar.dossier.dto.ServiceType
import com.sonar.expedition.scrawler.util.RdfFormat

class RDFConversionTest extends FlatSpec with ShouldMatchers with RDFConversion {

    "place touple" should "be written into RDF" in {
        val rdf = placeToRDF(ServiceType.foursquare.name(), "123ab", "Cafe Ben", "123 45th St", 40.0, -74.0, RdfFormat.RdfXmlAbbrev)
        assert(rdf contains """<lgdo:Amenity rdf:about="http://sonar.me/venue#foursquare_123ab">""")
    }

}

