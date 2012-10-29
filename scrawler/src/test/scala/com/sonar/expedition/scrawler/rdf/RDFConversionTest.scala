package com.sonar.expedition.scrawler.rdf

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.sonar.dossier.dto.ServiceType

class RDFConversionTest extends FlatSpec with ShouldMatchers with RDFConversion {

    "place touple" should "be written into RDF" in {
        val rdf = placeToRDF(ServiceType.foursquare.name(), "123ab", "Cafe Ben", "123 45th St", 40.0, -74.0)
        assert(rdf.nonEmpty)
    }

}

