package com.sonar.expedition.scrawler.rdf

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.sonar.dossier.dto.{Aliases, Gender, ServiceProfileDTO, ServiceType}
import com.sonar.expedition.scrawler.util.RdfFormat
import java.util.Date

class RDFConversionTest extends FlatSpec with ShouldMatchers with RDFConversion {

    "place touple" should "be written into RDF" in {
        val rdf = placeToRDF(ServiceType.foursquare.name(), "123ab", "Cafe Ben", "123 45th St", 40.0, -74.0, RdfFormat.RdfXmlAbbrev)
        assert(rdf contains """<lgdo:Amenity rdf:about="http://sonar.me/venue#foursquare_123ab">""")
    }

    /*"profile tuple" should "be written into RDF" in {
        val serviceProfile = new ServiceProfileDTO(serviceType = ServiceType.foursquare, userId = "rogerchang")
        serviceProfile.gender = Gender.male
        serviceProfile.fullName = "Roger Chang"
        serviceProfile.birthday = new Date
        serviceProfile.photoUrl = "http://www.sonar.me/somepic.jpg"
        serviceProfile.aliases = new Aliases(username = "rog", email = "roger@sonar.me", facebook = "fb1234", twitter = "tw1234")
        val rdf = profileToRDF(serviceProfile)
        println(rdf)
        assert(rdf.nonEmpty)
    }*/
}

