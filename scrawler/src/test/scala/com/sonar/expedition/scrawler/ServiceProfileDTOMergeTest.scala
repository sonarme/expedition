package com.sonar.expedition.scrawler

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import util.CommonFunctions
import com.sonar.dossier.dto.{UserEducation, ServiceProfileDTO, ServiceType}
import collection.JavaConversions._

class ServiceProfileDTOMergeTest extends FlatSpec with ShouldMatchers {

    /*mapper.addMappings(new PropertyMap[ServiceProfileDTO, ServiceProfileDTO]() {
      protected void configure() {
        map().setBillingStreet(source.getBillingStreetAddress());
            skip().
            skip().setBillingCity(null);
      }
    });*/
    "3 service profiles" should "merge into one" in {
        val a = ServiceProfileDTO(ServiceType.facebook, "a")
        a.bio = "bioa"
        a.education = new java.util.ArrayList[UserEducation](List(UserEducation(1L, "eda")))
        val b = ServiceProfileDTO(ServiceType.facebook, "b")
        b.education = new java.util.ArrayList[UserEducation](List(UserEducation(1L, "edb")))
        val c = ServiceProfileDTO(ServiceType.facebook, "c")
        val edc = UserEducation(1L, "edc")
        c.education = new java.util.ArrayList[UserEducation](List(edc))
        c.fullName = "fullNamec"
        val res = List(a, b, c).reduce[ServiceProfileDTO] {
            case (a, b) =>
                CommonFunctions.populateNonEmpty(a, b)
        }
        assert(res.bio === "bioa")
        assert(res.fullName === "fullNamec")
        assert(res.education.head === edc)
    }

}
