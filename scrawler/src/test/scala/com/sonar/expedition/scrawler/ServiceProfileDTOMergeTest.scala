package com.sonar.expedition.scrawler

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import util.{Haversine, LocationClusterer}
import com.sonar.dossier.dto.{UserEducation, Gender, ServiceProfileDTO, ServiceType}
import reflect.BeanProperty
import org.dozer.DozerBeanMapper
import org.dozer.loader.api.BeanMappingBuilder
import org.dozer.loader.api.TypeMappingOptions._
import collection.JavaConversions._
import org.apache.commons.beanutils.{BeanUtils, PropertyUtils}

class ServiceProfileDTOMergeTest extends FlatSpec with ShouldMatchers {
    val mapper = new DozerBeanMapper()

    mapper.addMapping(new BeanMappingBuilder {
        def configure() {
            mapping(classOf[ServiceProfileDTO], classOf[ServiceProfileDTO],
                oneWay(), mapNull(false), mapEmptyString(false)
            )
        }
    })
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
        a.education = List(UserEducation(1L, "eda"))
        val b = ServiceProfileDTO(ServiceType.facebook, "b")
        b.education = List(UserEducation(1L, "edb"))
        val c = ServiceProfileDTO(ServiceType.facebook, "c")
        c.education = List(UserEducation(1L, "edc"))
        c.fullName = "fullNamec"
        val res = List(a, b, c).reduce[ServiceProfileDTO] {
            case (a, b) =>
                mapper.map(a, b)
                b
        }
        res
    }

}
