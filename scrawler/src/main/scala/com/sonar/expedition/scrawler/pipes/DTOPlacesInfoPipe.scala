package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Job, Args}
import com.sonar.expedition.scrawler.dto._
import com.sonar.expedition.scrawler.json.JacksonObjectMapper
import scala.Array

class DTOPlacesInfoPipe(args: Args) extends Job(args)  {

    def getPlacesInfo(placesData: RichPipe): RichPipe = {

        val parsedPlaces = placesData.map('line -> ('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
                                         /*'classifiersCategory, 'classifiersType, 'classifiersSubcategory,*/ 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode)) {
            fields: (String) =>
                val (data) = fields
                val placesJson = parseJson(Option(data))
                val geometryType = getGeometryType(placesJson)
                val geometryLongitude = getGeometryCoordinates(placesJson).head
                val geometryLatitude = getGeometryCoordinates(placesJson).last
                val placeType = getType(placesJson)
                val id = getId(placesJson)
                val propertiesProvince = getPropertiesProvince(placesJson)
                val propertiesCity = getPropertiesCity(placesJson)
                val propertiesName = getPropertiesName(placesJson)
                val propertiesTags = getPropertiesTags(placesJson)
                val propertiesCountry = getPropertiesCountry(placesJson)
//                val classifiersCategory = getClassifiersCategory(placesJson)
//                val classifiersType = getClassifiersType(placesJson)
//                val classifiersSubcategory = getClassifiersSubcategory(placesJson)
                val propertiesPhone = getPropertiesPhone(placesJson)
                val propertiesHref = getPropertiesHref(placesJson)
                val propertiesAddress = getPropertiesAddress(placesJson)
                val propertiesOwner = getPropertiesOwner(placesJson)
                val propertiesPostcode = getPropertiesPostcode(placesJson)

                (geometryType, geometryLatitude, geometryLongitude, placeType, id, propertiesProvince, propertiesCity, propertiesName, propertiesTags, propertiesCountry,
                 /*classifiersCategory, classifiersType, classifiersSubcategory,*/ propertiesPhone, propertiesHref, propertiesAddress, propertiesOwner, propertiesPostcode)
        }.project('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
                /*'classifiersCategory, 'classifiersType, 'classifiersSubcategory,*/ 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode)
            parsedPlaces
    }

    def parseJson(jsonStringOption: Option[String]): Option[PlacesDTO] = {
        jsonStringOption map {
            jsonString =>
                JacksonObjectMapper.objectMapper.readValue(jsonString, classOf[PlacesDTO])
        }
    }

    def getGeometryType(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getGeometry.getType()).getOrElse("None")
    }

    def getGeometryCoordinates(placesData: Option[PlacesDTO]): Array[Double] = {
        placesData.map(_.getGeometry.getCoordinates()).getOrElse(Array(0.0, 0.0))
    }

    def getType(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getType()).getOrElse("None")
    }

    def getId(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getId()).getOrElse("None")
    }

    def getPropertiesProvince(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getProvince()).getOrElse("None")
    }

    def getPropertiesCity(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getCity()).getOrElse("None")
    }

    def getPropertiesName(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getName()).getOrElse("None")
    }

    def getPropertiesTags(placesData: Option[PlacesDTO]): Array[String] = {
        placesData.map(_.getProperties.getTags()).getOrElse(Array("None","None"))
    }

    def getPropertiesCountry(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getCountry()).getOrElse("None")
    }

//    def getClassifiersCategory(placesData: Option[PlacesDTO]): Option = {
//        placesData.map(_.getProperties.getClassifiers)
//    }
//
//    def getClassifiersType(placesData: Option[PlacesDTO]): String = {
//        placesData.map(_.getProperties.getClassifiers.getType()).getOrElse("None")
//    }
//
//    def getClassifiersSubcategory(placesData: Option[PlacesDTO]): String = {
//        placesData.map(_.getProperties.getClassifiers.getSubcategory()).getOrElse("None")
//    }

    def getPropertiesPhone(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getPhone()).getOrElse("None")
    }

    def getPropertiesHref(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getHref()).getOrElse("None")
    }

    def getPropertiesAddress(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getAddress()).getOrElse("None")
    }

    def getPropertiesOwner(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getOwner()).getOrElse("None")
    }

    def getPropertiesPostcode(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getPostcode()).getOrElse("None")
    }

}