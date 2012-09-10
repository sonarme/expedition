package com.sonar.expedition.scrawler.pipes

import com.twitter.scalding.{RichPipe, Args}
import com.sonar.expedition.scrawler.dto._
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper
import scala.Array
import com.sonar.expedition.scrawler.dto.PlacesClassifiersDTO
import com.sonar.expedition.scrawler.dto.PlacesDTO


trait DTOPlacesInfoPipe extends ScaldingImplicits {


    def getPlacesInfo(placesData: RichPipe) =
        placesPipe(placesData)
                .project('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode)

    def placesPipe(places: RichPipe) =
        places.mapTo(('line, 'offset) ->
                ('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
                        'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode, 'linenum)) {
            fields: (String, String) =>
                val (data, linenum) = fields
                val placesJson = parseJson(Option(data))
                val geometryType = getGeometryType(placesJson)
                val coordinates = getGeometryCoordinates(placesJson)
                val geometryLongitude = coordinates.head
                val geometryLatitude = coordinates.last
                val placeType = getType(placesJson)
                val id = getId(placesJson)
                val propertiesProvince = getPropertiesProvince(placesJson)
                val propertiesCity = getPropertiesCity(placesJson)
                val propertiesName = getPropertiesName(placesJson)
                val propertiesTags = getPropertiesTags(placesJson)
                val propertiesCountry = getPropertiesCountry(placesJson)

                val classifiersCategory = getClassifiers(placesJson).map(_.getCategory())
                val classifiersType = getClassifiers(placesJson).map(_.classifierType)
                val classifiersSubcategory = getClassifiers(placesJson).map(_.getSubcategory())
                val propertiesPhone = getPropertiesPhone(placesJson)
                val propertiesHref = getPropertiesHref(placesJson)
                val propertiesAddress = getPropertiesAddress(placesJson)
                val propertiesOwner = getPropertiesOwner(placesJson)
                val propertiesPostcode = getPropertiesPostcode(placesJson)
                (geometryType, geometryLatitude, geometryLongitude, placeType, id, propertiesProvince, propertiesCity, propertiesName, propertiesTags, propertiesCountry,
                        classifiersCategory, classifiersType, classifiersSubcategory, propertiesPhone, propertiesHref, propertiesAddress, propertiesOwner, propertiesPostcode, linenum)
        }


    def parseJson(jsonStringOption: Option[String]): Option[PlacesDTO] = {
        jsonStringOption map {
            jsonString =>
                try {
                    ScrawlerObjectMapper.mapper().readValue(jsonString, classOf[PlacesDTO])
                } catch {
                    case e => throw new RuntimeException("Error parsing JSON: " + jsonString, e)
                }
        }
    }

    def getGeometryType(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getGeometry.getGeometryType()).getOrElse("None")
    }

    def getGeometryCoordinates(placesData: Option[PlacesDTO]): Array[Double] = {
        placesData.map(_.getGeometry.getCoordinates()).getOrElse(Array(0.0, 0.0))
    }

    def getType(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getPlaceType()).getOrElse("None")
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

    def getPropertiesTags(placesData: Option[PlacesDTO]): List[String] = {
        placesData.map(_.getProperties.getTags().toList).getOrElse(List[String]())
    }

    def getPropertiesCountry(placesData: Option[PlacesDTO]): String = {
        placesData.map(_.getProperties.getCountry()).getOrElse("None")
    }

    def getClassifiers(placesData: Option[PlacesDTO]): List[PlacesClassifiersDTO] = {
        placesData.map(_.getProperties.getClassifiers.toList).getOrElse(List[PlacesClassifiersDTO]())
    }

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
