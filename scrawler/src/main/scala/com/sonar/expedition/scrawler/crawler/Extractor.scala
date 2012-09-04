package com.sonar.expedition.scrawler.crawler

import org.jsoup.Jsoup
import reflect.BeanProperty
import org.jsoup.select.Elements
import scala.collection.JavaConversions._

class Extractor(@BeanProperty val content: String) {

    def doc = Jsoup.parse(content)

    def businessName(): String = ""

    def category(): String = ""

    def subcategory(): String = ""

    def rating(): String = ""

    def latitude(): Double = 0.0

    def longitude(): Double = 0.0

    def address(): String = ""

    def city(): String = ""

    def state(): String = ""

    def zip(): String = ""

    def phone(): String = ""

    def priceRange(): String = ""

    def reviewCount(): Int = 0

    def reviews(): List[String] = List.empty[String]


    def extractById(id: String): Option[String] = {
        Option(doc.getElementById(id)) match {
            case Some(ele) => Some(ele.text())
            case None => None
        }
    }

    def extractByAttributeValue(key: String, value: String): Option[String] = {
        doc.getElementsByAttributeValue(key, value) match {
            case eles: Elements if eles.size() > 0 => Some(eles.iterator().map(_.text()).mkString(", "))
            case _ => None
        }
    }

    def extractListByAttributeValue(key: String, value: String): List[String] = {
        doc.getElementsByAttributeValue(key, value) match {
            case eles: Elements if eles.size() > 0 => eles.iterator().map(_.text()).toList
            case _ => List.empty[String]
        }
    }

    /**
     * Find element with key and value.  Then return the value of the specified attribute in that element
     * @param key
     * @param value
     * @param attribute
     * @return
     */
    def extractByAttributeValueAttribute(key: String, value: String, attribute: String): Option[String] = {
        doc.getElementsByAttributeValue(key, value) match {
            case eles: Elements if eles.size() > 0 => Some(eles.get(0).attr(attribute))
            case _ => None
        }
    }
}

class YelpExtractor(content: String) extends Extractor(content) {
    override def businessName() = extractByAttributeValue("itemprop", "name").getOrElse("")

    override def category() = extractByAttributeValue("itemprop", "title").getOrElse("")

    override def subcategory() = extractById("subcategory").getOrElse("")

    override def rating() = extractByAttributeValueAttribute("itemprop", "ratingValue", "content").getOrElse("")

    override def latitude() = extractByAttributeValueAttribute("property", "og:latitude", "content").getOrElse("0.0").toDouble

    override def longitude() = extractByAttributeValueAttribute("property", "og:longitude", "content").getOrElse("0.0").toDouble

    override def address() = extractByAttributeValue("itemprop", "streetAddress").getOrElse("")

    override def city() = extractByAttributeValue("itemprop", "addressLocality").getOrElse("")

    override def state() = extractByAttributeValue("itemprop", "addressRegion").getOrElse("")

    override def zip() = extractByAttributeValue("itemprop", "postalCode").getOrElse("")

    override def phone() = extractByAttributeValue("itemprop", "telephone").getOrElse("")

    override def priceRange() = extractById("price_tip").getOrElse("")

    override def reviewCount() = extractByAttributeValue("itemprop", "reviewCount").getOrElse("0").toInt

    override def reviews() = extractListByAttributeValue("itemprop", "description")
}

class CitySearchExtractor(content: String) extends Extractor(content) {

}