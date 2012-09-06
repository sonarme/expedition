package com.sonar.expedition.scrawler.crawler

import org.jsoup.Jsoup
import reflect.BeanProperty
import org.jsoup.select.Elements
import scala.collection.JavaConversions._
import org.jsoup.nodes.Element
import org.jsoup.parser.Tag

/**
 * extract content from a page (content)
 * @param content
 */
class Extractor(@BeanProperty val content: String) {

    def doc = Jsoup.parse(content)

    def businessName(): String = ""

    def category(): String = ""

    def subcategory(): String = ""

    def rating(): String = "" //right now it is on a scale of 0 to 5 (Because we started with yelp first)

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

    def peopleCount(): Int = 0

    def checkinCount(): Int = 0


    def extractById(id: String): Option[String] = {
        Option(doc.getElementById(id)) match {
            case Some(ele) => Some(Jsoup.parse(ele.text()).text())
            case None => None
        }
    }

    def extractByAttributeValue(key: String, value: String): Option[String] = {
        doc.getElementsByAttributeValue(key, value) match {
            case eles: Elements if eles.size() > 0 => Some(eles.iterator().map(e => Jsoup.parse(e.text()).text()).mkString(", "))
            case _ => None
        }
    }

    def extractListByAttributeValue(key: String, value: String): List[String] = {
        doc.getElementsByAttributeValue(key, value) match {
            case eles: Elements if eles.size() > 0 => eles.iterator().map(e => Jsoup.parse(e.text()).text()).toList
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

    override def rating() = extractByAttributeValueAttribute("itemprop", "ratingValue", "content").getOrElse("") //todo: need to normalize the rating value across the board. maybe a percentage?

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
    override def businessName() = extractById("coreInfo.name").getOrElse("")

    override def category() = extractByDtAndValue("Categories:")

    override def rating() = extractByAttributeValue("class", "average") match {
        case Some(rate) => (rate.toDouble / 20).toString
        case None => ""
    }


    override def latitude() = extractByAttributeValueAttribute("property", "place:location:latitude", "content").getOrElse("0.0").toDouble

    override def longitude() = extractByAttributeValueAttribute("property", "place:location:longitude", "content").getOrElse("0.0").toDouble

    override def address() = extractByAttributeValueAttribute("property", "og:street-address", "content").getOrElse("")

    override def city() = extractByAttributeValueAttribute("property", "og:locality", "content").getOrElse("")

    override def state() = extractByAttributeValueAttribute("property", "og:region", "content").getOrElse("")

    override def zip() = extractByAttributeValueAttribute("property", "og:postal-code", "content").getOrElse("")

    override def phone() = extractById("coreInfo.phone").getOrElse("")

    override def priceRange() = extractByDtAndValue("Price:")

    override def reviewCount() = Option(doc.getElementById("coreInfo.tabs.reviews")) match {
        case Some(ele) => {
            ele.getElementsByClass("itemCount").headOption match {
                case Some(e) => e.text().stripPrefix("(").stripSuffix(")").toInt
                case None => 0
            }
        }
        case None => 0
    }

    //custom extraction for citysearch
    def extractByDtAndValue(value: String) = {
        doc.getElementsByClass("sideBySide") match {
            case eles: Elements if (eles.size() > 0) => {
                val targetElement = eles.reduceLeft {
                    (a, b) => {
                        if (a.getElementsByTag("dt").head.text().equals(value))
                            a
                        else
                            b
                    }
                }
                //check if we found the correct element
                if (targetElement.getElementsByTag("dt").head.text().equals(value)) {
                    Jsoup.parse(targetElement.getElementsByTag("span").text()).text()
                } else
                    ""
            }
            case _ => ""
        }
    }
}

class FoursquareExtractor(content: String) extends Extractor(content) {
    override def businessName() = extractByAttributeValue("itemprop", "name").getOrElse("")

    override def category() = extractByAttributeValue("class", "categories").getOrElse("")

    override def rating() = extractByAttributeValue("itemprop", "ratingValue") match {
        // scale of 0 to 10
        case Some(rate) => (rate.toDouble / 2).toString
        case None => ""
    }

    override def latitude() = extractByAttributeValueAttribute("property", "playfoursquare:location:latitude", "content").getOrElse("0.0").toDouble

    override def longitude() = extractByAttributeValueAttribute("property", "playfoursquare:location:longitude", "content").getOrElse("0.0").toDouble

    override def address() = extractByAttributeValue("itemprop", "streetAddress").getOrElse("")

    override def city() = extractByAttributeValue("itemprop", "addressLocality").getOrElse("")

    override def state() = extractByAttributeValue("itemprop", "addressRegion").getOrElse("")

    override def zip() = extractByAttributeValue("itemprop", "postalCode").getOrElse("")

    override def phone() = extractByAttributeValue("itemprop", "telephone").getOrElse("")

    override def priceRange() = extractByAttributeValue("itemprop", "priceRange").getOrElse("").replace(" ", "") // "$ $" -> "$$"

    override def reviewCount() = extractByAttributeValueAttribute("property", "playfoursquare:number_of_tips", "content").getOrElse("0").toInt

}