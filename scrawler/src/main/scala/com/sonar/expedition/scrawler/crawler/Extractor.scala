package com.sonar.expedition.scrawler.crawler

import org.jsoup.Jsoup
import reflect.BeanProperty
import org.jsoup.select.Elements
import scala.collection.JavaConversions._
import com.sonar.expedition.scrawler.crawler.FacebookExtractor._
import com.sonar.expedition.scrawler.crawler.TwitterExtractor._
import com.fasterxml.jackson.databind.{DeserializationFeature, PropertyNamingStrategy, DeserializationConfig, ObjectMapper}
import com.sonar.dossier.ScalaGoodies._

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

    def wereHereCount(): Int = 0

    def talkingAboutCount(): Int = 0

    def likes(): Int = 0

    def price(): Double = 0.0

    def purchased(): Int = 0

    def savingsPercent(): Int = 0

    def dealDescription(): String = ""

    def dealImage(): String = ""

    def dealRegion(): String = ""

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

    override def reviewCount() = extractByAttributeValue("itemprop", "reviewCount").getOrElse("0").replace(",", "").toInt

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
                case Some(e) => e.text().stripPrefix("(").stripSuffix(")").replace(",", "").toInt
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

    override def reviewCount() = extractByAttributeValueAttribute("property", "playfoursquare:number_of_tips", "content").getOrElse("0").replace(",", "").toInt

    override def reviews() = extractListByAttributeValue("class", "tipText")

    override def peopleCount() = try {
        doc.getElementsByClass("statsnot6digits").get(1).text().replace(",", "").toInt
    } catch {
        case e: Exception => 0
    }

    override def checkinCount() = try {
        doc.getElementsByClass("statsnot6digits").get(2).text().replace(",", "").toInt
    } catch {
        case e: Exception => 0
    }
}

class FacebookExtractor(content: String) extends Extractor(content) {

    def fbPlace = try {
        FacebookPlaceObjectMapper.readValue(content, classOf[FacebookPlace])
    } catch {
        case e: Exception => new FacebookPlace()
    }

    override def businessName() = fbPlace.name

    override def category() = fbPlace.category

    override def latitude() = try {
        fbPlace.location.latitude
    } catch {
        case e: Exception => 0.0
    }

    override def longitude() = try {
        fbPlace.location.longitude
    } catch {
        case e: Exception => 0.0
    }

    override def address() = ?(fbPlace.location.street)

    override def city() = ?(fbPlace.location.city)

    override def state() = ?(fbPlace.location.state)

    override def zip() = ?(fbPlace.location.zip)

    override def phone() = fbPlace.phone

    override def likes() = fbPlace.likes

    override def talkingAboutCount() = fbPlace.talkingAboutCount

    override def wereHereCount() = fbPlace.wereHereCount

    override def checkinCount() = fbPlace.checkins
}

object FacebookExtractor {
    val FacebookPlaceObjectMapper = new ObjectMapper
    FacebookPlaceObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    FacebookPlaceObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
}

case class FacebookPlace(@BeanProperty name: String,
                         @BeanProperty phone: String = null,
                         @BeanProperty category: String = null,
                         @BeanProperty location: FacebookPlaceLocation = null,
                         @BeanProperty checkins: Int = 0,
                         @BeanProperty wereHereCount: Int = 0,
                         @BeanProperty talkingAboutCount: Int = 0,
                         @BeanProperty likes: Int = 0) {
    def this() = this(null)
}

case class FacebookPlaceLocation(@BeanProperty street: String,
                                 @BeanProperty city: String = null,
                                 @BeanProperty state: String = null,
                                 @BeanProperty country: String = null,
                                 @BeanProperty zip: String = null,
                                 @BeanProperty latitude: Double = 0.0,
                                 @BeanProperty longitude: Double = 0.0) {
    def this() = this(null)
}

class TwitterExtractor(content: String) extends Extractor(content) {

    def twGeo = TwitterGeoObjectMapper.readValue(content, classOf[TwitterGeo])

    override def businessName() = twGeo.name

    override def latitude() = try {
        twGeo.geometry.coordinates(1)
    } catch {
        case e: Exception => 0.0
    }

    override def longitude() = try {
        twGeo.geometry.coordinates(0)
    } catch {
        case e: Exception => 0.0
    }

    override def address() = ?(twGeo.attributes.streetAddress)

    override def zip() = ?(twGeo.attributes.postalCode)

    override def phone() = ?(twGeo.attributes.phone)
}

case class TwitterGeo(@BeanProperty name: String,
                      @BeanProperty attributes: GeoAttributes = null,
                      @BeanProperty geometry: GeoGeometry = null) {
    def this() = this(null)
}

case class GeoAttributes(@BeanProperty streetAddress: String,
                         @BeanProperty postalCode: String = null,
                         @BeanProperty region: String = null,
                         @BeanProperty locality: String = null,
                         @BeanProperty phone: String = null) {
    def this() = this(null)
}

case class GeoGeometry(@BeanProperty coordinates: Array[Double]) {
    def this() = this(null)
}

object TwitterExtractor {
    val TwitterGeoObjectMapper = new ObjectMapper
    TwitterGeoObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    TwitterGeoObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
}

class LivingSocialExtractor(content: String) extends Extractor(content) {
    override def businessName() = extractByAttributeValueAttribute("property", "og:merchant", "content").getOrElse(extractByAttributeValueAttribute("property", "og:title", "content").getOrElse(""))

    val latlng = doc.getElementsByClass("directions").headOption match {
        case Some(d) => d.getElementsByTag("a").headOption match {
            case Some(a) => {
                val href = a.attr("href")
                href.substring(href.indexOf("q=") + 2)
            }
            case None => "0.0"
        }
        case None => "0.0"
    }

    override def category() = try {
        content.substring(content.indexOf("dealCategory        =") + 23, content.indexOf("\",", content.indexOf("dealCategory        =") + 21))
    } catch {
        case e: Exception => ""
    }

    override def latitude() = try {
        latlng.split(",")(0).trim.toDouble
    } catch {
        case e: Exception => 0.0
    }

    override def longitude() = try {
        latlng.split(",")(1).trim.toDouble
    } catch {
        case e: Exception => 0.0
    }

    def fullAddress = extractByAttributeValue("class", "street_1").getOrElse("").split(",")

    override def address() = try {
        fullAddress(0).trim
    } catch {
        case e: Exception => ""
    }

    override def city() = try {
        fullAddress(1).trim
    } catch {
        case e: Exception => ""
    }

    override def state() = try {
        fullAddress(2).split(" ")(1).trim
    } catch {
        case e: Exception => ""
    }

    override def zip() = try {
        fullAddress(2).split(" ")(2).trim
    } catch {
        case e: Exception => ""
    }

    override def phone() = extractByAttributeValue("class", "phone").getOrElse("").stripSuffix(" |")

    override def price() = doc.getElementsByClass("deal-price").headOption match {
        case Some(e) => try{
            Jsoup.parse(e.text()).text().substring(1).replace(",", "").toDouble
        } catch {
            case ex: Exception => println("price: " + e.text() + "\n" + ex); 0.0
        }
        case None => 0.0
    }

    override def purchased() = {
        val purchasedStr = Option(doc.getElementById("deal-purchase-count")) match {
            case Some(d) => d.getElementsByClass("value").headOption match {
                case Some(e) => e.text()
                case None => ""
            }
            case None => doc.getElementsByClass("purchased").headOption match {
                //for /escapes
                case Some(d) => d.getElementsByClass("value").headOption match {
                    case Some(e) => e.text()
                    case None => ""
                }
                case None => ""
            }
        }
        try {
            purchasedStr.replace(",", "").toInt
        } catch {
            case e: Exception => println("purchased: " + purchasedStr + "\n" + e)
            try {
                purchasedStr.replace(",", "").replace("?", "").toInt
            } catch {
                case f: Exception => 0
            }
        }
    }

    override def savingsPercent() = try {
        extractById("percentage").getOrElse {
            doc.getElementsByClass("discount").headOption match {
                case Some(d) => d.getElementsByClass("value").headOption match {
                    case Some(e) => e.text()
                    case None => ""
                }
                case None => ""
            }
        }.stripSuffix("%").toInt
    } catch {
        case e: Exception => println(e); 0
    }

    override def dealDescription() = extractById("view-details-full").getOrElse{
        doc.getElementsByClass("deal-description").headOption match {
            case Some(d) => Jsoup.parse(d.text()).text()
            case None => ""
        }
    }

    override def dealImage() = doc.getElementsByClass("portrait").headOption match {
        case Some(e) => e.getElementsByTag("img").headOption match {
            case Some(img) => img.attr("src")
            case None => ""
        }
        case None => ""
    }

    override def dealRegion() = extractByAttributeValue("class", "market").getOrElse("")
}