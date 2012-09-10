package com.sonar.expedition.scrawler.util

import reflect.BeanProperty
import ch.hsr.geohash.WGS84Point
import ch.hsr.geohash.util.VincentyGeodesy

object VenueMatcher {
    def matches(venue1: Venue, venue2: Venue): Boolean = {
        val matchesCityStateZip = try {
            venue1.city.toLowerCase.equals(venue2.city.toLowerCase) && venue1.state.toLowerCase.equals(venue2.state.toLowerCase) && venue1.zip.toLowerCase.equals(venue2.zip.toLowerCase)
        } catch {
            case e: Exception => println(e); println("a:" + venue1.city + " " + venue1.state + " " + venue1.zip + ", b:" + venue2.city + " " + venue2.state + " " + venue2.zip); false
        }
        val point1 = new WGS84Point(venue1.lat, venue1.lng)
        val point2 = new WGS84Point(venue2.lat, venue2.lng)
        val distanceMeters = VincentyGeodesy.distanceInMeters(point1, point2)

        if (distanceMeters < 300)
            true
        else if (matchesCityStateZip) {
            if (venue1.address == null || venue2.address == null || venue1.address == "" || venue2.address == "")
                false
            else {
                try {
                    //attempt to match the number portion of the address
                    if (venue1.address.substring(0, venue1.address.indexOf(" ")).equals(venue2.address.substring(0, venue2.address.indexOf(" "))))
                        true
                    else if (venue1.name.toLowerCase.equals(venue2.name.toLowerCase))
                        true
                    else
                        false
                } catch {
                    case e: Exception => println(e); println("a:" + venue1.address + ", b:" + venue2.address); false
                }
            }
        } else
            false
    }
}

case class Venue(@BeanProperty name: String,
                 @BeanProperty address: String,
                 @BeanProperty city: String,
                 @BeanProperty state: String,
                 @BeanProperty zip: String,
                 @BeanProperty lat: Double,
                 @BeanProperty lng: Double)
