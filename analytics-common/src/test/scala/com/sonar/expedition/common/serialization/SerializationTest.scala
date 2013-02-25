package com.sonar.expedition.common.serialization

import org.scalatest.{FlatSpec, BeforeAndAfter}
import Serialization._
import org.openrtb.{Bool, BidRequest}

class SerializationTest extends FlatSpec with BeforeAndAfter {


    "deserialization" should "be able to read the json" in {
        val testJson = """{"id":"1","imp":[],"site":null,"app":{"id":"123","name":"sonar","domain":"sonar.me","cat":null,"sectioncat":null,"pagecat":null,"ver":null,"bundle":null,"privacypolicy":0,"paid":0,"publisher":{"id":null,"name":null,"cat":["IAB","IAB3"],"domain":null,"ext":null},"content":null,"keywords":null,"storeurl":null,"ext":null},"device":{"dnt":0,"ua":null,"ip":null,"geo":{"lat":40.738934,"lon":-73.98811,"country":null,"region":null,"regionfips104":null,"metro":null,"city":null,"zip":null,"type":0,"ext":null},"didsha1":null,"didmd5":null,"dpidsha1":null,"dpidmd5":null,"ipv6":null,"carrier":null,"language":null,"make":null,"model":null,"os":null,"Osv":null,"Js":0,"connectiontype":0,"devicetype":0,"flashver":null,"ext":null},"user":null,"at":2,"tmax":120,"wseat":null,"allimps":0,"cur":null,"bcat":null,"badv":null,"ext":null}"""
        val br = fromByteArray(testJson.getBytes("utf8"), new BidRequest())
        assert(br.getId === "1")
    }
    "serialization" should "use 0 and 1 for booleans" in {
        val br = new BidRequest("1")
        br.setAllimps(Bool.FALSE)
        assert(new String(toByteArray(br), "utf8") === """{"id":"1","allimps":0}""")
        br.setAllimps(Bool.TRUE)
        assert(new String(toByteArray(br), "utf8") === """{"id":"1","allimps":1}""")
    }
}
