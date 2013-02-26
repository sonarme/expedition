package com.sonar.expedition.common.serialization

import org.scalatest.{FlatSpec, BeforeAndAfter}
import Serialization._
import org.openrtb.{Bool, BidRequest}

class SerializationTest extends FlatSpec with BeforeAndAfter {


    "deserialization" should "be able to read the json" in {
        val testJson = """{"at": 2,"device":{"connectiontype": 0,"devicetype": 1, "geo":{"lat": 42.357777,"lon": -71.06167,"type": 3},"ip": "201.252.0.0","js": 0,"make": "Apple","model": "iPhone","os": "iPhone OS","ua": "Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML, like Gecko) Version/4.0 Mobile/7A341 Safari/528.16"},"ext": {"udi": {}},"id": "3jwMOvJfOB","imp": [{"banner":{"btype":[1,3],"h": 50,"mimes":["image/gif","image/jpeg","image/png"],"w": 300},"bidfloor": 0,"displaymanager": "SOMA","id": "1"}],"site":{"cat": ["IAB1"],"domain": "www.jacksbar.co.uk","id": "0","name": "Jack Rabbit Slims","publisher": {"id": "6"}},"user":{"gender": "M","yob": 1979}}"""
        val br = fromByteArray(testJson.getBytes("utf8"), new BidRequest())
        assert(br.getId === "3jwMOvJfOB")
    }
    "serialization" should "use 0 and 1 for booleans" in {
        val br = new BidRequest("1")
        br.setAllimps(Bool.FALSE)
        assert(new String(toByteArray(br), "utf8") === """{"id":"1","allimps":0}""")
        br.setAllimps(Bool.TRUE)
        assert(new String(toByteArray(br), "utf8") === """{"id":"1","allimps":1}""")
    }
}
