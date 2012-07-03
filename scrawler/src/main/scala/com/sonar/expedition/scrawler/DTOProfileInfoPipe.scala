package com.sonar.expedition.scrawler

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO, Checkin}
import java.security.MessageDigest
import cascading.pipe.{Each, Pipe}
import com.twitter.scalding.TextLine
import cascading.flow.FlowDef
import com.twitter.scalding._
import java.nio.ByteBuffer
import json.JacksonObjectMapper
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import java.util.Calendar

class DTOProfileInfoPipe(args: Args) extends Job(args) {


    def getDTOProfileInfoInTuples(datahandle: RichPipe): RichPipe = {
        val numProfiles = datahandle.groupBy('id) {
            _.size
        }.rename('size -> 'numProfiles)

        val profiles = datahandle.joinWithSmaller('id -> 'id, numProfiles)

        val dupProfiles = profiles.rename(('id, 'serviceType, 'jsondata, 'numProfiles) ->('id2, 'serviceType2, 'jsondata2, 'numProfiles2))

        val dtoProfiles = profiles.joinWithSmaller('id -> 'id2, dupProfiles).project('id, 'serviceType, 'jsondata, 'serviceType2, 'jsondata2)
                .mapTo(Fields.ALL -> Fields.ALL) {
            fields: (String, String, String, String, String) =>
                val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
//                val fbId = getFBId(serviceType, serviceType2)
                val fbJson = getFBJson(serviceType, serviceType2, jsondata)
//                val lnId = getLinkedInId(serviceType, serviceType2)
                val lnJson = getLNKDINJson(serviceType, serviceType2, jsondata2)
                (fields._1, serviceType, fbJson, serviceType2, lnJson)
        }
                .mapTo(Fields.ALL -> Fields.ALL) {
            fields: (String, String, Option[String], String, Option[String]) =>
                val (id, serviceType, fbJson, serviceType2, lnJson) = fields
                val fbServiceProfile = parseJson(fbJson)
                val lnServiceProfile = parseJson(lnJson)
                val fbid = getID(fbServiceProfile)
                val lnid = getID(lnServiceProfile)
                (fields._1, fbid, fbServiceProfile, lnid, lnServiceProfile)
        }
                .mapTo(Fields.ALL ->('rowkey, 'username, 'fbid, 'lnid, 'fbedu, 'lnedu, 'fbwork, 'lnwork, 'city)) {
            fields: (String, Option[String], Option[ServiceProfileDTO], Option[String], Option[ServiceProfileDTO]) =>
                val (id, fbid, fbJson, lnid, lnJson) = fields
                val fbedu = getEducation(fbJson)
                val lnedu = getEducation(lnJson)
                val fbwork = getWork(fbJson)
                val lnwork = getWork(lnJson)
                val fbusername = getUserName(fbJson)
                val fbcity = getCity(fbJson)
                val lncity = getCity(lnJson)
                val city = formCityList(fbcity, lncity)
                (fields._1, fbusername, fbid, lnid, fbedu, lnedu, fbwork, lnwork, city)
        }.map(Fields.ALL ->('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity)) {
            fields: (String, Option[String], Option[String], Option[String], List[UserEducation], List[UserEducation], List[UserEmployment], List[UserEmployment], List[String]) =>
                val (rowkey, fbname, fbid, lnid, fbedu, lnedu, fbwork, lnwork, city) = fields
                val edulist = formEducationlist(fbedu, lnedu)
                val worklist = formWorkHistoryList(fbwork, lnwork)
                (rowkey, fbname, fbid, lnid, edulist, worklist, city)
        }
                .project(('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity))
                .mapTo(Fields.ALL ->('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle)) {
            fields: (String, Option[String], Option[String], Option[String], List[UserEducation], List[UserEmployment], List[String]) =>
                val (rowkey, fbname, fbid, lnid, edu, work, city) = fields
                val educationschool = getFirstEdu(edu)
                val edudegree = getFirstEduDegree(edu)
                var eduyear = getFirstEduDegreeYear(edu)
                val workcomp = getFirstWork(work)
                val worktitle = getWorkTitle(work)
                val ccity = getcurrCity(city)
                //(rowkey, fbname.mkString, md5SumString(fbid.mkString.getBytes("UTF-8")), md5SumString(lnid.mkString.getBytes("UTF-8")), educationschool.mkString, workcomp.mkString, ccity.mkString, edudegree.mkString, eduyear.mkString, worktitle.mkString, workdesc.mkString)
                (rowkey, fbname.getOrElse(None), fbid.mkString, lnid.mkString, educationschool, workcomp, ccity, edudegree, eduyear, worktitle)
            //}.project('key, 'name, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
        }

        dtoProfiles

    }

    def getcurrCity(city: List[String]): String = {
        city.headOption.mkString
    }

    def getFirstEdu(edu: List[UserEducation]): String = {
        edu.headOption.map(_.getSchoolName()).mkString
    }


    def getFirstEduDegree(edu: List[UserEducation]): String = {
        edu.headOption.map(_.getDegree()).mkString
    }


    def getFirstEduDegreeYear(edu: List[UserEducation]): String = {
        edu.headOption.map(_.getYear()).mkString
    }

    def getFirstWork(work: List[UserEmployment]): String = {
        work.headOption.map(_.getCompanyName()).mkString
    }

    def getWorkSummary(work: List[UserEmployment]): String = {
        work.headOption.map(_.getSummary()).mkString
    }

    def getWorkTitle(work: List[UserEmployment]): String = {
        work.headOption.map(_.getTitle()).mkString
    }

    def getLatitute(checkins: List[CheckinObjects]): String = {
        checkins.map(_.getLatitude).toString()
    }

    def formCityList(fbcitydata: Option[String], lnkdcitydata: Option[String]): List[String] = {
        fbcitydata.toList ++ lnkdcitydata.toList
    }

    def formWorkHistoryList(fbworkdata: List[UserEmployment], lnkdworkdata: List[UserEmployment]): List[UserEmployment] = {
        fbworkdata.toList ++ lnkdworkdata.toList
    }

    def formEducationlist(fbedulist: scala.collection.immutable.List[UserEducation], lnedulist: scala.collection.immutable.List[UserEducation]): scala.collection.immutable.List[UserEducation] = {
        fbedulist ++ lnedulist
    }

    def getWork(serviceProfile: Option[ServiceProfileDTO]): List[UserEmployment] = {
        serviceProfile.map(_.getWork().toList).getOrElse(List[UserEmployment]())
    }

    def getEducation(serviceProfile: Option[ServiceProfileDTO]): List[UserEducation] = {
        serviceProfile.map(_.getEducation().toList).getOrElse(List[UserEducation]())
    }

    def getUserName(serviceProfile: Option[ServiceProfileDTO]): Option[String] = {
        serviceProfile.map(_.getFullName())
    }

    def getCity(serviceProfile: Option[ServiceProfileDTO]): Option[String] = {
        serviceProfile.map(_.getLocation())
    }

    def getFBId(serviceTypeFB: String, serviceTypeLn: String): Option[String] = {
        if (serviceTypeFB == null)
            None
        if ((serviceTypeFB == serviceTypeLn) && serviceTypeFB.trim == "ln") {
            None
        }
        else {
            Option(serviceTypeFB)
        }
    }

    def getLinkedInId(serviceTypeFB: String, serviceTypeLn: String): Option[String] = {
        if (serviceTypeLn == null)
            None
        if ((serviceTypeFB == serviceTypeLn) && serviceTypeLn.trim == "fb") {
            None
        }
        else {
            Option(serviceTypeLn)
        }
    }

    def getFBJson(serviceTypeFB: String, serviceTypeLn: String, jsonString: String): Option[String] = {
        if (serviceTypeFB == null)
            None
        if (serviceTypeFB == serviceTypeLn && serviceTypeFB.trim == "ln") {
            None
        }
        else {
            Option(jsonString)
        }
    }

    def getLNKDINJson(serviceTypeFB: String, serviceTypeLn: String, jsonString: String): Option[String] = {
        if (serviceTypeLn == null)
            None
        if (serviceTypeFB == serviceTypeLn && serviceTypeLn.trim == "fb") {
            None
        }
        else {
            Option(jsonString)
        }
    }

    def parseJson(jsonStringOption: Option[String]): Option[ServiceProfileDTO] = {
        jsonStringOption map {
            jsonString =>
                JacksonObjectMapper.objectMapper.readValue(jsonString, classOf[ServiceProfileDTO])
        }
    }


    def getID(serviceProfile: Option[ServiceProfileDTO]): Option[String] = {
        serviceProfile.map(_.getUserId())
    }

    def prettyPrint(foo: Option[String]): String = foo match {
        case Some(x) => x
        case None => "None"
    }

    def md5SumString(bytes: Array[Byte]): String = {
        val md5 = MessageDigest.getInstance("MD5")
        md5.reset()
        md5.update(bytes)
        md5.digest().map(0xFF & _).map {
            "%02x".format(_)
        }.foldLeft("") {
            _ + _
        }
    }

    def getFriendName(friends: List[FriendObjects]): String = {
        friends.map(_.getFriendName).toString()
    }

    def getServiceType(friends: List[FriendObjects]): String = {
        friends.map(_.getServiceType).toString()
    }

    def getServiceProfileId(friends: List[FriendObjects]): String = {
        friends.map(_.getServiceProfileId).toString()
    }

    def getVenue(checkins: List[CheckinObjects]): String = {
        checkins.map(_.getVenueName).toString
    }

}
