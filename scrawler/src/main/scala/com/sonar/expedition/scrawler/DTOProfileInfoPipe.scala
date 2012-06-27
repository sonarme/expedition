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
                var value2 = getFBId(serviceType, serviceType2)
                var value3 = getFBJson(serviceType, serviceType2, jsondata)
                var value4 = getLinkedInId(serviceType, serviceType2)
                var value5 = getLNKDINJson(serviceType, serviceType2, jsondata2)
                (fields._1, value2, value3, value4, value5)
        }
                .mapTo(Fields.ALL -> Fields.ALL) {
            fields: (String, String, Option[String], String, Option[String]) =>
                val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
                var fbid = getID(jsondata)
                var lnid = getID(jsondata2)
                (id, fbid, jsondata, lnid, jsondata2)
        } //.write(out2)
                .mapTo(Fields.ALL ->('rowkey, 'username, 'fbid, 'lnid, 'fbedu, 'lnedu, 'fbwork, 'lnwork, 'city)) {
            fields: (String, Option[String], Option[String], Option[String], Option[String]) =>
                val (id, serviceType, jsondata, serviceType2, jsondata2) = fields
                var fbid = getID(jsondata)
                var lnid = getID(jsondata2)
                var fbedu = getEducation(jsondata)
                var lnedu = getEducation(jsondata2)
                var fbwork = getWork(jsondata)
                var lnwork = getWork(jsondata2)
                var fbusername = getUserName(jsondata)
                var fbcity = getCity(jsondata)
                var lncity = getCity(jsondata2)
                var city = formCityList(fbcity, lncity)
                //var currwork = getWork(jsondata,jsondata2)
                //var currcity = getCity(jsondata,jsondata2)
                //var name
                //var worktitle
                //var workhistory = getWorkHistory(jsondata,jsondata2)
                //var locationhistory
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
            fields: (String, Option[String], Option[String], Option[String], scala.collection.immutable.List[UserEducation], scala.collection.immutable.List[UserEmployment], List[String]) =>
                val (rowkey, fbname, fbid, lnid, edu, work, city) = fields
                val educationschool = getFirstEdu(edu)
                val edudegree = getFirstEduDegree(edu)
                var eduyear = getFirstEduDegreeYear(edu)
                val workcomp = getFirstWork(work)
                val worktitle = getWorkTitle(work)
                val workdesc = getWorkSummary(work)
                val ccity = getcurrCity(city)
                //(rowkey, fbname.mkString, md5SumString(fbid.mkString.getBytes("UTF-8")), md5SumString(lnid.mkString.getBytes("UTF-8")), educationschool.mkString, workcomp.mkString, ccity.mkString, edudegree.mkString, eduyear.mkString, worktitle.mkString, workdesc.mkString)
                (rowkey, fbname.mkString, fbid.mkString, lnid.mkString, educationschool.mkString, workcomp.mkString, ccity.mkString, edudegree.mkString, eduyear.mkString, worktitle.mkString)
            //}.project('key, 'name, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
        }

        dtoProfiles

    }

    def getcurrCity(city: List[String]): String = {
        city.headOption.mkString
    }

    def getFirstEdu(edu: scala.collection.immutable.List[UserEducation]): String = {
        edu.headOption.map(_.getSchoolName()).mkString
    }


    def getFirstEduDegree(edu: scala.collection.immutable.List[UserEducation]): String = {
        edu.headOption.map(_.getDegree()).mkString
    }


    def getFirstEduDegreeYear(edu: scala.collection.immutable.List[UserEducation]): String = {
        edu.headOption.map(_.getYear()).mkString
    }

    def getFirstWork(work: scala.collection.immutable.List[UserEmployment]): String = {
        work.headOption.map(_.getCompanyName()).mkString
    }

    def getWorkSummary(work: scala.collection.immutable.List[UserEmployment]): String = {
        work.headOption.map(_.getSummary()).mkString
    }

    def getWorkTitle(work: scala.collection.immutable.List[UserEmployment]): String = {
        work.headOption.map(_.getTitle()).mkString
    }

    def getLatitute(checkins: List[CheckinObjects]): String = {
        checkins.map(_.getLatitude).toString
    }

    def formCityList(fbcitydata: Option[String], lnkdcitydata: Option[String]): List[String] = {
        fbcitydata.toList ++ lnkdcitydata.toList
    }

    def formWorkHistoryList(fbworkdata: scala.collection.immutable.List[UserEmployment], lnkdworkdata: scala.collection.immutable.List[UserEmployment]): scala.collection.immutable.List[UserEmployment] = {
        fbworkdata.toList ++ lnkdworkdata.toList
    }

    def formEducationlist(fbedulist: scala.collection.immutable.List[UserEducation], lnedulist: scala.collection.immutable.List[UserEducation]): scala.collection.immutable.List[UserEducation] = {
        fbedulist ++ lnedulist
    }

    def getWork(workJson: Option[String]): List[UserEmployment] = {
        val resultantJson = workJson.mkString;
        val fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        Option(fbdata).map(_.getWork().toList).getOrElse(List[UserEmployment]())
    }

    def getEducation(educationJson: Option[String]): List[UserEducation] = {
        val resultantJson = educationJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        Option(fbdata).map(_.getEducation().toList).getOrElse(List[UserEducation]())

    }

    def getUserName(fbJson: Option[String]): Option[String] = {
        val resultantJson = fbJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var res4 = Option(fbdata)
        res4.map(_.getFullName())
    }

    def getCity(fbJson: Option[String]): Option[String] = {
        val resultantJson = fbJson.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var res4 = Option(fbdata)
        res4.map(_.getLocation())
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
        if (serviceTypeFB == null)
            None
        if ((serviceTypeFB == serviceTypeLn) && serviceTypeFB.trim == "fb") {
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
        if (serviceTypeFB == null)
            None
        if (serviceTypeFB == serviceTypeLn && serviceTypeFB.trim == "fb") {
            None
        }
        else {
            Option(jsonString)
        }
    }

    def getID(jsonString: Option[String]): Option[String] = {
        val resultantJson = jsonString.mkString;
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")));
        var res4 = Option(fbdata)
        res4.map(_.getUserId())
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
