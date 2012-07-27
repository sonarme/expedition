package com.sonar.expedition.scrawler.pipes

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto.{UserEmployment, UserEducation, ServiceProfileDTO, Checkin}
import java.security.MessageDigest
import cascading.pipe.{Each, Pipe}
import com.twitter.scalding.TextLine
import cascading.flow.FlowDef
import com.twitter.scalding._
import java.nio.ByteBuffer
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper
import util.matching.Regex
import grizzled.slf4j.Logging
import com.sonar.dossier.dao.cassandra.{CheckinDao, ServiceProfileDao}
import java.util.Calendar
import com.sonar.expedition.scrawler.objs._
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.mongodb.util.JSON
import util.parsing.json.JSONObject
import twitter4j.json.JSONObjectType
import cascading.pipe.joiner.LeftJoin

class DTOProfileInfoPipe(args: Args) extends Job(args) {


    def getDTOProfileInfoInTuples(datahandle: RichPipe): RichPipe = {
        val numProfiles = datahandle.groupBy('id) {
            _.size
        }.rename('size -> 'numProfiles)

        val profiles = datahandle.joinWithSmaller('id -> 'id, numProfiles)

        val dupProfiles = profiles.rename(('id, 'serviceType, 'jsondata, 'numProfiles) ->('id2, 'serviceType2, 'jsondata2, 'numProfiles2))

        val dtoProfiles = profiles.joinWithSmaller('id -> 'id2, dupProfiles).project(('id, 'serviceType, 'jsondata, 'serviceType2, 'jsondata2))
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
                val fbServiceProfile = ScrawlerObjectMapper.parseJson(fbJson, classOf[ServiceProfileDTO])
                val lnServiceProfile = ScrawlerObjectMapper.parseJson(lnJson, classOf[ServiceProfileDTO])
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
                val fbusername = getUserName(fbJson).getOrElse(getUserName(lnJson).getOrElse(None))
                val fbcity = getCity(fbJson)
                val lncity = getCity(lnJson)
                val city = formCityList(fbcity, lncity)
                (fields._1, fbusername, fbid, lnid, fbedu, lnedu, fbwork, lnwork, city)
        }.map(Fields.ALL ->('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity)) {
            fields: (String, String, Option[String], Option[String], List[UserEducation], List[UserEducation], List[UserEmployment], List[UserEmployment], List[String]) =>
                val (rowkey, fbname, fbid, lnid, fbedu, lnedu, fbwork, lnwork, city) = fields
                val edulist = formEducationlist(fbedu, lnedu)
                val worklist = formWorkHistoryList(fbwork, lnwork)
                (rowkey, fbname, fbid, lnid, edulist, worklist, city)
        }
                .project(('skey, 'fbuname, 'fid, 'lid, 'edu, 'work, 'currcity))
                .mapTo(Fields.ALL ->('keyid, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle)) {
            fields: (String, String, Option[String], Option[String], List[UserEducation], List[UserEmployment], List[String]) =>
                val (rowkey, fbname, fbid, lnid, edu, work, city) = fields
                val educationschool = getFirstEdu(edu)
                val edudegree = getFirstEduDegree(edu)
                var eduyear = getFirstEduDegreeYear(edu)
                val workcomp = getFirstWork(work)
                val worktitle = getWorkTitle(work)
                val ccity = getcurrCity(city)
                //(rowkey, fbname.mkString, md5SumString(fbid.mkString.getBytes("UTF-8")), md5SumString(lnid.mkString.getBytes("UTF-8")), educationschool.mkString, workcomp.mkString, ccity.mkString, edudegree.mkString, eduyear.mkString, worktitle.mkString, workdesc.mkString)
                (rowkey, fbname, fbid.mkString, lnid.mkString, educationschool, workcomp, ccity, edudegree, eduyear, worktitle)
            //}.project('key, 'name, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)

        }
                .groupBy('keyid) {
            group => group
                    .toList[String]('uname, 'uname).sortBy('uname)
                    .toList[String]('fbid, 'first).sortBy('fbid)
                    .toList[String]('lnid, 'second).sortBy('lnid)
                    .toList[String]('educ, 'educ).sortBy('educ)
                    .toList[String]('worked, 'worked).sortBy('worked)
                    .toList[String]('city, 'city).sortBy('city)
                    .toList[String]('edegree, 'edegree).sortBy('edegree)
                    .toList[String]('eyear, 'eyear).sortBy('eyear)
                    .toList[String]('worktitle, 'worktitle).sortBy('worktitle)

        }
                .map(('keyid, 'first, 'second) ->('key, 'fbid, 'lnid)) {
            fields: (String, List[String], List[String]) =>
                val (user, first, second) = fields
                val filterFirst = first.filter {
                    fi: String => isNumeric(fi)
                }
                var headFirst = filterFirst.headOption.getOrElse("")
                if (!filterFirst.isEmpty && !filterFirst.tail.isEmpty) {
                    headFirst = filterFirst.tail.head
                }



                val filterSecond = second.filter {
                    fi: String => !isNumeric(fi)
                }
                val headSecond = filterSecond.headOption.getOrElse("")

                (user, headFirst, headSecond)
        }
                .mapTo(('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle) ->('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle)) {
            fields: (String, List[String], String, String, List[String], List[String], List[String], List[String], List[String], List[String]) =>
                val (key, uname, fbid, lnid, educ, worked, city, edegree, eyear, worktitle) = fields
                (key, getFirstNonNull(uname), fbid, lnid, getFirstNonNull(educ), getFirstNonNull(worked), getFirstNonNull(city), getFirstNonNull(edegree), getFirstNonNull(eyear), getFirstNonNull(worktitle))
        }

        dtoProfiles

    }

    def twitterProfileTuples(twitterPipe: RichPipe): RichPipe = {
        val data = twitterPipe
                .map('jsondata -> ('twid, 'twServiceProfile, 'twname)){
            twJson: String => {
                val twServiceProfile = ScrawlerObjectMapper.parseJson(Option(twJson), classOf[ServiceProfileDTO])
                val twid = getID(twServiceProfile).getOrElse(twJson)
                val twname = getUserName(twServiceProfile).getOrElse("")
                (twid, twServiceProfile, twname)
            }
        }
                .project('id, 'twid, 'twname)


        data
    }

    def getTotalProfileTuples(serviceProfileData: RichPipe, twServiceProfileData: RichPipe): RichPipe = {

        val fbln = getDTOProfileInfoInTuples(serviceProfileData)
        val tw = twitterProfileTuples(twServiceProfileData)

        val fblnWithTw = fbln.joinWithSmaller('key -> 'id, tw, new LeftJoin)
                .discard('id)
                .project(('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'twid, 'twname))
        val twWithFbln = tw.joinWithSmaller('id -> 'key, fbln, new LeftJoin)
                .discard('key)
                .rename('id -> 'key)
                .project(('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'twid, 'twname))

        val total = (fblnWithTw++twWithFbln)
                .unique(('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'twid, 'twname))
                .mapTo(('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'twid, 'twname) -> ('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'twid)) {
            fields: (String, String, String, String, String, String, String, String, String, String, String, String) => {
                val (key, uname, fbid, lnid, educ, worked, city, edegree, eyear, worktitle, twid, twname) = fields
                val key2 = Option(key).getOrElse("")
                val uname2 = Option(uname).getOrElse(twname)
                val fbid2 = Option(fbid).getOrElse("")
                val lnid2 = Option(lnid).getOrElse("")
                val educ2 = Option(educ).getOrElse("")
                val worked2 = Option(worked).getOrElse("")
                val city2 = Option(city).getOrElse("")
                val edegree2 = Option(edegree).getOrElse("")
                val eyear2 = Option(eyear).getOrElse("")
                val worktitle2 = Option(worktitle).getOrElse("")
                val twid2 = Option(twid).getOrElse("")
                (key2, uname2, fbid2, lnid2, educ2, worked2, city2, edegree2, eyear2, worktitle2, twid2)
            }
        }



        total
    }

    def getWrkDescProfileTuples(datahandle: RichPipe): RichPipe = {
        val numProfiles = datahandle.groupBy('id) {
            _.size
        }.rename('size -> 'numProfiles)

        val profiles = datahandle.joinWithSmaller('id -> 'id, numProfiles)

        val dupProfiles = profiles.rename(('id, 'serviceType, 'jsondata, 'numProfiles) ->('id2, 'serviceType2, 'jsondata2, 'numProfiles2))

        val dtoProfilesWithDesc = profiles.joinWithSmaller('id -> 'id2, dupProfiles).project(('id, 'serviceType, 'jsondata, 'serviceType2, 'jsondata2))
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
                val fbServiceProfile = ScrawlerObjectMapper.parseJson(fbJson, classOf[ServiceProfileDTO])
                val lnServiceProfile = ScrawlerObjectMapper.parseJson(lnJson, classOf[ServiceProfileDTO])
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
                val fbusername = getUserName(fbJson).getOrElse(getUserName(lnJson).getOrElse(None))
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
                (rowkey, fbname, fbid.mkString, lnid.mkString, educationschool, workcomp, ccity, edudegree, eduyear, worktitle)
            //}.project('key, 'name, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)

        }.project(('key, 'name, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc))
                .groupBy('keyid) {
            group => group
                    .toList[String]('uname, 'uname).sortBy('uname)
                    .toList[String]('fbid, 'first).sortBy('fbid)
                    .toList[String]('lnid, 'second).sortBy('lnid)
                    .toList[String]('educ, 'educ).sortBy('educ)
                    .toList[String]('worked, 'worked).sortBy('worked)
                    .toList[String]('city, 'city).sortBy('city)
                    .toList[String]('edegree, 'edegree).sortBy('edegree)
                    .toList[String]('eyear, 'eyear).sortBy('eyear)
                    .toList[String]('worktitle, 'worktitle).sortBy('worktitle)
                    .toList[String]('workdesc, 'workdesc).sortBy('workdesc)

        }
                .map(('keyid, 'first, 'second) ->('key, 'fbid, 'lnid)) {
            fields: (String, List[String], List[String]) =>
                val (user, first, second) = fields
                val filterFirst = first.filter {
                    fi: String => isNumeric(fi)
                }
                var headFirst = filterFirst.headOption.getOrElse("")
                if (!filterFirst.isEmpty && !filterFirst.tail.isEmpty) {
                    headFirst = filterFirst.tail.head
                }



                val filterSecond = second.filter {
                    fi: String => !isNumeric(fi)
                }
                val headSecond = filterSecond.headOption.getOrElse("")

                (user, headFirst, headSecond)
        }
                .mapTo(('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc) ->('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)) {
            fields: (String, List[String], String, String, List[String], List[String], List[String], List[String], List[String], List[String], List[String]) =>
                val (key, uname, fbid, lnid, educ, worked, city, edegree, eyear, worktitle, workdesc) = fields
                (key, uname.head, fbid, lnid, educ.head, worked.head, city.head, edegree.head, eyear.head, worktitle.head, workdesc.head)

//               val myFunction = () => {
//
//                    Option("")
//               }
        }


        dtoProfilesWithDesc

    }

    def getFirstNonNull(input: List[String]): String = {
        val filtered = input.filter {
            st: String => !st.equals("") && !st.isNone && !st.equals("null")
        }
        filtered.headOption.getOrElse("")
    }

    def isNumeric(input: String): Boolean = input.forall(_.isDigit)

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


@JsonIgnoreProperties
class Foo extends ServiceProfileDTO {

}