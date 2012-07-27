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
import cascading.pipe.joiner._

class DTOProfileInfoPipe(args: Args) extends Job(args) {


    // don't use anymore, new one also has foursquare

    /* def getDTOProfileInfoInTuplesOld(datahandle: RichPipe): RichPipe = {

        val dupProfiles = datahandle.rename(('id, 'serviceType, 'jsondata) ->('id2, 'serviceType2, 'jsondata2))

        val dtoProfiles = datahandle.joinWithSmaller('id -> 'id2, dupProfiles).project(('id, 'serviceType, 'jsondata, 'serviceType2, 'jsondata2))
                .mapTo(('id, 'serviceType, 'jsondata, 'serviceType2, 'jsondata2) ->('id, 'serviceType, 'fbJson, 'serviceType2, 'lnJson)) {
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

    }  */

    // updated to include foursquare data
    // uncomment last line to get hashes of fb, ln, and fsids to compare prod data

    def getDTOProfileInfoInTuples(datahandle: RichPipe): RichPipe = {

        val dtoProfiles = datahandle
                .mapTo(('id, 'serviceType, 'jsondata) ->('id, 'serviceType, 'fbJson, 'lnJson, 'fsJson)) {
            fields: (String, String, String) =>
                val (id, serviceType, jsondata) = fields
                val fbJson = getJson(serviceType, jsondata, "fb")
                val lnJson = getJson(serviceType, jsondata, "ln")
                val fsJson = getJson(serviceType, jsondata, "4s")
                (id, serviceType, fbJson, lnJson, fsJson)
        }

        val combinedProfiles = dtoProfiles
                .groupBy('id){
            _
                    .toList[Option[String]]('fbJson -> 'fbJsonList)
                    .toList[Option[String]]('lnJson -> 'lnJsonList)
                    .toList[Option[String]]('fsJson -> 'fsJsonList)

        }
        .map(('fbJsonList, 'lnJsonList, 'fsJsonList) -> ('fbJson, 'lnJson, 'fsJson)) {
            fields: (List[Option[String]], List[Option[String]], List[Option[String]]) =>
                val (fbJsonList, lnJsonList, fsJsonList) = fields
                val fbJson = getFirstNonNullOption(fbJsonList)
                val lnJson = getFirstNonNullOption(lnJsonList)
                val fsJson = getFirstNonNullOption(fsJsonList)
                (fbJson, lnJson, fsJson)
        }
        .rename('id -> 'key)
        .project(('key, 'fbJson, 'lnJson, 'fsJson))

        val output = combinedProfiles
                .map(('fbJson, 'lnJson, 'fsJson) ->('fbid, 'fbServiceProfile, 'lnid, 'lnServiceProfile, 'fsid, 'fsServiceProfile)) {
            fields: (Option[String], Option[String], Option[String]) =>
                val (fbJson, lnJson, fsJson) = fields
                val fbServiceProfile = ScrawlerObjectMapper.parseJson(fbJson, classOf[ServiceProfileDTO])
                val lnServiceProfile = ScrawlerObjectMapper.parseJson(lnJson, classOf[ServiceProfileDTO])
                val fsServiceProfile = ScrawlerObjectMapper.parseJson(fsJson, classOf[ServiceProfileDTO])
                val fbid = getID(fbServiceProfile).getOrElse(fbJson.getOrElse(""))
                val lnid = getID(lnServiceProfile).getOrElse(lnJson.getOrElse(""))
                val fsid = getID(fsServiceProfile).getOrElse(fsJson.getOrElse(""))
                (fbid, fbServiceProfile, lnid, lnServiceProfile, fsid, fsServiceProfile)
        }
                .mapTo(('key, 'fbid, 'fbServiceProfile, 'lnid, 'lnServiceProfile, 'fsid, 'fsServiceProfile) ->('key, 'username, 'fbid, 'lnid, 'fsid, 'twalias, 'edu, 'work, 'city)) {
            fields: (String, String, Option[ServiceProfileDTO], String, Option[ServiceProfileDTO], String, Option[ServiceProfileDTO]) =>
                val (id, fbid, fbJson, lnid, lnJson, fsid, fsJson) = fields
                val fbedu = getEducation(fbJson)
                val lnedu = getEducation(lnJson)
                val fsedu = getEducation(fsJson)
                val edu = fbedu.toList ++ lnedu.toList ++ fsedu.toList
                val fbwork = getWork(fbJson)
                val lnwork = getWork(lnJson)
                val fswork = getWork(fsJson)
                val work = fbwork.toList ++ lnwork.toList ++ fswork.toList
                val username = getUserName(fbJson).getOrElse(getUserName(lnJson).getOrElse(getUserName(fsJson).getOrElse("")))
                val fbcity = getCity(fbJson)
                val lncity = getCity(lnJson)
                val fscity = getCity(fsJson)
                val fsfbid = getFSFBID(fsJson)
                val fstwid = getFSTWID(fsJson)
                val fbidnew = selectNonNullString(fbid, fsfbid)
                val city = fbcity.toList ++ lncity.toList ++ fscity.toList
                (id, username, fbidnew, lnid, fsid, fstwid, edu, work, city)
        }
                .mapTo(('key, 'username, 'fbid, 'lnid, 'fsid, 'twalias, 'edu, 'work, 'city) ->('key, 'uname, 'fbid, 'lnid, 'fsid, 'twalias, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)) {
            fields: (String, String, String, String, String, String, List[UserEducation], List[UserEmployment], List[String]) =>
                val (rowkey, fbname, fbid, lnid, fsid, twalias, edu, work, city) = fields
                val educationschool = getFirstEdu(edu)
                val edudegree = getFirstEduDegree(edu)
                var eduyear = getFirstEduDegreeYear(edu)
                val workcomp = getFirstWork(work)
                val worktitle = getWorkTitle(work)
                val workdesc = getWorkSummary(work)
                val ccity = getcurrCity(city)
                //(rowkey, fbname, md5SumString(fbid.getBytes("UTF-8")), md5SumString(lnid.getBytes("UTF-8")), md5SumString(fsid.getBytes("UTF-8")), educationschool, workcomp, ccity, edudegree, eduyear, worktitle, workdesc)
                (rowkey, fbname, fbid, lnid, fsid, twalias, educationschool, workcomp, ccity, edudegree, eduyear, worktitle, workdesc)
        }

        output

    }

    // twitter pipe

    def twitterProfileTuples(twitterPipe: RichPipe): RichPipe = {
        val data = twitterPipe
                .map('jsondata ->('twid, 'twServiceProfile, 'twname)) {
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

    // joins twitter pipe and rest of profiles

    def getTotalProfileTuples(serviceProfileData: RichPipe, twServiceProfileData: RichPipe): RichPipe = {

        val fblnfs = getDTOProfileInfoInTuples(serviceProfileData)
        val tw = twitterProfileTuples(twServiceProfileData)

        val total = fblnfs.joinWithSmaller('key -> 'id, tw, new OuterJoin)
                .map(('key, 'id) -> 'mainkey) {
            fields: (String, String) => {
                val (key, id) = fields
                if (key == null)
                    id
                else
                    key
            }

        }
                .discard('key)
                .rename('mainkey -> 'key)
                .mapTo(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twalias, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'twid, 'twname) ->('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle)) {
            fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String) => {
                val (key, uname, fbid, lnid, fsid, twalias, educ, worked, city, edegree, eyear, worktitle, twid, twname) = fields
                val key2 = Option(key).getOrElse("")
                val uname2 = Option(uname).getOrElse(twname)
                val fbid2 = Option(fbid).getOrElse("")
                val lnid2 = Option(lnid).getOrElse("")
                val fsid2 = Option(fsid).getOrElse("")
                val educ2 = Option(educ).getOrElse("")
                val worked2 = Option(worked).getOrElse("")
                val city2 = Option(city).getOrElse("")
                val edegree2 = Option(edegree).getOrElse("")
                val eyear2 = Option(eyear).getOrElse("")
                val worktitle2 = Option(worktitle).getOrElse("")
                val twid2 = Option(twid).getOrElse(twalias)
                (key2, uname2, fbid2, lnid2, fsid2, twid2, educ2, worked2, city2, edegree2, eyear2, worktitle2)
            }
        }



        total
    }

    // not necessary, just use normal function

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

    def getFirstNonNullOption(input: List[Option[String]]): Option[String] = {
        val filtered = input.filter {
            opst: Option[String] => {
                val st = opst.getOrElse("")
                !st.equals("") && !st.equals("null")
            }
        }
        filtered.headOption.getOrElse(None)
    }

    def selectNonNullString(str1: String, str2: String): String = {
        if (!str1.equals(""))
            str1
        else
            str2
    }

    def isNumeric(input: String): Boolean = input.forall(_.isDigit)

    def getcurrCity(city: List[String]): String = {
        Option(city.headOption.getOrElse("")).getOrElse("")
    }

    def getFirstEdu(edu: List[UserEducation]): String = {
        val educ = edu.headOption
        if (educ.isEmpty)
            ""
        else
            Option(educ.get.getSchoolName).getOrElse("")
    }


    def getFirstEduDegree(edu: List[UserEducation]): String = {
        val educ = edu.headOption
        if (educ.isEmpty)
            ""
        else
            Option(educ.get.getDegree).getOrElse("")
    }


    def getFirstEduDegreeYear(edu: List[UserEducation]): String = {
        val educ = edu.headOption
        if (educ.isEmpty)
            ""
        else
            Option(educ.get.getYear).getOrElse("")
    }

    def getFirstWork(work: List[UserEmployment]): String = {
        val emp = work.headOption
        if (emp.isEmpty)
            ""
        else
            Option(emp.get.getCompanyName).getOrElse("")
    }

    def getWorkSummary(work: List[UserEmployment]): String = {
        val emp = work.headOption
        if (emp.isEmpty)
            ""
        else
            Option(emp.get.getSummary).getOrElse("")
    }

    def getWorkTitle(work: List[UserEmployment]): String = {
        val emp = work.headOption
        if (emp.isEmpty)
            ""
        else
            Option(emp.get.getTitle).getOrElse("")
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

    def getFSFBID(serviceProfile: Option[ServiceProfileDTO]): String = {
        val alias = serviceProfile.map(_.getAliases)
        if (alias.isEmpty)
            ""
        else
            Option(alias.get.getFacebook).getOrElse("")
    }

    def getFSTWID(serviceProfile: Option[ServiceProfileDTO]): String = {
        val alias = serviceProfile.map(_.getAliases)
        if (alias.isEmpty)
            ""
        else
            Option(alias.get.getTwitter).getOrElse("")
    }


    def getCity(serviceProfile: Option[ServiceProfileDTO]): Option[String] = {
        serviceProfile.map(_.getLocation())
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

    def getJson(serviceType: String, jsonString: String, serviceDesired: String): Option[String] = {
        if (serviceType == null)
            None
        else if (serviceType == serviceDesired)
            Option(jsonString)
        else
            None
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