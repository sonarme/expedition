package com.sonar.expedition.scrawler.pipes

import cascading.tuple.Fields
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import com.sonar.dossier.dto._
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
import com.sonar.dossier.dto.UserEducation
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.dossier.dto.UserEmployment
import com.sonar.expedition.scrawler.util.CommonFunctions._

class DTOProfileInfoPipe(args: Args) extends Job(args) {

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
                .groupBy('id) {
            _
                    .toList[Option[String]]('fbJson -> 'fbJsonList)
                    .toList[Option[String]]('lnJson -> 'lnJsonList)
                    .toList[Option[String]]('fsJson -> 'fsJsonList)

        }
                .map(('fbJsonList, 'lnJsonList, 'fsJsonList) ->('fbJson, 'lnJson, 'fsJson)) {
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
                val fsfbid = getAliasID(fsJson, _.getFacebook)
                val fstwid = getAliasID(fsJson, _.getTwitter)
                val fbidnew = selectNonNullString(fbid, fsfbid)
                val city = fbcity.toList ++ lncity.toList ++ fscity.toList
                (id, username, fbidnew, lnid, fsid, fstwid, edu, work, city)
        }
                .mapTo(('key, 'username, 'fbid, 'lnid, 'fsid, 'twalias, 'edu, 'work, 'city) ->('key, 'uname, 'fbid, 'lnid, 'fsid, 'twalias, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)) {
            fields: (String, String, String, String, String, String, List[UserEducation], List[UserEmployment], List[String]) =>
                val (rowkey, fbname, fbid, lnid, fsid, twalias, edu, work, city) = fields
                val educationschool = getFirstElement[UserEducation](edu, _.getSchoolName)
                val edudegree = getFirstElement[UserEducation](edu, _.getDegree)
                var eduyear = getFirstElement[UserEducation](edu, _.getYear)
                val workcomp = getFirstElement[UserEmployment](work, _.getCompanyName)
                val worktitle = getFirstElement[UserEmployment](work, _.getTitle)
                val workdesc = getFirstElement[UserEmployment](work, _.getSummary)
                val ccity = getcurrCity(city)
                // (rowkey, fbname, fbid, lnid, fsid, twalias, educationschool, workcomp, ccity, edudegree, eduyear, worktitle, workdesc)
                (rowkey, fbname, md5SumString(fbid.getBytes("UTF-8")), md5SumString(lnid.getBytes("UTF-8")), md5SumString(fsid.getBytes("UTF-8")), twalias, educationschool, workcomp, ccity, edudegree, eduyear, worktitle, workdesc)
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


    def getcurrCity(city: List[String]): String = {
        Option(city.headOption.getOrElse("")).getOrElse("")
    }

    def getFirstElement[T](list: List[T], func: (T => String)): String = {
        val first = list.headOption
        if (first.isEmpty)
            ""
        else
            Option(func(first.get)).getOrElse("")
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

    def getAliasID(serviceProfile: Option[ServiceProfileDTO], func: Aliases => String): String = {
        val alias = serviceProfile.map(_.getAliases)
        if (alias.isEmpty)
            ""
        else
            Option(func(alias.get)).getOrElse("")
    }

    def getCity(serviceProfile: Option[ServiceProfileDTO]): Option[String] = {
        serviceProfile.map(_.getLocation())
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


}


@JsonIgnoreProperties
class Foo extends ServiceProfileDTO {

}