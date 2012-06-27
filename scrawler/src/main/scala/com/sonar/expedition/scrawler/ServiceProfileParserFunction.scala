package com.sonar.expedition.scrawler

import com.twitter.scalding.{RichPipe, TextLine, Job, Args}
import com.sonar.dossier.dto.{UserEducation, ServiceProfileDTO, UserEmployment}
import com.sonar.dossier.domain.cassandra.converters.JsonSerializer
import java.nio.ByteBuffer
import cascading.tuple.Fields
import util.matching.Regex
import com.sonar.expedition.scrawler.ServiceProfileParserFunction._

class ServiceProfileParserFunction (args: Args) extends Job(args) {

    def parseServiceProfiles(serviceProfileInput : RichPipe): RichPipe = {

        val data = (serviceProfileInput.flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
            line: String => {
                line match {
                    case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                    case _ => List.empty
                }
            }
        }).project('id, 'serviceType, 'jsondata)

        val joinedProfiles = getDTOProfileInfoInTuples(data)

        val companies = joinedProfiles.project('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle)

//  this part can filter out any profiles that do not include work info
//            .filter('worked) { name: String => !name.trim.equals("") }
//                .project('key, 'fbid, 'lnid)

        companies
    }


    def getWork(workJson: Option[String]): List[UserEmployment] = {
        val resultantJson = workJson.mkString
        val fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")))
        Option(fbdata).map(_.getWork().toList).getOrElse(List[UserEmployment]())
    }

    def getEducation(educationJson: Option[String]): List[UserEducation] = {
        val resultantJson = educationJson.mkString
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")))
        Option(fbdata).map(_.getEducation().toList).getOrElse(List[UserEducation]())

    }

    def getUserName(fbJson: Option[String]): Option[String] = {
        val resultantJson = fbJson.mkString
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")))
        var res4 = Option(fbdata)
        res4.map(_.getFullName())
    }

    def getCity(fbJson: Option[String]): Option[String] = {
        val resultantJson = fbJson.mkString
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")))
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
        val resultantJson = jsonString.mkString
        var fbdata = JsonSerializer.get[ServiceProfileDTO]().fromByteBuffer(ByteBuffer.wrap(resultantJson.getBytes("UTF-8")))
        var res4 = Option(fbdata)
        res4.map(_.getUserId())
    }

    def getcurrCity(city: List[String]): String = {
        city.headOption.mkString
    }

    def formCityList(fbcitydata: Option[String], lnkdcitydata: Option[String]): List[String] = {
        fbcitydata.toList ++ lnkdcitydata.toList
    }

    def formWorkHistoryList(fbworkdata: scala.collection.immutable.List[UserEmployment], lnkdworkdata: scala.collection.immutable.List[UserEmployment]): scala.collection.immutable.List[UserEmployment] = {
        // fbworkdata ++ lnkdworkdata
        // Option(fbworkdata ++ lnkdworkdata).toList
        fbworkdata.toList ++ lnkdworkdata.toList
    }

    def formEducationlist(fbedulist: scala.collection.immutable.List[UserEducation], lnedulist: scala.collection.immutable.List[UserEducation]): scala.collection.immutable.List[UserEducation] = {
        fbedulist ++ lnedulist
    }

    def getFirstEdu(edu: scala.collection.immutable.List[UserEducation]): String = {
        // if(edu.length>0)
        //     println(edu)
        edu.headOption.map(_.getSchoolName()).mkString
        //"job"

    }


    def getFirstEduDegree(edu: scala.collection.immutable.List[UserEducation]): String = {
        // if(edu.length>0)
        //     println(edu)
        edu.headOption.map(_.getDegree()).mkString
        //"job"

    }


    def getFirstEduDegreeYear(edu: scala.collection.immutable.List[UserEducation]): String = {
        // if(edu.length>0)
        //     println(edu)
        edu.headOption.map(_.getYear()).mkString
        //"job"

    }

    def getFirstWork(work: scala.collection.immutable.List[UserEmployment]): String = {
        //println(work)
        work.headOption.map(_.getCompanyName()).mkString
        //"work"

    }

    def getWorkSummary(work: scala.collection.immutable.List[UserEmployment]): String = {
        //println(work)
        work.headOption.map(_.getSummary()).mkString
        //"work"

    }

    def getWorkTitle(work: scala.collection.immutable.List[UserEmployment]): String = {
        //println(work)
        work.headOption.map(_.getTitle()).mkString
        //"work"

    }

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

                (rowkey, fbname.mkString, fbid.mkString, lnid.mkString, educationschool.mkString, workcomp.mkString, ccity.mkString, edudegree.mkString, eduyear.mkString, worktitle.mkString)
        }

        dtoProfiles
    }
}


object ServiceProfileParserFunction {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs) : (.*)""".r
}