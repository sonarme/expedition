package com.sonar.expedition.scrawler.pipes

import com.sonar.dossier.dto._
import com.twitter.scalding._
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.sonar.dossier.dto.UserEducation
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.dossier.dto.UserEmployment
import cascading.pipe.joiner._
import com.sonar.expedition.scrawler.util.CommonFunctions._


case class ProfileData(key: String,
                       name: String = "",
                       var fbid: String = "",
                       var lnid: String = "",
                       var fsid: String = "",
                       var twalias: String = "",
                       educationschool: String = "",
                       workcomp: String = "",
                       ccity: String = "",
                       edudegree: String = "",
                       eduyear: String = "",
                       worktitle: String = "",
                       workdesc: String = "")

class DTOProfileInfoPipe(args: Args) extends Job(args) {

    // updated to include foursquare data
    // uncomment last line to get hashes of fb, ln, and fsids to compare prod data

    def getDTOProfileInfoInTuples(datahandle: RichPipe): RichPipe = {

        val dtoProfiles = datahandle
                .flatMap(('id, 'serviceType, 'jsondata) ->('priority, 'profile)) {
            in: (String, String, String) =>
                val (rowkey, serviceType, json) = in
                ScrawlerObjectMapper.parseJson(Some(json), classOf[ServiceProfileDTO]) map {
                    parsed =>

                        val priority = if (serviceType == "ln") 0 else 1
                        val work = parsed.work.headOption
                        val education = parsed.education.headOption
                        val profileData = new ProfileData(
                            key = rowkey,
                            name = Option(parsed.fullName).getOrElse(""),
                            workcomp = work.map(_.companyName).getOrElse(""), // TODO, don't use empty string
                            worktitle = work.map(_.title).getOrElse(""),
                            educationschool = education.map(_.schoolName).getOrElse(""),
                            eduyear = education.map(_.getYear()).getOrElse(""),
                            edudegree = education.map(_.getDegree()).getOrElse(""),
                            workdesc = work.map(_.getSummary()).getOrElse(""),
                            ccity = Option(parsed.getLocation()).getOrElse("") // TODO, don't use empty string
                        )

                        serviceType match {
                            case "ln" => profileData.lnid = Option(hashed(parsed.userId)).getOrElse("")
                            case "fb" => profileData.fbid = Option(hashed(parsed.userId)).getOrElse("")

                            profileData.twalias = getAliasID(Option(parsed), _.getTwitter())
                            case "4s" => profileData.fsid = Option(hashed(parsed.userId)).getOrElse("")

                        }

                        (priority, profileData)
                }

        }

        val combinedProfiles = dtoProfiles.project('id, 'priority, 'profile)

                .groupBy('id) {
            _.sortBy('priority)
                    .reduce('profile -> 'combinedProfile) {
                (profileAcc: ProfileData, profile: ProfileData) =>
                    val profileData = new ProfileData(
                        key = profileAcc.key,
                        name = if (profileAcc.name != "") profileAcc.name else profile.name,
                        workdesc = if (profileAcc.workdesc != "") profileAcc.workdesc else profile.workdesc,
                        workcomp = if (profileAcc.workcomp != "") profileAcc.workcomp else profile.workcomp,
                        worktitle = if (profileAcc.worktitle != "") profileAcc.worktitle else profile.worktitle,
                        educationschool = if (profileAcc.educationschool != "") profileAcc.educationschool else profile.educationschool,
                        eduyear = if (profileAcc.eduyear != "") profileAcc.eduyear else profile.eduyear,
                        edudegree = if (profileAcc.edudegree != "") profileAcc.edudegree else profile.edudegree,
                        ccity = if (profileAcc.ccity != "") profileAcc.ccity else profile.ccity

                    )
                    profileData.fbid = if (profileAcc.fbid != "") profileAcc.fbid else profile.fbid
                    profileData.fsid = if (profileAcc.fsid != "") profileAcc.fsid else profile.fsid
                    profileData.lnid = if (profileAcc.lnid != "") profileAcc.lnid else profile.lnid
                    profileData.twalias = if (profileAcc.twalias != "") profileAcc.twalias else profile.twalias

                    profileData
            }


        }.unpack[ProfileData]('combinedProfile ->('key, 'name, 'fbid, 'lnid, 'fsid, 'twalias, 'educationschool, 'workcomp, 'ccity, 'edudegree, 'eduyear, 'worktitle, 'workdesc))
                .rename(('name, 'educationschool, 'workcomp, 'ccity, 'edudegree, 'eduyear) ->('uname, 'educ, 'worked, 'city, 'edegree, 'eyear))
                .unique('key, 'uname, 'fbid, 'lnid, 'fsid, 'twalias, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)

        combinedProfiles

    }


    // twitter pipe

    def twitterProfileTuples(twitterPipe: RichPipe): RichPipe = {
        val data = twitterPipe
                .map('jsondata ->('twid, 'twServiceProfile, 'twname)) {
            twJson: String => {
                val twServiceProfile = ScrawlerObjectMapper.parseJson(Option(twJson), classOf[ServiceProfileDTO])
                val twid = hashed(getID(twServiceProfile).getOrElse(twJson))
                val twname = getUserName(twServiceProfile).getOrElse("")
                (twid, twServiceProfile, twname)
            }
        }
                .unique(('id, 'twid, 'twname))


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
                .mapTo(('key, 'uname, 'fbid, 'lnid, 'fsid, 'twalias, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'twid, 'twname, 'workdesc) ->('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)) {
            fields: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) => {
                val (key, uname, fbid, lnid, fsid, twalias, educ, worked, city, edegree, eyear, worktitle, twid, twname, workdesc) = fields
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
                val workdesc2 = Option(workdesc).getOrElse("")
                val twid2 = Option(twid).getOrElse(twalias)
                (key2, uname2, fbid2, lnid2, fsid2, twid2, educ2, worked2, city2, edegree2, eyear2, worktitle2, workdesc2)
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

    def sortEducation(list: List[UserEducation]): List[UserEducation] = {
        val filteredList = list.filter(!Option(_).isEmpty).sortBy[String](x => Option(x.getYear).getOrElse("")).reverse
        filteredList
    }

    def getWork(serviceProfile: Option[ServiceProfileDTO]): List[UserEmployment] = {
        serviceProfile.map(_.getWork().toList).getOrElse(List[UserEmployment]())
    }

    def getEducation(serviceProfile: Option[ServiceProfileDTO]): List[UserEducation] = {
        serviceProfile.map(_.getEducation().toList).getOrElse(List[UserEducation]())
    }

    def getLikes(serviceProfile: Option[ServiceProfileDTO]): List[UserLike] = {
        serviceProfile.map(_.getLike().toList).getOrElse(List[UserLike]())
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