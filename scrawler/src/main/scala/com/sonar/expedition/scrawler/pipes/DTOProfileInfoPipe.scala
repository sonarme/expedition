package com.sonar.expedition.scrawler.pipes

import com.sonar.dossier.dto._
import com.twitter.scalding.{SequenceFile, RichPipe, Args}
import com.sonar.expedition.scrawler.json.ScrawlerObjectMapper
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.sonar.dossier.dto.UserEducation
import com.sonar.dossier.dto.ServiceProfileDTO
import com.sonar.dossier.dto.UserEmployment
import cascading.pipe.joiner._
import com.sonar.expedition.scrawler.util.CommonFunctions._
import com.sonar.dossier.ScalaGoodies._
import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.StringSerializer
import org.apache.cassandra.utils.ByteBufferUtil

case class ProfileData(key: String,
                       name: String = "",
                       var fbid: String = "",
                       var lnid: String = "",
                       var fsid: String = "",
                       var twid: String = "",
                       educationschool: String = "",
                       workcomp: String = "",
                       ccity: String = "",
                       edudegree: String = "",
                       eduyear: String = "",
                       worktitle: String = "",
                       workdesc: String = "")

trait DTOProfileInfoPipe extends ScaldingImplicits {
    val ProfileTuple = ('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
    // updated to include foursquare data
    // uncomment last line to get hashes of fb, ln, and fsids to compare prod data

    def serviceProfiles(args: Args) = SequenceFile(args("serviceProfileInput"), ('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc, 'impliedGender, 'impliedGenderProb, 'age, 'degree)).read

    def getDTOProfileInfoInTuples(datahandle: RichPipe) =
        datahandle
                .flatMapTo('jsondataBuffer ->('profileId, 'profile)) {
            jsondataBuffer: ByteBuffer =>
                if (jsondataBuffer == null || !jsondataBuffer.hasRemaining) None
                else {
                    val jsondata = ByteBufferUtil.getArray(jsondataBuffer)
                    val result = ScrawlerObjectMapper.parseJsonBytes[ServiceProfileDTO](jsondata)
                    result.map {
                        profile => (profile.profileId, profile)
                    }

                    /*map {
                                            parsed =>

                                                // TODO: fix nulls to be empty
                                                val work = parsed.work.headOption
                                                val education = parsed.education.headOption
                                                val profileData = new ProfileData(
                                                    key = "",
                                                    name = Option(parsed.fullName).getOrElse(""),
                                                    workcomp = work.map(_.companyName).getOrElse(""), // TODO, don't use empty string
                                                    worktitle = work.map(_.title).getOrElse(""),
                                                    educationschool = education.map(_.schoolName).getOrElse(""),
                                                    eduyear = education.map(_.getYear()).getOrElse(""),
                                                    edudegree = education.map(_.getDegree()).getOrElse(""),
                                                    workdesc = work.map(_.getSummary()).getOrElse(""),
                                                    ccity = Option(parsed.getLocation()).getOrElse("") // TODO, don't use empty string
                                                )
                                                profileData.twid = ??(parsed.aliases.twitter).map(hashed).getOrElse("")
                                                profileData.fbid = ??(parsed.aliases.facebook).map(hashed).getOrElse("")

                                                parsed.serviceType match {
                                                    case ServiceType.linkedin => profileData.lnid = Option(hashed(parsed.userId)).getOrElse("")
                                                    case ServiceType.facebook => profileData.fbid = Option(hashed(parsed.userId)).getOrElse("")
                                                    case ServiceType.twitter => profileData.twid = Option(hashed(parsed.userId)).getOrElse("")
                                                    case ServiceType.foursquare => profileData.fsid = Option(hashed(parsed.userId)).getOrElse("")
                                                }

                                                profileData
                                        }
                    */

                }


        }.groupBy('profileId) {
            _.reduce('profile -> 'combinedProfile) {
                (profileAgg: ServiceProfileDTO, profile: ServiceProfileDTO) =>
                    populateNonEmpty(profileAgg, profile)
            }
        }

    /*

            val combinedProfiles = dtoProfiles

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
                        profileData.twid = if (profileAcc.twid != "") profileAcc.twid else profile.twid

                        profileData
                }


            }.unpack[ProfileData]('combinedProfile ->('key, 'name, 'fbid, 'lnid, 'fsid, 'twid, 'educationschool, 'workcomp, 'ccity, 'edudegree, 'eduyear, 'worktitle, 'workdesc))
                    .rename(('name, 'educationschool, 'workcomp, 'ccity, 'edudegree, 'eduyear) ->('uname, 'educ, 'worked, 'city, 'edegree, 'eyear))
                    .unique(ProfileTuple)
    */


    def getTotalProfileTuples(args: Args) =
        SequenceFile(args("serviceProfileInput"), ('key, 'uname, 'fbid, 'lnid, 'fsid, 'twid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)).read

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
        if (alias == null || alias.isEmpty)
            ""
        else
            Option(func(alias.get)).getOrElse("")
    }

    def getCity(serviceProfile: Option[ServiceProfileDTO]): Option[String] = {
        serviceProfile.map(_.getLocation())
    }

    def getJson(serviceType: String, jsonString: String, serviceDesired: String): Option[String] = {
        Option(jsonString).map(validJson => if (serviceType == serviceDesired) validJson else null)
    }

    def getID(serviceProfile: Option[ServiceProfileDTO]): Option[String] = {
        serviceProfile.map(_.getUserId())
    }


}
