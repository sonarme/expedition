package com.sonar.expedition.scrawler.jobs

import com.sonar.expedition.scrawler.apis.APICalls
import com.sonar.expedition.scrawler.util._
import com.twitter.scalding._
import DataAnalyser._
import com.sonar.expedition.scrawler.pipes._
import scala.util.matching.Regex
import cascading.pipe.joiner._
import com.twitter.scalding.TextLine
import com.sonar.expedition.scrawler.objs.serializable.{LuceneIndex, GenderFromNameProbablity}


/*


run the code with two arguments passed to it.
input : the  file path from which the already parsed profile links are taken
output : the file to which the non visited profile links will be written to

 */


class DataAnalyser(args: Args) extends Job(args) {

    val inputData = args("serviceProfileData")
    val finp = args("friendData")
    val chkininputData = args("checkinData")
    val jobOutput = args("output")
    val placesData = args("placesData")
    val jobtraineddata = TextLine(args("occupationCodetsv"))
    val malepipe = TextLine(args("male"))
    val femalepipe = TextLine(args("female"))
    //these two are used to complete the pipe i.e sink, while forming the luucene indexes and male/female detection resp.
    val jobtrainedoutpipe = TextLine(args("occupationCodeTsvOutpipe"))
    val genderoutpipe = TextLine(args("genderdataOutpipe"))

    val data = (TextLine(inputData).read.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
        line: String => {
            line match {
                case ExtractLine(userProfileId, serviceType, json) => List((userProfileId, serviceType, json))
                case _ => List.empty
            }
        }
    }).project(('id, 'serviceType, 'jsondata))

    val dtoProfileGetPipe = new DTOProfileInfoPipe(args)
    val employerGroupedServiceProfilePipe = new DTOProfileInfoPipe(args)
    val friendInfoPipe = new FriendInfoPipe(args)
    val checkinGrouperPipe = new CheckinGrouperFunction(args)
    val checkinInfoPipe = new CheckinInfoPipe(args)
    val apiCalls = new APICalls(args)
    val metaphoner = new StemAndMetaphoneEmployer()
    val levenshteiner = new Levenshtein()
    val checker = new EmployerCheckinMatch
    val haversiner = new Haversine
    val coworkerPipe = new CoworkerFinderFunction((args))
    val friendGrouper = new FriendGrouperFunction(args)
    val dtoPlacesInfoPipe = new DTOPlacesInfoPipe(args)
    val gendperpipe = new GenderInfoReadPipe(args)
    val jobCodeReader = new  JobCodeReader(args)
    val scorer = new LocationScorer
    var genderprob: GenderFromNameProbablity = new  GenderFromNameProbablity()
    var lucene = new LuceneIndex()
    lucene.initialise






    val joinedProfiles = dtoProfileGetPipe.getDTOProfileInfoInTuples(data)

    val filteredProfiles = joinedProfiles.project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle))
            .map('worked ->('stemmedWorked, 'mtphnWorked)) {
            fields: String =>
            val (worked) = fields
            val stemmedWorked = StemAndMetaphoneEmployer.getStemmed(worked)
            val mtphnWorked = StemAndMetaphoneEmployer.getStemmedMetaphone(worked)

            (stemmedWorked, mtphnWorked)
    }.project(('key, 'uname, 'fbid, 'lnid, 'stemmedWorked, 'mtphnWorked, 'city, 'worktitle))

    val placesPipe = dtoPlacesInfoPipe.getPlacesInfo(TextLine(placesData).read)
            .project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'propertiesName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))
            .map('propertiesName ->('stemmedName, 'mtphnName)) {
        fields: String =>
            val (placeName) = fields
            val stemmedName = StemAndMetaphoneEmployer.getStemmed(placeName)
            val mtphnName = StemAndMetaphoneEmployer.getStemmedMetaphone(placeName)
            (stemmedName, mtphnName)
    }.project(('geometryType, 'geometryLatitude, 'geometryLongitude, 'type, 'id, 'propertiesProvince, 'propertiesCity, 'stemmedName, 'mtphnName, 'propertiesTags, 'propertiesCountry,
            'classifiersCategory, 'classifiersType, 'classifiersSubcategory, 'propertiesPhone, 'propertiesHref, 'propertiesAddress, 'propertiesOwner, 'propertiesPostcode))

    //find companies with uqniue coname and city
    //val unq_cmp_city = tmpcompanies.unique('mtphnWorked, 'city, 'fbid, 'lnid)
    /*
    if city is not filled up find city form chekcins and friends checkin

     */


    val malegender = gendperpipe.DataPipe(malepipe.read)
    val femalegender = gendperpipe.DataPipe(femalepipe.read)
    val mpipe=malegender.project(('name, 'freq)).mapTo(('name, 'freq)->('name1, 'freq1)){
        fields: (String, String) =>
            val (name, freq) = fields
            genderprob.addMaleItems(name.toUpperCase,freq)
            (name,freq)
    }
    val fpipe = femalegender.project(('name, 'freq)).mapTo(('name, 'freq)->('name1, 'freq1)){
        fields: (String, String) =>
            val (name, freq) = fields
            genderprob.addFemaleItems(name.toUpperCase,freq)
            (name, freq)
    }

    val gpipe=mpipe.++(fpipe).mapTo(('name1, 'freq1)->
            ('key1, 'uname1,'gender, 'fbid1, 'lnid1, 'city1, 'worktitle1, 'lat1, 'long1, 'geometryLatitude1, 'geometryLongitude1, 'worked1, 'stemmedWorked1, 'stemmedName1, 'score1, 'certainty1,'workcategory))
    {
        fields: (String, String) =>
            val (name1, freq) = fields
            ("-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1")

    }

    val jobs = jobCodeReader.readJobTypes(TextLine(args("occupationCodetsv")).read)
                .mapTo(('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite) ->('matrixocccode1, 'matrixocctitle1, 'cpscode1, 'cpsocctite1)) {
                fields: (String, String, String, String) =>
                val (matrixocccode, matrixocctitle, cpscode, cpsocctite) = fields
                lucene.addItems(cpsocctite, matrixocctitle)
                (matrixocccode, matrixocctitle, cpscode, cpsocctite)
    }

    val jpipe=jobs.mapTo(('matrixocccode1, 'matrixocctitle1, 'cpscode1, 'cpsocctite1)
            ->
            ('key1, 'uname1,'gender, 'fbid1, 'lnid1, 'city1, 'worktitle1, 'lat1, 'long1, 'geometryLatitude1, 'geometryLongitude1, 'worked1, 'stemmedWorked1, 'stemmedName1, 'score1, 'certainty1,'workcategory)){
        fields: (String, String,String, String) =>
            val (matrixocccode, matrixocctitle, cpscode, cpsocctite) = fields
            ("-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1","-1")

    }.project('key1, 'uname1,'gender, 'fbid1, 'lnid1, 'city1, 'worktitle1, 'lat1, 'long1, 'geometryLatitude1, 'geometryLongitude1, 'worked1, 'stemmedWorked1, 'stemmedName1, 'score1, 'certainty1,'workcategory)
    .++(gpipe)


    var friends = friendInfoPipe.friendsDataPipe(TextLine(finp).read)
    val friendData = TextLine(finp).read.project('line)

    val chkindata = checkinGrouperPipe.groupCheckins(TextLine(chkininputData).read)

    val profilesAndCheckins = filteredProfiles.joinWithLarger('key -> 'keyid, chkindata).project(('key, 'serType, 'serProfileID, 'serCheckinID, 'venName, 'venAddress, 'chknTime, 'ghash, 'loc))

    val employerGroupedServiceProfiles = employerGroupedServiceProfilePipe.getDTOProfileInfoInTuples(data).project(('key, 'worked))

    val serviceIds = joinedProfiles.project(('key, 'fbid, 'lnid)).rename(('key, 'fbid, 'lnid) ->('row_keyfrnd, 'fbId, 'lnId))

    val friendsForCoworker = friendGrouper.groupFriends(friendData)

    val coworkerCheckins = coworkerPipe.findCoworkerCheckinsPipe(employerGroupedServiceProfiles, friendsForCoworker, serviceIds, chkindata)

    val findcityfromchkins = checkinInfoPipe.findClusteroidofUserFromChkins(profilesAndCheckins.++(coworkerCheckins))


    val pipefilteredProfiles = filteredProfiles.joinWithSmaller('key -> 'key1, findcityfromchkins).project(('key, 'uname, 'fbid, 'lnid, 'mtphnWorked, 'city, 'worktitle, 'centroid, 'stemmedWorked))
            .map('mtphnWorked -> 'worked) {
            fields: (String) =>
            var (mtphnWorked) = fields
            if (mtphnWorked == null || mtphnWorked == "") {
                mtphnWorked = " "
            }
            mtphnWorked
    }
            .joinWithSmaller('worked -> 'mtphnName, placesPipe, joiner = new LeftJoin).project(('key, 'uname, 'fbid, 'lnid, 'worked, 'city, 'worktitle, 'centroid, 'geometryLatitude, 'geometryLongitude, 'stemmedName, 'stemmedWorked))
            .map('centroid ->('lat, 'long)) {
        fields: String =>
            val (centroid) = fields
            val latLongArray = centroid.split(":")
            val lat = latLongArray.head
            val long = latLongArray.last
            (lat, long)
    }
            .map(('stemmedWorked, 'lat, 'long, 'stemmedName, 'geometryLatitude, 'geometryLongitude) ->('work, 'workLatitude, 'workLongitude, 'place, 'placeLatitude, 'placeLongitude, 'score, 'certainty)) {
        fields: (String, String, String, String, String, String) =>
            val (work, workLatitude, workLongitude, place, placeLatitude, placeLongitude) = fields
            val score = scorer.getScore(work, workLatitude, workLongitude, place, placeLatitude, placeLongitude)
            val certainty = scorer.certaintyScore(score, work, place)
            (work, workLatitude, workLongitude, place, placeLatitude, placeLongitude, score, certainty)
    }
//            .groupBy('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'worked, 'stemmedWorked) {
//        _
//                .toList[Double]('certainty -> 'certaintyList)
//    }
//            .map(('certaintyList) -> ('certaintyScore)) {
//        fields: (List[Double]) =>
//            val (certaintyList) = fields
//            val certainty = certaintyList.max
//            certainty
//    }


            .project(('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'geometryLatitude, 'geometryLongitude, 'worked, 'stemmedWorked, 'stemmedName, 'score, 'certainty))
            .mapTo((('key, 'uname, 'fbid, 'lnid, 'city, 'worktitle, 'lat, 'long, 'geometryLatitude, 'geometryLongitude, 'worked, 'stemmedWorked, 'stemmedName, 'score, 'certainty))->(('key1, 'uname1,'gender, 'fbid1, 'lnid1, 'city1, 'worktitle1, 'lat1, 'long1, 'geometryLatitude1, 'geometryLongitude1, 'worked1, 'stemmedWorked1, 'stemmedName1, 'score1, 'certainty1,'workcategory))){
            fields: (String, String, String, String, String, String,String, String, String, String, String, String, String, String, String) =>
            val (key, uname, fbid, lnid, city, worktitle, lat, long, geometryLatitude, geometryLongitude, worked, stemmedWorked, stemmedName, score, certainty) = fields

            (key, uname,genderprob.getGender(uname), fbid, lnid, city, worktitle, lat, long, geometryLatitude, geometryLongitude, worked, stemmedWorked, stemmedName, score, certainty,lucene.search(worktitle))

            }
            .project(('key1, 'uname1,'gender, 'fbid1, 'lnid1, 'city1, 'worktitle1, 'lat1, 'long1, 'geometryLatitude1, 'geometryLongitude1, 'worked1, 'stemmedWorked1, 'stemmedName1, 'score1, 'certainty1,'workcategory))
            .++(jpipe)

    //filteredProfiles.++(gpipe).++(jpipe)
     .write(TextLine(jobOutput))

    //jpipe.write(jobtrainedoutpipe)

    //.write(TextLine(jobOutput))

    /*comments:
     just need to make sure this pipes get joined with  filteredProfiles or else cascading is smart enough to separte out independent job flows in whcih case
     it will be impossible to use  genderprob and lucene as they will initailed as part of a different job flow.
     */

    //mpipe.++(fpipe).write(jobtrainedoutpipe)
    //jobs.write(genderoutpipe)



}


object DataAnalyser {
    val ExtractLine: Regex = """([a-zA-Z\d\-]+)_(fb|ln|tw|fs) : (.*)""".r
    val DataExtractLine: Regex = """([a-zA-Z\d\-]+)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)::(.*)""".r
    val companiesregex: Regex = """(.*):(.*)""".r
    val Profile = """([a-zA-Z\d\- ]+)\t(ln|fb|tw)\t([a-zA-Z\d\- ]+)""".r


}
