package com.sonar.expedition.scrawler.batchjobs

import com.twitter.scalding.{RichPipe, TextLine, Job, Args}

import util.matching.Regex

import com.sonar.expedition.scrawler._
import com.twitter.scalding._
import com.twitter.scalding.TextLine


class CategoriseJobTypeMain(args: Args) extends Job(args) {

    //val types = getJobTypePipes(TextLine("/tmp/dataAnalyse.txt").read.project('line))
    val output1 = TextLine("/tmp/output1.txt")
    val output2 = TextLine("/tmp/output2.txt")

    /* var lucene = new LuceneIndex();
     lucene.initialise()
    val codes = TextLine("/tmp/occupationCodetsv.txt").project('line)
            .mapTo('line->('matrixocccode,'matrixocctitle,'cpscode,'cpsocctite))//cpsocc: CPS OCCUPATION
    {
        line: String => {
            line match {
                case Occupation(matrixocccode,matrixocctitle,cpscode,cpsocctite) =>  (matrixocccode,matrixocctitle,cpscode,cpsocctite)
                case _ =>  ("None","None","None","None")
            }
        }

    }.project('matrixocccode,'matrixocctitle,'cpscode,'cpsocctite)
            .mapTo(('matrixocccode,'matrixocctitle,'cpscode,'cpsocctite) -> ('matrixocccode1,'matrixocctitle1,'cpscode1,'cpsocctite1)) {
            fields: (String,String,String, String) =>
            val (matrixocccode,matrixocctitle,cpscode,cpsocctite) = fields
            lucene.addItems(matrixocctitle,cpsocctite)
            (matrixocccode,matrixocctitle,cpscode,cpsocctite)
    }.project('matrixocccode1,'matrixocctitle1,'cpscode1,'cpsocctite1)
    .write(output1)

     lucene.closeWriter();

     val data = (TextLine("/tmp/jobs.txt").read.project('line).mapTo(('line) ->('jobtype)) {
            fields: (String) =>
            val (job) = fields
            val jobtype= lucene.search(job.mkString.trim)
            jobtype
         }
         .project('jobtype)).write(output2)

    lucene.closeObjects()*/
    /*var classifier = new Classify(new NaiveBayesMultinomialUpdateable)
    val categories = codes.project(Fields.ALL).mapTo(('matrixocccode,'matrixocctitle,'cpscode,'cpsocctite) -> ('matrixocccode1,'matrixocctitle1,'cpscode1,'cpsocctite1)) {
        fields: (String,String,String, String) =>
        val (matrixocccode,matrixocctitle,cpscode,cpsocctite) = fields
        classifier.addCategory(cpsocctite.mkString)
        (matrixocccode,matrixocctitle,cpscode,cpsocctite)
    }.project('matrixocccode1,'matrixocctitle1,'cpscode1,'cpsocctite1)

    classifier.setupAfterCategorysAdded();

    val categoriesAndData = codes.project('matrixocctitle, 'cpsocctite).mapTo(('matrixocctitle, 'cpsocctite) ->('title, 'category)) {
        fields: (String, String) =>
            val (title, category) = fields
            classifier.addData(title.mkString, category.mkString)
            (title, category)
    }.write(output2)

    val categorise = types.project('key, 'uname, 'worktitle).mapTo(('key, 'uname, 'worktitle) ->('key1, 'uname1, 'worktitle1)) {
        fields: (String, String, String) =>
            val (key, uname, worktitle) = fields
            val category = classifier.classifyMessage(worktitle.mkString)
            (key, uname, category)
    }.write(output1)
    */


    /*def getJobTypePipes(path:RichPipe): RichPipe ={
             //val chkininputData1 = TextLine(path)
             var data1 = (path.project('line).flatMap(('line) ->('id, 'serviceType, 'jsondata)) {
                 line: String => {
                     line match {
                         case ExtractLine(userProfileId, serviceType, json) => List(userProfileId, serviceType, json)
                         case _ => List.empty
                     }
                 }
             }).project('id, 'serviceType, 'jsondata)

             val dtoProfileGetPipe1 = new DTOProfileInfoPipe(args)
             val joinedProfiles1 = dtoProfileGetPipe1.getWrkDescProfileTuples(data1)
             val filteredProfiles1 = joinedProfiles1.project('key, 'uname, 'fbid, 'lnid, 'educ, 'worked, 'city, 'edegree, 'eyear, 'worktitle, 'workdesc)
             filteredProfiles1

         }
    */


}

object CategoriseJobTypeMain {
    val Profile = """([a-zA-Z\d\- ]+)\t(ln|fb|tw)\t([a-zA-Z\d\- ]+)""".r
    val Occupation: Regex = """([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)\t([a-zA-Z\d\- ]+)\t([a-zA-Z\d\- ,]+)""".r

}
