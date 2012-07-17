package com.sonar.expedition.scrawler.jobs


import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.JobCodeReader._
import scala.Predef._
import scala._
import com.twitter.scalding.TextLine


class MahoutClassification(args: Args) extends Job(args) {

    val pipe = TextLine("/tmp/occupationCodetsv.txt").project('line)
        .mapTo('line ->('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite)) {
        line: String => {
            line match {
                case Occupation(matrixocccode, matrixocctitle, cpscode, cpsocctite) => (matrixocccode, matrixocctitle, cpscode, cpsocctite)
                case _ => ("None", "None", "None", "None")
            }
        }

    }.flatMapTo(('matrixocccode, 'matrixocctitle, 'cpscode, 'cpsocctite) ->  ('matrixocccode, 'matrixocctitle1, 'cpscode, 'cpsocctite)){
            fields: (String,String,String,String) =>
            val (matrixocccode, matrixocctitle, cpscode, cpsocctite) = fields

            matrixocctitle.split("[\\s|,]+").map{word => (matrixocccode, word, cpscode, cpsocctite)}


    }
    .project('matrixocccode, 'matrixocctitle1, 'cpscode, 'cpsocctite)
    .groupBy('cpsocctite){
        _
            .toList[String]('matrixocctitle1,'matrixocctitle2)

    }.project('cpsocctite,'matrixocctitle2)

    val pipew=pipe.write(TextLine("/tmp/occupationCodetsvtest1.txt"))

    val termfreq = pipe.flatMapTo(('cpsocctite,'matrixocctitle2)->('cpsocctite1,'matrixocctitle3)){
        fields: (String,List[String]) =>
        val (job,words )= fields
        words.map{word:String => (job,word)}

    }.project(('cpsocctite1,'matrixocctitle3))
    .groupBy(('cpsocctite1,'matrixocctitle3)){_.size}
    .project('cpsocctite1,'matrixocctitle3,'size)
    .groupBy('cpsocctite1){
        _
                .toList[String]('matrixocctitle3,'matrixocctitle4)
                .toList[Int]('size,'size1)
    }.mapTo(('cpsocctite1,'matrixocctitle4,'size1)->('cpsocctite2,'matrixocctitle5,'rms)){
        fields: (String,List[String],List[Int]) =>
        val (job,words,freqlist )= fields
        val freq=normaliseList(freqlist)

        (job,words,freq )
    }
    .mapTo(('cpsocctite2,'matrixocctitle5,'rms)->('cpsocctite2,'matrixocctitlerms)){
            fields: (String,List[String],List[Double]) =>
            val (job,words,freqlist )= fields
            val words_rms=combineList(words,freqlist)
            (job,words_rms)
    }.flatMapTo(('cpsocctite2,'matrixocctitlerms)->('cpsocctite2,'matrixocctitle , 'rms)){
            fields: (String,List[String]) =>
            val (job,words_rms)= fields
            words_rms.map{word:String => val wrds=word.split("::"); (job,wrds(0),wrds(1))}
    }.rename(('cpsocctite2,'matrixocctitle , 'rms)->('class,'term , 'rms))

    val termfreq2 =termfreq .write(TextLine("/tmp/occupationCodetsvtest2.txt"))

     val docfreq = pipe.flatMapTo(('cpsocctite,'matrixocctitle2)->('cpsocctite1,'matrixocctitle3)){
             fields: (String,List[String]) =>
             val (job,words )= fields
             words.map{word:String => (job,word)}

     }.project(('cpsocctite1,'matrixocctitle3))
     .unique(('cpsocctite1,'matrixocctitle3))
     .groupBy('matrixocctitle3){
         _
            .toList[String]('cpsocctite1->'cpsocctite2)
     }
     .mapTo(('matrixocctitle3,'cpsocctite2) -> ('matrixocctitle4,'cpsocctite3)){
         fields: (String,List[String]) =>
         val (words,jobcategory )= fields
         (words,jobcategory.size)
     }.project('matrixocctitle4,'cpsocctite3)
     .rename(('matrixocctitle4,'cpsocctite3)->('term,'docfreq))

     val bayespipe=docfreq.joinWithSmaller('term->'term,termfreq)
     .project('class,'term , 'rms,'docfreq)
     .groupBy('class){
         _
                 .toList[String]('term->'term1)
                 .toList[String]('rms->'rms1)
                 .toList[String]('docfreq->'docfreq1)


     }
     //.write(TextLine("/tmp/occupationCodetsvtest3.txt"))

    val readpipe=TextLine("/tmp/testjob.txt").read.project('line->'class){
        fields: (String) =>
        val (words)= fields
        val classlabel =  search(words,bayespipe);
        classlabel
    }.write(TextLine("/tmp/occupationCodetsvtest3.txt"))


    def search(word:String, classpipe:RichPipe) = {

    }
    /*.groupAll{
        _
           .toList[String]('matrixocctitle4,'matrixocctitle5)
           .toList[Int]('cpsocctite3,'cpsocctite4)


    }.mapTo(('matrixocctitle5,'cpsocctite4)->('matrixocctitle6,'cpsocctite5,'cpsocctite2,'matrixocctitle5,'rms)) {
        fields: (List[String],List[Int]) =>
        val (terms,docfreq )= fields
        termfreq.map{
            fields: (String,List[String],List[Double]) =>
            val (job,words,freqlist )= fields
            (job,words,freqlist,terms,docfreq)
        }

    }*/
     //.project(('matrixocctitle4,'cpsocctite3))
     //.project(('matrixocctitle3,'cpsocctite2))
     //.write(TextLine("/tmp/occupationCodetsvtest3.txt"))


      def normaliseList(freq:List[Int]):List[Double]={
          val dividend = math.sqrt(freq reduce {(acc,elem)=> acc+(elem*elem)})
          freq map (_ / dividend)

      }

    def combineList(words:List[String],rms:List[Double]) :List[String] = {
        var dm  = List[String]()
        //var dk = List[Map[String,Double]]()

        for (i <- 0 until words.size){
            dm = words(i) +"::"+ rms(i) :: dm
           // dk = Map(words(i) -> rms(i)) :: dk
        }
        //println(dm(0))
        dm
    }

}

