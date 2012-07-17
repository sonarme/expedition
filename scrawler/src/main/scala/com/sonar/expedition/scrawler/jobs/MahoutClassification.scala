package com.sonar.expedition.scrawler.jobs


import com.twitter.scalding._
import com.sonar.expedition.scrawler.pipes.JobCodeReader._
import scala.Predef._
import scala.math._
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

    val termfreq2 =termfreq .write(SequenceFile("/tmp/occupationCodetsvtest2.txt"))


     /*val docfreq = pipe.flatMapTo(('cpsocctite,'matrixocctitle2)->('cpsocctite1,'matrixocctitle3)){
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
*/
     /*val bayespipe=docfreq.joinWithSmaller('term->'term,termfreq)
     .project('class,'term , 'rms,'docfreq)
     .groupBy('class){
         _
                 .toList[String]('term->'term1)
                 .toList[String]('rms->'rms1)
                 .toList[String]('docfreq->'docfreq1)


     }.mapTo(('class,'term1 , 'rms1,'docfreq1) -> ('class,'term1 , 'rms1,'docfreq1,'tfidfweights)){

             fields: (String,List[String],List[String],List[String]) =>
             val (label,term,rmsvalues,docfreq )= fields
             val normalised_tfidf = tfidf(term,rmsvalues,docfreq)
             (label,term,rmsvalues,docfreq,normalised_tfidf)
     }
     .write(TextLine("/tmp/occupationCodetsvtest3.txt"))
*/
    /*val resultpipe= search("/tmp/testjob.txt",bayespipe)
    .write(TextLine("/tmp/occupationCodetsvtest4.txt"))
*/

    def search(readpath:String, classpipe:RichPipe): RichPipe = {

        val readpipe = TextLine(readpath).read.project('line).mapTo('line->('words,'dummy)){
                fields: (String) =>
                val (words)= fields

                (words,1)

        }

        val dummylabel =  classpipe.mapTo(('class,'term1 , 'rms1,'docfreq1,'tfidfweights)->('class2,'term2 , 'rms2,'docfreq2,'tfidfweights2,'dummy)){
            fields: (String,List[String],List[String],List[String],List[String]) =>
            val (label,term,rmsvalues,docfreq,tfidfweights)= fields

            (label,term,rmsvalues,docfreq,tfidfweights,1)
        }

        val joinedpipe = dummylabel.joinWithSmaller('dummy->'dummy,readpipe).project('class2,'term2 , 'rms2,'docfreq2,'tfidfweights2,'words, 'dummy)
        .mapTo(('class2,'term2 , 'rms2,'docfreq2,'tfidfweights2,'words, 'dummy) -> ('class3,'score)){
            fields: (String,List[String],List[String],List[String],List[String],String,String) =>
            val (label,term,rmsvalues,docfreq,tfidfweights,words,dummy)= fields


            val score = sum(words,term,tfidfweights);

            (label,score)

        }

        /*val label = classpipe.mapTo(('class,'term1 , 'rms1,'docfreq1,'tfidfweights)->('class,'score)){
             fields: (String,List[String],List[String],List[String],List[String]) =>
             val (label,term,rmsvalues,docfreq,tfidfweights)= fields
             //for each term check if term exists , if exits add the tfidf score and return the summation
             val score = sum(word,term,tfidfweights);
             (label,score)
         }.project('class,'score)
         */
         .groupAll{
             _.max('score)
         }.project('class,'score)

         joinedpipe
    }

    def sum(word:String,term:List[String],tfidfweights:List[String]):Double={
        val sum=0;
        for (i <- 0 until term.size){
            if (term(i).equalsIgnoreCase(word)){
                sum.+(tfidfweights(i))
            }
        }
        sum
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

      def tfidf(term:List[String],rmsvalues:List[String],docfreq:List[String]):List[String]={
          var dm  = List[String]()
          val classsize=487 //need to calculate it in scalding by groupby
          for (i <- 0 until term.size){
              val weightedTfid =   rmsvalues(i).toDouble.*( log10(classsize / (docfreq(i).toDouble)))
              dm ::= weightedTfid.toString
          }
          //added in oppposite direction, so need to reverse to maintain the one one mapping
          dm.reverse
      }

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
        dm
    }

}

