package com.sonar.expedition.scrawler.jobs


import com.twitter.scalding.{TextLine, Job, Args}
import com.sonar.expedition.scrawler.pipes.JobCodeReader._



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
    }.mapTo(('cpsocctite1,'matrixocctitle4,'size1)->('cpsocctite2,'matrixocctitle5,'size2)){
        fields: (String,List[String],List[Int]) =>
        val (job,words,freqlist )= fields
        val freq=normaliseList(freqlist)

        (job,words,freq )
    }
    .write(TextLine("/tmp/occupationCodetsvtest2.txt"))

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
     }
     .project(('matrixocctitle4,'cpsocctite3))
     //.project(('matrixocctitle3,'cpsocctite2))
     .write(TextLine("/tmp/occupationCodetsvtest3.txt"))

      def normaliseList(freq:List[Int])={
          val dividend = math.sqrt(freq reduce {(acc,elem)=> acc+(elem*elem)})
          freq map (_ / dividend)

      }

}

