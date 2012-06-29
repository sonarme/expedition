package com.sonar.expedition.scrawler

import org.jsoup.nodes.Document
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpPost
import org.jsoup.Jsoup
import org.jsoup.select.Elements

object FoursqScrawlerUtils {

    def getFSQWorkplaceLatLongWithoutKeys(workplace:String,  citylocationLat:String,  citylocationLong:String):String = {

        val work = replacespaceURLencd(workplace) ;
        val fsqurl = "https://foursquare.com/search?tab=venueResults&q="+work+"&lat="+citylocationLat+"&lng="+ citylocationLong  +"&source=q";
        /*
        var document: Document = null
        var responseBodyString = ""
        var results: String = ""
        //todo: FIx me!!!! We should probably only have a singleton httpClient
        var httpclient = new DefaultHttpClient();
        var method = new HttpPost(fsqurl);

        // Execute the method.
        var statusCode = httpclient.execute(method);
        responseBodyString = method.getEntity.getContent.toString; //bytes
        //document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get
        document = Jsoup.parse(responseBodyString, fsqurl);
        val links: Elements = document.select("div.info")
        import scala.collection.JavaConversions._
        for (link <- links) {
            val classname: String = link.children()
            if (classname.indexOf("memName") != -1) {
                val memurl: String = link.attr("abs:href")
                results += memurl + "\n"
            }
        } */
        fsqurl
        //todo later for 4sq eury without oauth


    }

    def replacespaceURLencd(value:String) : String= { value.replaceAll(" ","%20") }


}
