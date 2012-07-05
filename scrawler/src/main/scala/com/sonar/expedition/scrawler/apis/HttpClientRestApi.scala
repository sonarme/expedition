package com.sonar.expedition.scrawler.apis

import org.apache.http.client.methods.{HttpPost, HttpGet}

object HttpClientRestApi {

    def fetchresponse(urlpass: String): String = {
        /* val input = new URL("http://example.com/foo.json").openStream();
        val reader = new InputStreamReader(input, "UTF-8");
        val data = new Gson().fromJson(reader, Data.class);
        */
        //todoneed to chek this
        //todo
        val resp = new HttpClientRest()
        resp.getresponse(urlpass)

    }
}
