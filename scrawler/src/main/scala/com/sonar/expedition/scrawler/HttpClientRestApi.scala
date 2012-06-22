import com.sonar.expedition.scrawler.HttpClientRest
import java.io.InputStreamReader
import java.net.URL
import javax.xml.crypto.Data
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.{HttpPost, HttpGet}
import org.apache.http.client.utils._
import org.jsoup.Jsoup
import org.jsoup.nodes.Document


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
