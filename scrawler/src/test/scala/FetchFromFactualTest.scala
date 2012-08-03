import com.sonar.expedition.scrawler.util.CommonFunctions._
import org.testng.annotations.Test

class FetchFromFactualTest {

    @Test
    def testFetchByZip() {
        /* val httpRequest = new HttpClientRest()
        val data = httpRequest.getFactualLocsFrmZip("10016")
        println(data)*/

        val twitter = "carlossastre"
        val fb = "100000165934297"
        val fbname = "scott.vonjouanne"

        println(hashed(twitter))

        println(hashed(fb))

        println(hashed(fbname))

        /*

0097391c251aae630597ce1761c16c5d
20a29642f9cf9a95b517a872b29dbebb
14d4302f2683a82568bf23f1707613b4
         */
    }

}
