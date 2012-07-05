import com.sonar.expedition.scrawler.apis.HttpClientRest
import com.sonar.expedition.scrawler.HttpClientRest
import org.testng.annotations.Test

class FetchFromFactualTest {

    @Test
    def testFetchByZip() {
        val httpRequest = new HttpClientRest()
        val data = httpRequest.getFactualLocsFrmZip("10016")
        println(data)
    }

}
