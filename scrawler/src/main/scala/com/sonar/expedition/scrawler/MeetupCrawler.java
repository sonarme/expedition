package com.sonar.expedition.scrawler

import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.config.UserAgent;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.examples.CrawlConfig;
import bixo.examples.CrawlDbDatum;
import bixo.examples.SimpleCrawlToolOptions;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
//import cascading.flow.PlannerException;
//import cascading.scheme.SequenceFile;
//import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import com.sonar.dossier.dto.ServiceType;
import crawlercommons.sitemaps.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.kohsuke.args4j.CmdLineParser;
import org.springframework.util.FileCopyUtils;

import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.Date;
import java.util.regex.Pattern;
import java.io.*;
import java.net.*;
import org.jsoup.*;
import org.jsoup.nodes.Document;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;

import java.io.*;
/**
 * Created by IntelliJ IDEA.
 * User: paul
 * Date: 9/5/11
 * Time: 11:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class MeetupCrawler {

    private static final transient Logger LOGGER = Logger.getLogger(MeetupCrawler.class);

    static final int NUM_LOOPS = 3;
    static int CRAWLER_DURATION = 3 * 24 * 60;
    static final int MAX_SITEMAP_URLS = 500000; //TODO: FIXME!

    public static final Pattern GROUP_PAGE_PATTERN = Pattern.compile("http://www.meetup.com/[\\w\\d\\-]+/?");
    public static final Pattern MEMBER_PAGE_PATTERN = Pattern.compile("http://www.meetup.com/([\\w\\d\\-]+/)?members/[\\d]+/?]");

    // Filter URLs that fall outside of the target domain
    @SuppressWarnings("serial")
    private static class DomainUrlFilter extends BaseUrlFilter {


        private String _domain;
        private Pattern _suffixExclusionPattern;
        private Pattern _protocolInclusionPattern;

        public DomainUrlFilter(String domain) {
            _domain = domain;
            _suffixExclusionPattern = Pattern.compile("(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe)$");
            _protocolInclusionPattern = Pattern.compile("(?i)^(http|https)://");
        }

        @Override
        public boolean isRemove(UrlDatum datum) {
            String urlAsString = datum.getUrl();

            // Skip URLs with protocols we don't want to try to process
            if (!_protocolInclusionPattern.matcher(urlAsString).find()) {
                return true;
            }

            if (_suffixExclusionPattern.matcher(urlAsString).find()) {
                return true;
            }

            try {
                URL url = new URL(urlAsString);
                String host = url.getHost();
                return (!(host.contains(_domain) && url.getPath().contains("/member")));
            } catch (MalformedURLException e) {
                LOGGER.warn("Invalid URL: " + urlAsString);
                return true;
            }
        }
    }

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    // Create log output file in loop directory.
    private static void setLoopLoggerFile(String outputDirName, int loopNumber) {
        Logger rootLogger = Logger.getRootLogger();

        String filename = String.format("%s/%d-SimpleCrawlTool.log", outputDirName, loopNumber);
        FileAppender appender = (FileAppender) rootLogger.getAppender("loop-logger");
        if (appender == null) {
            appender = new FileAppender();
            appender.setName("loop-logger");
            appender.setLayout(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}:%L - %m%n"));

            // We have to do this before calling addAppender, as otherwise Log4J warns us.
            appender.setFile(filename);
            appender.activateOptions();
            rootLogger.addAppender(appender);
        } else {
            appender.setFile(filename);
            appender.activateOptions();
        }
    }

    public static void importOneDomain() throws Exception {

        try {
            //Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
            //TupleEntryCollector writer = urlSink.openForWrite(conf);
            SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();

            SiteMapParser siteMapParser = new SiteMapParser();
            URL url = new URL("http://www.meetup.com/sitemap.xml");
            URLConnection connection = url.openConnection();
            String siteMapContent = FileCopyUtils.copyToString(new InputStreamReader(connection.getInputStream()));
            String contentType = connection.getContentType();
            siteMapContent = StringUtils.stripToEmpty(siteMapContent);
            AbstractSiteMap abstractSiteMapIndex = siteMapParser.parseSiteMap(contentType, siteMapContent.getBytes(), url);
            int counter = 0;
            if (abstractSiteMapIndex.isIndex()) {
                SiteMapIndex siteMapIndex = (SiteMapIndex) abstractSiteMapIndex;
                Collection<AbstractSiteMap> siteMaps = siteMapIndex.getSitemaps();
                for (AbstractSiteMap abstractSiteMap : siteMaps) {
                    if (!abstractSiteMap.isIndex()) {
                        SiteMap siteMap = (SiteMap) abstractSiteMap;
                        URL curSiteMapUrl = siteMap.getUrl();
                        String curSiteMapUrlString = curSiteMapUrl.toExternalForm();
                        if (!curSiteMapUrlString.contains("group") || curSiteMapUrlString.endsWith("kmz")) {
                            LOGGER.debug("skipping sitemap: " + curSiteMapUrl.toExternalForm() + " as it is not a group sitemap");
                            continue;
                        }
                        URLConnection curSiteMapConnection = curSiteMapUrl.openConnection();
                        byte[] curSiteMapContent = FileCopyUtils.copyToByteArray(curSiteMapConnection.getInputStream());
                        String curContentType = curSiteMapConnection.getContentType();
                        AbstractSiteMap curSiteMap = siteMapParser.parseSiteMap("application/gzipped", curSiteMapContent, curSiteMapUrl);
                        Collection<SiteMapURL> siteMapURLs = ((SiteMap) curSiteMap).getSiteMapUrls();
                        for (SiteMapURL siteMapURL : siteMapURLs) {
                            //todo: update datum to include other sitemapurl details, such as priority and frequency
                            String currentSiteMapUrl = siteMapURL.getUrl().toExternalForm();
                            CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize(currentSiteMapUrl), 0, 0, UrlStatus.UNFETCHED, 0);

                           // writer.add(datum.getTuple());
                            counter++;
                            if (counter > MAX_SITEMAP_URLS) {
                                break;
                            }
                        }
                    }
                }
            }


           //writer.close();
        } catch (Exception e) {
            //HadoopUtils.safeRemove(crawlDbPath.getFileSystem(conf), crawlDbPath);
            throw e;
        }
    }


    public static void importOneDomain(String targetDomain, Path crawlDbPath, JobConf conf) throws Exception {

        try {
            ///Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
            //TupleEntryCollector writer = urlSink.openForWrite(conf);
            SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();

            SiteMapParser siteMapParser = new SiteMapParser();
            URL url = new URL("http://www.meetup.com/sitemap.xml");
            URLConnection connection = url.openConnection();
            String siteMapContent = FileCopyUtils.copyToString(new InputStreamReader(connection.getInputStream()));
            String contentType = connection.getContentType();
            siteMapContent = StringUtils.stripToEmpty(siteMapContent);
            AbstractSiteMap abstractSiteMapIndex = siteMapParser.parseSiteMap(contentType, siteMapContent.getBytes(), url);
            int counter = 0;
            if (abstractSiteMapIndex.isIndex()) {
                SiteMapIndex siteMapIndex = (SiteMapIndex) abstractSiteMapIndex;
                Collection<AbstractSiteMap> siteMaps = siteMapIndex.getSitemaps();
                for (AbstractSiteMap abstractSiteMap : siteMaps) {
                    if (!abstractSiteMap.isIndex()) {
                        SiteMap siteMap = (SiteMap) abstractSiteMap;
                        URL curSiteMapUrl = siteMap.getUrl();
                        String curSiteMapUrlString = curSiteMapUrl.toExternalForm();
                        if (!curSiteMapUrlString.contains("group") || curSiteMapUrlString.endsWith("kmz")) {
                            LOGGER.debug("skipping sitemap: " + curSiteMapUrl.toExternalForm() + " as it is not a group sitemap");
                            continue;
                        }
                        URLConnection curSiteMapConnection = curSiteMapUrl.openConnection();
                        byte[] curSiteMapContent = FileCopyUtils.copyToByteArray(curSiteMapConnection.getInputStream());
                        String curContentType = curSiteMapConnection.getContentType();
                        AbstractSiteMap curSiteMap = siteMapParser.parseSiteMap("application/gzipped", curSiteMapContent, curSiteMapUrl);
                        Collection<SiteMapURL> siteMapURLs = ((SiteMap) curSiteMap).getSiteMapUrls();
                        for (SiteMapURL siteMapURL : siteMapURLs) {
                            //todo: update datum to include other sitemapurl details, such as priority and frequency
                            String currentSiteMapUrl = siteMapURL.getUrl().toExternalForm();
                            CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize(currentSiteMapUrl), 0, 0, UrlStatus.UNFETCHED, 0);

              //              writer.add(datum.getTuple());
                            counter++;
                            if (counter > MAX_SITEMAP_URLS) {
                                break;
                            }
                        }
                    }
                }
            }


            //writer.close();
        } catch (Exception e) {
            //HadoopUtils.safeRemove(crawlDbPath.getFileSystem(conf), crawlDbPath);
            throw e;
        }
    }


    public static String importLinks(String urlsitemap) throws Exception {
        String urls="";
        try {
            ///Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
            //TupleEntryCollector writer = urlSink.openForWrite(conf);
            SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();

            SiteMapParser siteMapParser = new SiteMapParser();
            URL url = new URL("http://www.meetup.com/sitemap.xml");
            URLConnection connection = url.openConnection();
            String siteMapContent = FileCopyUtils.copyToString(new InputStreamReader(connection.getInputStream()));
            String contentType = connection.getContentType();
            siteMapContent = StringUtils.stripToEmpty(siteMapContent);
            AbstractSiteMap abstractSiteMapIndex = siteMapParser.parseSiteMap(contentType, siteMapContent.getBytes(), url);
            int counter = 0;
            if (abstractSiteMapIndex.isIndex()) {
                SiteMapIndex siteMapIndex = (SiteMapIndex) abstractSiteMapIndex;
                Collection<AbstractSiteMap> siteMaps = siteMapIndex.getSitemaps();
                for (AbstractSiteMap abstractSiteMap : siteMaps) {
                    if (!abstractSiteMap.isIndex()) {
                        SiteMap siteMap = (SiteMap) abstractSiteMap;
                        URL curSiteMapUrl = siteMap.getUrl();
                        String curSiteMapUrlString = curSiteMapUrl.toExternalForm();
                        if (!curSiteMapUrlString.contains("group") || curSiteMapUrlString.endsWith("kmz")) {
                            LOGGER.debug("skipping sitemap: " + curSiteMapUrl.toExternalForm() + " as it is not a group sitemap");
                            continue;
                        }
                        URLConnection curSiteMapConnection = curSiteMapUrl.openConnection();
                        byte[] curSiteMapContent = FileCopyUtils.copyToByteArray(curSiteMapConnection.getInputStream());
                        String curContentType = curSiteMapConnection.getContentType();
                        AbstractSiteMap curSiteMap = siteMapParser.parseSiteMap("application/gzipped", curSiteMapContent, curSiteMapUrl);
                        Collection<SiteMapURL> siteMapURLs = ((SiteMap) curSiteMap).getSiteMapUrls();
                        for (SiteMapURL siteMapURL : siteMapURLs) {
                            //todo: update datum to include other sitemapurl details, such as priority and frequency
                            String currentSiteMapUrl = siteMapURL.getUrl().toExternalForm();
                            urls+=currentSiteMapUrl+"\n";
                            //System.out.println(currentSiteMapUrl);
                            //CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize(currentSiteMapUrl), 0, 0, UrlStatus.UNFETCHED, 0);

                            //              writer.add(datum.getTuple());
                            counter++;
                            if (counter > MAX_SITEMAP_URLS) {
                                break;
                            }
                        }
                    }
                }
            }


            //writer.close();
        } catch (Exception e) {
            //HadoopUtils.safeRemove(crawlDbPath.getFileSystem(conf), crawlDbPath);
            throw e;
        }
        return urls;
    }

    public static boolean checkURL(String url){

        Pattern pattern = Pattern.compile(".*\\d{7,8}/");

        Matcher matcher = pattern.matcher(url);

        return matcher.matches();
    }

    public static boolean checkPageLinksURL(String url){

        try{
            if(url.indexOf("offset")!=-1){
                return true;
            }

        } catch (Exception e){

        }
        return false;

    }

    public static String getProfileInfo(String urlpass){

        return extractProfileMeetUpId(urlpass) + "," + extractProfileFBId(urlpass) +"," +
                extractProfileTWId(urlpass) + "," + extractProfileLINKEDId(urlpass)  +
                "," + extractProfileGrpName(urlpass) + "," + extractProfileLocation(urlpass);
    }

    public static String extractProfileMeetUpId(String url){

        try{
            return url.substring(url.indexOf("members/") + 8,url.length()-1);
        }   catch (Exception e){
            return "0" ;
        }

    }


    public static String extractProfileGrpName(String url){

        try{
            return url.substring(url.indexOf("com/") + 4,url.indexOf("/members/"));
        }   catch (Exception e){
            return "0" ;
        }

    }


    public static String extractProfileLocation(String urlpass){
        Document document = null;
        String results="";
        String locality="",region="",country="";
        try{
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            Elements elem = document.getElementsByClass("locality") ;
            locality=elem.text();

            try{
            elem = document.getElementsByClass("region") ;
            region=elem.text().split(" ")[0];
            try{
                //int address=document.text().indexOf("addressCountry")+16;
                //String endindex=document.text().substring(address);
                ///country = document.text().substring(address, address+ endindex.indexOf("</")) ;
                //  country =     document.text();
            }   catch (Exception e){

            }

            }catch (Exception e){

            }

            return locality + " " + region +" " +country ;

        }   catch (Exception e){
            return "0" ;
        }

    }

    public static String extractProfileFBId(String urlpass){

        Document document = null;
        String results="";
        try{
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            Elements elem = document.getElementsByClass("badge-facebook-24") ;
            String urlID=elem.attr("abs:href");
            //return urlID.substring(urlID.indexOf(".com/")+5,urlID.length());
            if(urlID.indexOf("id=")!=-1){
                return urlID.substring(urlID.indexOf("id=") + 3,urlID.length());
            }
            return urlID.substring(urlID.indexOf("www.facebook.com") + 17);
        }   catch (Exception e){
            return "0" ;
        }


    }


    public static String extractProfileTWId(String urlpass){

        Document document = null;
        String results="";
        try{
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            Elements elem = document.getElementsByClass("badge-twitter-24") ;
            String urlID=elem.attr("abs:href");
            //return urlID.substring(urlID.indexOf(".com/")+5,urlID.length()-1);
            return urlID.substring(urlID.indexOf("twitter.com")+12,urlID.length()-1);
        }   catch (Exception e){
            return "0" ;
        }


    }

    public static String extractProfileLINKEDId(String urlpass){

        Document document = null;
        String results="";
        try{
            //badge-facebook-24
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();
            Elements elem = document.getElementsByClass("badge-linkedin-24") ;
            String urlID=elem.attr("abs:href");
            //return urlID.substring(urlID.indexOf(".com/")+8,urlID.length());
            return urlID.substring(urlID.indexOf("linkedin.com")+16,urlID.length());
        }   catch (Exception e){
            return "0" ;
        }


    }

    public static String extractContentsfromPageLinks(String urlpass){


        Document document = null;
        String results="";
        try {

            System.out.println("sundi url " + urlpass);
            Thread.sleep(1000);
            document = Jsoup.connect(urlpass).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0").get();

            Elements links = document.select("a[href]");

            for (Element link : links) {

                String classname=link.className();
                if(classname.indexOf("memName")!=-1){
                    String memurl = link.attr("abs:href");
                   // Pattern pattern = Pattern.compile(".*?[members]/\\d{7,8}/");

                    //Matcher matcher = pattern.matcher(memurl);

                    //if(matcher.matches())
                        results+=memurl +"\n" ;

                }


                /*
                if(attribute.indexOf("linkedin")!=-1 ){
                            results+=link.href+"\n" ;
                }
                if(attribute.indexOf("facebook")!=-1){
                    results+=link.href+"\n" ;
                }
                if(attribute.indexOf("twitter")!=-1 )
                {
                    results+=link.href+"\n" ;
                } */

            }
            return results;

        } catch (Exception e) {

            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return "error fetching";
        }


        //String contents = MeetupContentExtractor.parseJsoup(document);

    }

    public static String extractContentsPageLinks(String url){
        String results="";
        try{
            int highestpage = findhighestepageNum(url);
            //int highestpage = 10;

            int index=url.indexOf("meetup.com")+11;
            String groupname= url.substring(index,index+url.substring(index).indexOf("/"));

            for(int i=0;i<=highestpage;i++){
                String tmpurl="http://www.meetup.com/" + groupname +"/members/?offset="+i*20 +"&desc=1&sort=chapter_member.atime";
                results+=tmpurl + "\n";
            }
        }   catch (Exception e){
            e.printStackTrace();
            return "error fetching";
        }
        return results;

    }

    public static int findhighestepageNum(String urlpass){
        Document document = null;
        String results="";
        int highestPage=1;
        int tmppage=1;
        String contents="";
        try {

            /*
            int index=urlpass.indexOf("meetup.com")+11;
            String groupname= urlpass.substring(index,index+urlpass.substring(index).indexOf("/"));
            document = Jsoup.connect("http://www.meetup.com/"+groupname+"/members/").get();

            //Elements content = document.select("img[src$=http://img2.meetupstatic.com/94038890976300987/img/sprites/1x1.gif]");

            //System.out.println("sundi debug" +document.text().indexOf("http://img2.meetupstatic.com/94038890976300987/img/sprites/1x1.gif"));
            contents=document.getElementById("meetupBody").text();
            Elements image = document.select("a[href]");

            //Elements image = document.getAllElements();
            //getElementsByAttributeValue("src","http://img2.meetupstatic.com/94038890976300987/img/sprites/1x1.gif");
            System.out.println("sundi test image url1 " + image.size());
            for (Element link : image) {
                String test =  link.attr("abs:href") + link.text();
                System.out.println("sundi test image url2 " + test);

            }


            Elements links = document.select("a[href^=http:]");

            for (Element link : links) {

                //System.out.println(link.text());
                String memurl = link.attr("abs:href");
                //System.out.println(memurl);
                if(memurl.indexOf("?offset=")!=-1){

                    int tmpindex=memurl.indexOf("?offset=")+8;
                    //tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +memurl.substring(tmpindex).indexOf("&amp;")));
                    //tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +memurl.substring(tmpindex).indexOf("&amp;")-1));
                    int innertmpind = memurl.substring(tmpindex).indexOf("&");
                    //System.out.println(memurl.substring(tmpindex) + "," + innertmpind +"," + tmpindex);
                    tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +innertmpind));

                     if(tmppage>highestPage)
                            highestPage=tmppage;

                }



            } */

            String respcontents="";
            String memurl="";
            try {



                respcontents = doHttpUrlConnectionAction(urlpass);

                    memurl = respcontents.split("relative_page")[2];
                    //System.out.println(memurl);
                    if(memurl.indexOf("?offset=")!=-1){

                        int tmpindex=memurl.indexOf("?offset=")+8;
                        //tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +memurl.substring(tmpindex).indexOf("&amp;")));
                        //tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +memurl.substring(tmpindex).indexOf("&amp;")-1));
                        int innertmpind = memurl.substring(tmpindex).indexOf("&");
                        //System.out.println(memurl.substring(tmpindex) + "," + innertmpind +"," + tmpindex);
                        tmppage = Integer.parseInt(memurl.substring(tmpindex,tmpindex +innertmpind));

                        if(tmppage>highestPage)
                            highestPage=tmppage;

                    }






            } catch (Exception e) {
            }
            System.out.println("highest page number : " + highestPage);
            return highestPage/20;

        } catch (Exception e) {

            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return highestPage;
        }



    }

    /*
    * Returns the output from the given URL.
    *
            * I tried to hide some of the ugliness of the exception-handling
    * in this method, and just return a high level Exception from here.
            * Modify this behavior as desired.
    *
            * @param desiredUrl
    * @return
            * @throws Exception
    */
    private static String doHttpUrlConnectionAction(String desiredUrl)
            throws Exception
    {
        URL url = null;
        BufferedReader reader = null;
        StringBuilder stringBuilder;

        try
        {
            // create the HttpURLConnection
            url = new URL(desiredUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // just want to do an HTTP GET here
            connection.setRequestMethod("POST");

            // uncomment this if you want to write output to this url
            //connection.setDoOutput(true);

            // give it 15 seconds to respond
            connection.setReadTimeout(15*1000);

            connection.setRequestProperty("USER-AGENT","Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)" );

            connection.setRequestProperty("Content-type","application/x-www-form-urlenCcoded");
            connection.setAllowUserInteraction( true );
            connection.connect();

            // read the output from the server
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            stringBuilder = new StringBuilder();

            String line = null;
            while ((line = reader.readLine()) != null)
            {
                stringBuilder.append(line + "\n");
            }
            return stringBuilder.toString();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            // close the reader; this can throw an exception too, so
            // wrap it in another try/catch block.
            if (reader != null)
            {
                try
                {
                    reader.close();
                }
                catch (IOException ioe)
                {
                    ioe.printStackTrace();
                }
            }
        }
    }


    public static String extractContents(String url){


        Document document = null;
        String results="";
        try {

            int index=url.indexOf("meetup.com")+11;
            String groupname= url.substring(index,index+url.substring(index).indexOf("/"));
            document = Jsoup.connect("http://www.meetup.com/"+groupname+"/members/").get();

            Elements links = document.select("a[href]");

            for (Element link : links) {

                String memurl = link.attr("abs:href");
                if(memurl.indexOf("/members/")!=-1){
                      results+=memurl +"\n" ;
                      if(memurl.indexOf("offset=20")!=-1){
                          results+=memurl.replaceFirst("offset=20","offset=0").concat("") + "\n" ;
                      }
                }



            }
            return results;

        } catch (Exception e) {

            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return "error fetching";
        }


        //String contents = MeetupContentExtractor.parseJsoup(document);

    }

    public static String importDomainContents(String abstractSiteMap){

        URL u;
        InputStream is = null;
        DataInputStream dis;
        String s="";
        try{
            String curSiteMapUrlString =    abstractSiteMap;
            if (!curSiteMapUrlString.contains("group") || curSiteMapUrlString.endsWith("kmz")) {
                LOGGER.debug("skipping sitemap: " + " as it is not a group sitemap");
                return "";
            }

            u = new URL(abstractSiteMap);

            //----------------------------------------------//
            // Step 3:  Open an input stream from the url.  //
            //----------------------------------------------//

            is = u.openStream();         // throws an IOException

            //-------------------------------------------------------------//
            // Step 4:                                                     //
            //-------------------------------------------------------------//
            // Convert the InputStream to a buffered DataInputStream.      //
            // Buffering the stream makes the reading faster; the          //
            // readLine() method of the DataInputStream makes the reading  //
            // easier.                                                     //
            //-------------------------------------------------------------//

            dis = new DataInputStream(new BufferedInputStream(is));

            //------------------------------------------------------------//
            // Step 5:                                                    //
            //------------------------------------------------------------//
            // Now just read each record of the input stream, and print   //
            // it out.  Note that it's assumed that this problem is run   //
            // from a command-line, not from an application or applet.    //
            //------------------------------------------------------------//
            String tmp="";
            while ((tmp = dis.readLine()) != null) {
                System.out.println(s);
                s+=tmp;
            }

        }
        catch (Exception e){
            e.printStackTrace();
        }


        return s;
    }

    public static void main(String[] args) {
        /* SimpleCrawlToolOptions options = new SimpleCrawlToolOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }*/

        if (args.length > 0) {
            try {
                CRAWLER_DURATION = Integer.parseInt(args[0]) * 60;
            } catch (NumberFormatException nfe) {
                LOGGER.error("Can't parse: " + args[0], nfe);
            }
        }

        // Before we get too far along, see if the domain looks valid.
        String domain = "www.meetup.com";
        if (domain.startsWith("http")) {
            System.err.println("The target domain should be specified as just the host, without the http protocol: " + domain);
            return;
        }

        if (!domain.equals("localhost") && (domain.split("\\.").length < 2)) {
            System.err.println("The target domain should be a valid paid-level domain or subdomain of the same: " + domain);
            return;
        }

        String baseOutputPath = "s3n://sonar-resources/";

        String outputDirName = baseOutputPath + "meetupDataOutput_" + FastDateFormat.getInstance("yyyy-MM-dd-HH-mm").format(new Date());

        System.setProperty("bixo.root.level", "INFO");


        // Set console vs. DRFA vs. something else
        System.setProperty("bixo.appender", "console");


        try {
            JobConf conf = new JobConf();
            Path outputPath = new Path(outputDirName);
            FileSystem fs = outputPath.getFileSystem(conf);

            // See if the user isn't starting from scratch then set up the
            // output directory and create an initial urls subdir.
            if (!fs.exists(outputPath)) {
                fs.mkdirs(outputPath);

                // Create a "0-<timestamp>" sub-directory with just a /urls subdir
                // In the /urls dir the input file will have a single URL for the target domain.

                Path curLoopDir = CrawlDirUtils.makeLoopDir(fs, outputPath, 0);
                String curLoopDirName = curLoopDir.toUri().toString();
                setLoopLoggerFile(curLoopDirName, 0);

                Path crawlDbPath = new Path(curLoopDir, CrawlConfig.CRAWLDB_SUBDIR_NAME);
                importOneDomain(domain, crawlDbPath, conf);
            }

            Path latestDirPath = CrawlDirUtils.findLatestLoopDir(fs, outputPath);

            if (latestDirPath == null) {
                System.err.println("No previous cycle output dirs exist in " + outputDirName);
                throw new RuntimeException("No previous cycle output dirs exist in " + outputDirName);
            }

            Path crawlDbPath = new Path(latestDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);

            // Set up the start and end loop counts.
            int startLoop = CrawlDirUtils.extractLoopNumber(latestDirPath);
            int endLoop = startLoop + NUM_LOOPS;

            // Set up the UserAgent for the fetcher.
            UserAgent userAgent = new UserAgent(UserAgent.DEFAULT_BROWSER_VERSION, "Macintosh; U; Intel Mac OS X 10_6_6; en-US",
                    "AppleWebKit/534.13 (KHTML, like Gecko) Chrome/9.0.597.107 Safari/534.13");

            // You also get to customize the FetcherPolicy
            FetcherPolicy defaultPolicy = new FetcherPolicy();
            defaultPolicy.setCrawlDelay(100L); // delay between page loads
            defaultPolicy.setMaxConnectionsPerHost(50);
            defaultPolicy.setMaxRequestsPerConnection(100);
            defaultPolicy.setMaxContentSize(1024 * 1024);
            defaultPolicy.setFetcherMode(FetcherMode.COMPLETE);

            // It is a good idea to set up a crawl duration when running long crawls as you may
            // end up in situations where the fetch slows down due to a 'long tail' and by
            // specifying a crawl duration you know exactly when the crawl will end.
            int crawlDurationInMinutes = CRAWLER_DURATION; // 12 hours
            boolean hasEndTime = crawlDurationInMinutes != SimpleCrawlToolOptions.NO_CRAWL_DURATION;
            long targetEndTime = hasEndTime ? System.currentTimeMillis()
                    + (crawlDurationInMinutes * CrawlConfig.MILLISECONDS_PER_MINUTE) : FetcherPolicy.NO_CRAWL_END_TIME;

            // By setting up a url filter we only deal with urls that we want to
            // instead of all the urls that we extract.
            BaseUrlFilter urlFilter = new DomainUrlFilter(domain);

            // OK, now we're ready to start looping, since we've got our current settings
            for (int curLoop = startLoop + 1; curLoop <= endLoop; curLoop++) {

                // Adjust target end time, if appropriate.
                if (hasEndTime) {
                    int remainingLoops = (endLoop - curLoop) + 1;
                    long now = System.currentTimeMillis();
                    long perLoopTime = (targetEndTime - now) / remainingLoops;
                    defaultPolicy.setCrawlEndTime(now + perLoopTime);
                }

                Path curLoopDirPath = CrawlDirUtils.makeLoopDir(fs, outputPath, curLoop);
                String curLoopDirName = curLoopDirPath.toUri().toString();
                setLoopLoggerFile(curLoopDirName, curLoop);

                //Flow flow = com.sonar.dossier.crawler.DossierCrawlerWorkflow.createFlow(curLoopDirPath, crawlDbPath, defaultPolicy, userAgent, urlFilter, ServiceType.Meetup);
                //flow.complete();

                // Writing out .dot files is a good way to verify your flows.
                //flow.writeDOT(outputDirName + "/valid-flow.dot");

                // Update crawlDbPath to point to the latest crawl db
                crawlDbPath = new Path(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            }
        } catch (Exception e) {
            //e.writeDOT(outputDirName + "/failed-flow.dot");
            //System.err.println("PlannerException: " + e.getMessage());
            //e.printStackTrace(System.err);
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }


}
