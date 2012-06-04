package com.sonar.expedition.scrawler;//package com.sonar.expedition.scrawler;

import bixo.urls.SimpleUrlNormalizer;
import crawlercommons.sitemaps.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.util.FileCopyUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;


public class MeetupCrawler {

    private static final transient Logger LOGGER = Logger.getLogger(MeetupCrawler.class);

    static final int MAX_SITEMAP_URLS = 500000; //TODO: FIXME!

    public static String importLinks(String urlsitemap) throws Exception {
        String urls = "";
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
                            urls += currentSiteMapUrl + "\n";
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


    public static String doHttpUrlConnectionAction(String desiredUrl)
            throws Exception {
        URL url = null;
        BufferedReader reader = null;
        StringBuilder stringBuilder;

        try {
            // create the HttpURLConnection
            url = new URL(desiredUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // just want to do an HTTP GET here
            connection.setRequestMethod("POST");

            // give it 15 seconds to respond
            connection.setReadTimeout(15 * 1000);

            connection.setRequestProperty("USER-AGENT", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");

            connection.setRequestProperty("Content-type", "application/x-www-form-urlenCcoded");
            connection.setAllowUserInteraction(true);
            connection.connect();

            // read the output from the server
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            stringBuilder = new StringBuilder();

            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line + "\n");
            }
            return stringBuilder.toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            // close the reader; this can throw an exception too, so
            // wrap it in another try/catch block.
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
    }


}
