package com.sonar.expedition.scrawler.meetup;

import crawlercommons.sitemaps.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;

import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: paul
 * Date: 9/5/11
 * Time: 11:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class MeetupCrawler {

    private static final transient Logger LOGGER = LoggerFactory.getLogger(MeetupCrawler.class);

    static final int MAX_SITEMAP_URLS = 500000; //TODO: FIXME!

    public static String importLinks(String urlsitemap) throws Exception {
        String urls = "";
        try {

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
            throw e;
        }
        return urls;
    }
}
