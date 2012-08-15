package com.sonar.expedition.scrawler.linkedin;

import crawlercommons.sitemaps.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.regex.Pattern;

public class LinkedinCrawler {


    public static void main(String[] args) {
        LinkedinCrawler sitemapParser = new LinkedinCrawler();
        try {
            LinkedinCrawler.importLinks(args[0]);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private static final transient Logger LOGGER = LoggerFactory.getLogger(LinkedinCrawler.class);
    static final int MAX_SITEMAP_URLS = 500000000; //0000; //TODO: FIXME!
//    public static final String LI_PUBLIC_PROFILE_PAGE_PATTERN = "(/in/[\\w\\d%-]+|/pub/[\\w\\d%-]+/[\\w\\d]{1,4}/[\\w\\d]{1,4}/[\\w\\d]{1,4})/?";
//    public static final String LI_PRIVATE_PROFILE_PAGE_PATTERN = "(/profile/view)/?";
//    static boolean privateExtraction = true;
//
//    public static String getPagePattern() {
//        return privateExtraction ? LI_PRIVATE_PROFILE_PAGE_PATTERN : LI_PUBLIC_PROFILE_PAGE_PATTERN;
//    }

    public static final Pattern LI_VALID_US_SITEMAP_ITEM = Pattern.compile("/sitemaps/x_p_us_\\w\\w\\w\\.xml\\.gz");
    public static final String SITEMAP_DATA_EXPORT_FILENAME = "/tmp/linkedinSitemapData2.txt";
    public static final String LI_PUBLIC_PROFILE_PAGE_PATTERN = "(/in/[\\w\\d%-]+|/pub/[\\w\\d%-]+/[\\w\\d]{1,4}/[\\w\\d]{1,4}/[\\w\\d]{1,4})/?";
    public static final String LI_PRIVATE_PROFILE_PAGE_PATTERN = "(/profile/view)/?";
    public static final String SITEMAP_URL = "http://partner.linkedin.com/sitemaps/smindex.xml.gz";

    static boolean privateExtraction = true;
    public static String getPagePattern() {
        return privateExtraction ? LI_PRIVATE_PROFILE_PAGE_PATTERN : LI_PUBLIC_PROFILE_PAGE_PATTERN;
    }

    public static String importLinks(String urlsitemap) throws Exception {
        String sitemapFileToParse = SITEMAP_DATA_EXPORT_FILENAME;

        if (urlsitemap != null && urlsitemap.length() > 0) {
            sitemapFileToParse = urlsitemap;
        }
        String urls = "";
        BufferedWriter fstream = null;

        try {
             File newFile = new File(sitemapFileToParse);
            boolean createdNewFile = newFile.createNewFile();

            fstream = new BufferedWriter(new FileWriter(sitemapFileToParse));
            SiteMapParser siteMapParser = new SiteMapParser();

//            JavaUncompress.main(new String[]{"/tmp/smindex.xml.gz"});
            String contentType = null;
            File file = new File("/tmp/smindex2.xml.gz");
            if (file.getName().endsWith(".gz")) {
                contentType = "application/gzip";
            } else if (file.getName().endsWith(".xml")) {
                contentType = "text/xml";
            } else if (file.getName().endsWith("txt")) {
                contentType = "text/plain";
            } else {
                contentType = "text/xml";
            }

           URL url = null;
            try {
                // The file need not exist. It is made into an absolute path
                // by prefixing the current working directory
                url = file.toURL();          // file:/d:/almanac1.4/java.io/filename
            } catch (MalformedURLException e) {
            }


            /*
//            URL url = new URL("http://partner.linkedin.com/sitemaps/smindex.xml.gz");
            URLConnection connection = url.openConnection();*/
//            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream("/tmp/smindex.xml.gz"));

            FileInputStream fis = new FileInputStream(file);
            byte[] siteMapContent = FileCopyUtils.copyToByteArray(fis);
//            URLConnection connection = url.openConnection();
//            String contentType = connection.getContentType();
//            siteMapContent = StringUtils.stripToEmpty(siteMapContent);
//            LOGGER.debug(siteMapContent);
            AbstractSiteMap abstractSiteMapIndex = siteMapParser.parseSiteMap(contentType, siteMapContent, url);
            int counter = 0;
            if (abstractSiteMapIndex.isIndex()) {
                SiteMapIndex siteMapIndex = (SiteMapIndex) abstractSiteMapIndex;
                Collection<AbstractSiteMap> siteMaps = siteMapIndex.getSitemaps();
                for (AbstractSiteMap abstractSiteMap : siteMaps) {
                    if (!abstractSiteMap.isIndex()) {
                        SiteMap siteMap = (SiteMap) abstractSiteMap;
                        URL curSiteMapUrl = siteMap.getUrl();
                        String curSiteMapPath = curSiteMapUrl.getPath();
//                        String curSiteMapHost = curSiteMapUrl.getHost();
//                        curSiteMapHost = curSiteMapHost.replaceFirst("www", "partner");
                        if (!LI_VALID_US_SITEMAP_ITEM.matcher(curSiteMapPath).matches()) {
                            LOGGER.debug("skipping sitemap: " + curSiteMapUrl.toExternalForm() + " as it is not a group sitemap");
                            continue;
                        }
                        URLConnection curSiteMapConnection = curSiteMapUrl.openConnection();
                        byte[] curSiteMapContent = FileCopyUtils.copyToByteArray(curSiteMapConnection.getInputStream());
                        String curContentType = curSiteMapConnection.getContentType();
                        AbstractSiteMap curSiteMap = siteMapParser.parseSiteMap(curContentType, curSiteMapContent, curSiteMapUrl);
//                        URLConnection newcurSiteMapConnection = curSiteMap.getUrl().openConnection();
//                        AbstractSiteMap newcurSiteMap = siteMapParser.parseSiteMap("application/gzipped", FileCopyUtils.copyToByteArray(newcurSiteMapConnection.getInputStream()), curSiteMap.getUrl());
                        Collection<SiteMapURL> siteMapURLs = ((SiteMap) curSiteMap).getSiteMapUrls();
                        for (SiteMapURL siteMapURL : siteMapURLs) {
                            //todo: update datum to include other sitemapurl details, such as priority and frequency
                            String currentSiteMapUrl = siteMapURL.getUrl().toExternalForm();
                            fstream.write(currentSiteMapUrl);
                            fstream.newLine();
//                            urls += currentSiteMapUrl + "\n";
                            counter++;
                            if (counter > MAX_SITEMAP_URLS) {
                                break;
                            }
                        }
                        fstream.flush();
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            try {
                if (fstream != null) {
                    fstream.flush();
                    fstream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return urls;
    }
}
