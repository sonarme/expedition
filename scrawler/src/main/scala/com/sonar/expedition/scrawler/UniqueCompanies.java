package com.sonar.expedition.scrawler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class UniqueCompanies {

    String res="";

	public static String relevantScore(String company1, String company2) {

		String comp1,comp2;
		comp1=company1;
		comp2=company2;
		int count=0;
		JsonFactory jfactory = new JsonFactory();

		try{

			String APPID ="73FBA04BA3B5DA1A28909E7CE8916F8034376378";
			String tweet = comp1;
			tweet=replacespace(tweet);

			String url1="http://api.bing.net/json.aspx?AppId="+APPID  +"&Version=2.2&Market=en-US&Query=\""+tweet+"\"&Sources=web+spell&Web.Count=10";
			//String url = "http://api.bing.net/json.aspx?AppId=73FBA04BA3B5DA1A28909E7CE8916F8034376378&Version=2.2&Market=en-US&Query="+"jyotirmoy%20sundi"+"&Sources=web+spell&Web.Count=20";
			URL yahoo = new URL(url1);

			count++;
	        URLConnection yc = yahoo.openConnection();
	        //yc.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 1.2.30703)");
			count++;
	        BufferedReader in = new BufferedReader(
	                                new InputStreamReader(
	                                yc.getInputStream()));
	        String inputLine="";
	        String response="";
	        while ((inputLine = in.readLine()) != null){
	        	//count++;
	        	response+=inputLine;
	        }
	        in.close();
	        tweet = comp2;
			tweet=replacespace(tweet);

			url1="http://api.bing.net/json.aspx?AppId="+APPID  +"&Version=2.2&Market=en-US&Query=\""+tweet+"\"&Sources=web+spell&Web.Count=10";
			//String url = "http://api.bing.net/json.aspx?AppId=73FBA04BA3B5DA1A28909E7CE8916F8034376378&Version=2.2&Market=en-US&Query="+"jyotirmoy%20sundi"+"&Sources=web+spell&Web.Count=20";
			yahoo = new URL(url1);

			yc = yahoo.openConnection();
	        //yc.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 1.2.30703)");
			in = new BufferedReader(
	                                new InputStreamReader(
	                                yc.getInputStream()));
	        inputLine="";
	        String response2="";
	        while ((inputLine = in.readLine()) != null){
	        	//count++;
	        	response2+=inputLine;
	        }
	        in.close();

	        ArrayList<String> list1= new ArrayList<String>();
	        ArrayList<String> list2= new ArrayList<String>();

	        JsonParser jParser = jfactory.createJsonParser(response);
	        while (jParser.nextToken() != null) {

	    		String fieldname = jParser.getCurrentName();
	    		String url="";
	    		String desc="";
	    		if ("Description".equals(fieldname)) {

		    		  // current token is "name",
		                      // move to next, which is "name"'s value
		    		  jParser.nextToken();
		    		 // System.out.println(jParser.getText()); // display mkyong
		    		  desc=jParser.getText();

		    		  jParser.nextToken();
		    		  fieldname=jParser.getText();
		    	}

	    		if ("Url".equals(fieldname)) {

	    		  // current token is "name",
	                      // move to next, which is "name"'s value
	    		  jParser.nextToken();
	    		  //System.out.println(jParser.getText()); // display mkyong
	    		  url=jParser.getText();

	    		}



	    		if(!url.trim().equalsIgnoreCase("")
	    				&& !desc.trim().equalsIgnoreCase("")){
	    			list1.add(url+","+desc);
	    			//System.out.println("res1" +url+","+desc); // display mkyong
	    		}


	    	  }
	    	  jParser.close();


	    	  //System.out.println("     ---  ");

		         jParser = jfactory.createJsonParser(response2);
		        while (jParser.nextToken() != null) {

		    		String fieldname = jParser.getCurrentName();
		    		String url="";
		    		String desc="";
		    		if ("Description".equals(fieldname)) {

			    		  // current token is "name",
			                      // move to next, which is "name"'s value
			    		  jParser.nextToken();
			    		 // System.out.println(jParser.getText()); // display mkyong
			    		  desc=jParser.getText();

			    		  jParser.nextToken();
			    		  fieldname=jParser.getText();
			    	}

		    		if ("Url".equals(fieldname)) {

		    		  // current token is "name",
		                      // move to next, which is "name"'s value
		    		  jParser.nextToken();
		    		  //System.out.println(jParser.getText()); // display mkyong
		    		  url=jParser.getText();

		    		}



		    		if(!url.trim().equalsIgnoreCase("")
		    				&& !desc.trim().equalsIgnoreCase("")){
		    			list2.add(url+","+desc);
		    			//System.out.println("res2" +url+","+desc); // display mkyong
		    		}


		    	  }
		    	  jParser.close();


		    	  int numoflinksmatch = linksmatch(list1,list2);
	        	  double corelation_metatags = corMetatags(list1,list2);

                  return numoflinksmatch +":" + corelation_metatags;
	        	  //System.out.println("Weight of Links : " + numoflinksmatch +", correlation : " + corelation_metatags);


		}catch (Exception e) {
			// TODO: handle exception
			//System.out.println("error" );
			e.printStackTrace();
		}

        return "-1.00:-1.00" ;
		//ContactsService p = new ContactsService(applicationName, requestFactory, authTokenFactory)
	}

	private static double corMetatags(ArrayList<String> list1,
			ArrayList<String> list2) {
		// TODO Auto-generated method stub
		String res1="";
		String res2="";
		String stopwords="a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your";
		for (int i = 0; i < list1.size(); i++) {
			String[] inres = list1.get(i).toString().split(",")[1].trim().split(" ");
			for (int j = 0; j < inres.length; j++) {
				if(!stopwords.contains(inres[j]) && inres[j].length()>2)
					res1+=inres[j].toLowerCase()+" ";
			}
			
			
		}
		
		for (int i = 0; i < list2.size(); i++) {
			String[] inres = list2.get(i).toString().split(",")[1].trim().split(" ");
			for (int j = 0; j < inres.length; j++) {
				if(!stopwords.contains(inres[j])&& inres[j].length()>2)
					res2+=inres[j].toLowerCase()+" ";
			}
			
		}
//		System.out.println("res1" + res1);
//		System.out.println("res2" + res2);
		double uniquewrds=uniq(res1,res2);
		double common = common(res1,res2);
		//System.out.println(uniquewrds +","+common);
		return common/uniquewrds;
		
	}

	private static int common(String resp1, String resp2) {
		// TODO Auto-generated method stub
		ArrayList<String> map1 = new ArrayList<String>();
		ArrayList<String> map2 = new ArrayList<String>();
		String[] res1 = resp1.split(" ");
		String[] res2 = resp2.split(" ");
		for (int i = 0; i <res1.length; i++) {
			if(!map1.contains(res1[i]))
				map1.add(res1[i]);
		}
		for (int i = 0; i <res2.length; i++) {
			if(!map2.contains(res2[i].trim()) && map1.contains(res2[i].trim()))
				map2.add(res2[i]);
		}
		
		return map2.size();
	}

	private static int uniq(String resp1, String resp2) {
		// TODO Auto-generated method stub

		String[] res1 = resp1.split(" ");
		String[] res2 = resp2.split(" ");
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (int i = 0; i <res1.length; i++) {
			if(!map.containsKey(res1[i]))
				map.put(res1[i], 1);
		}
		for (int i = 0; i <res2.length; i++) {
			if(!map.containsKey(res2[i]))
				map.put(res2[i], 1);
		}
		
		return map.size();
	}

	private static int linksmatch(ArrayList<String> list1,
			ArrayList<String> list2) {
		int cnt=0;
		int size1=list1.size();
		int size2=list2.size();
		String infosites1="wikipedia.org";
		String infosites2="facebook.com/pages";
		for (int i = 0; i < list1.size(); i++) {
			String urlout=list1.get(i).toString().split(",")[0].trim();
			urlout= urlout.substring(7,8+urlout.substring(8).indexOf("/"));
			if(urlout.toLowerCase().contains(infosites1)
					||
					urlout.toLowerCase().contains(infosites2))
				continue;
				
			for (int j = 0; j < list2.size(); j++) {
				String urlin=list2.get(j).toString().split(",")[0].trim();
				urlin= urlin.substring(7,8+urlin.substring(8).indexOf("/"));
				if(urlin.toLowerCase().contains(infosites1)
						||
						urlin.toLowerCase().contains(infosites2))
					continue;	
				if(urlin.equalsIgnoreCase(urlout)){
					//System.out.println("matching " + urlin +","+ urlout);
					cnt=cnt+size1-i+size2-j;
				}
					
			}
			
		}
		return cnt;
	}

	private static String replacespace(String tweet) {
		return tweet.replaceAll(" ", "%20");
	}

}
