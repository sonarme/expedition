package com.sonar.expedition.scrawler;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import com.factual.driver.Circle;
import com.factual.driver.Factual;
import com.factual.driver.Query;
import com.factual.driver.ReadResponse;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class HttpClientRest {

    public HttpClientRest() {
    }

    public String getresponse(String url) {
        String response = "";

        try {
            URL get = new URL(url);

            URLConnection getc = get.openConnection();

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            getc.getInputStream()));
            String inputLine = "";
            while ((inputLine = in.readLine()) != null) {
                //count++;
                response += inputLine;
            }
            in.close();
        } catch (Exception e) {

        }
        return response;
    }

    public String getLatLong(String place) {

        place = place.replaceAll(" ", "%20");
        String url = "http://maps.googleapis.com/maps/api/geocode/json?address=" + place + "&sensor=true";
        JSONObject jsonObject = new JSONObject();
        try {
            String getresp = getresponse(url);
            jsonObject = new JSONObject(getresp);
            double longitute = ((JSONArray) jsonObject.get("results")).getJSONObject(0).getJSONObject("geometry").getJSONObject("location").getDouble("lng");
            double latitude = ((JSONArray) jsonObject.get("results")).getJSONObject(0).getJSONObject("geometry").getJSONObject("location").getDouble("lat");
            return latitude + ":" + longitute;
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            return "0:0";
        }

    }


    public String getFSQWorkplaceLatLongWithKeys(String workplace, String citylocationLat, String citylocationLong) {
        workplace = workplace.replaceAll(" ", "%20");
        String url = "https://api.foursquare.com/v2/venues/search?ll=" + citylocationLat + "," + citylocationLong + "&query=" + workplace + "&client_id=NB45JIY4HBP3VY232KO12XGDAZGF4O3DKUOBRTGZ5REY50E1&client_secret=5NCZW0FWUCHCJ5VS35YDG20AYHGBC2H5Z1W2OIG13IUEDHNK&v=20120621";
        JSONObject jsonObject = new JSONObject();
        String latitude = "-1";
        String longitute = "-1";
        String postcode = "-1";
        String getresp = "";
        try {
            //System.out.println(url);
            getresp = getresponse(url);
            jsonObject = new JSONObject(getresp);
            //System.out.println("fs1" + jsonObject);
            JSONObject foursqRespList = jsonObject.getJSONObject("response");
            //JSONObject latitude = ((JSONArray)jsonObject.get("results")).getJSONObject(0).getJSONObject("geometry").getJSONObject("location").getDouble("lat");
            //System.out.println("fs2" + foursqRespList);
            for (int i = 0; i < foursqRespList.length(); i++) {
                JSONArray loc = foursqRespList.getJSONArray("venues");
                //System.out.println("fs3" + loc);
                for (int j = 0; j < loc.length(); j++) {  // **line 2**
                    JSONObject childJSONObject = loc.getJSONObject(i);
                    String chkname = childJSONObject.getString("name");
                    if (chkname.startsWith(workplace.split(",")[0].trim())) {
                        JSONObject name = childJSONObject.getJSONObject("location");
                        latitude = name.getString("lat");
                        longitute = name.getString("lng");
                        postcode = name.getString("postalCode");
                    }

                }

            }
            return latitude + ":" + longitute + ":" + postcode;
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            System.out.println(getresp);

            e.printStackTrace();


        }
        return latitude + ":" + longitute + ":" + postcode;

    }

    public String getFactualWorkplaceLatLongWithKeys(String workplace, String citylocationLat, String citylocationLong) {
        Factual factual = new Factual("xxFiqmh1H26jMWUXmmVrexYbwIIL3RE269Dk8hUx", "mFEshzJuARYclNyoBPmeOZr9wyduWGq9LTm5aQpT");
        int radius = 10000;
        ReadResponse resp = factual.fetch("places", new Query().within(new Circle(Double.parseDouble(citylocationLat), Double.parseDouble(citylocationLong), radius)).search("" + workplace + "").field("name").beginsWith("" + workplace + ""));
        String factualResp = resp.toString();
        return parseResponseToMap(factualResp, workplace);

    }

    private static String parseResponseToMap(String factualResp, String workplace) {
        String latitude = "-1";
        String longitute = "-1";
        String postcode = "-1";
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        try {
            FactualMapper o = mapper.readValue(factualResp, FactualMapper.class);

            int len = o.getFactualresp().getData().size();
            for (int i = 0; i < len; i++) {
                System.out.println(o.getFactualresp().getData().get(i).getLatitude() + ", " + o.getFactualresp().getData().get(i).getLongitude());
                String coname = o.getFactualresp().getData().get(i).getName();
                if (coname.startsWith(workplace.split(",")[0].trim())) {
                    latitude = o.getFactualresp().getData().get(i).getLatitude();
                    longitute = o.getFactualresp().getData().get(i).getLongitude();
                    postcode = o.getFactualresp().getData().get(i).getPostcode();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return latitude + ":" + longitute + ":" + postcode;

    }


    public String getFactualLocsFrmZip(String zip) {
        Factual factual = new Factual("xxFiqmh1H26jMWUXmmVrexYbwIIL3RE269Dk8hUx", "mFEshzJuARYclNyoBPmeOZr9wyduWGq9LTm5aQpT");

        ReadResponse resp = factual.fetch("places", new Query().field("postcode").equal(zip).limit(50));
        String factualResp = resp.getJson();
        System.out.println(factualResp);
        return getListOfLocations(factualResp);

    }

    private static String getListOfLocations(String factualResp) {
        String latitude = "-1";
        String longitute = "-1";
        String postcode = "-1";
        String RESULTS = "";
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        try {
            FactualMapper o = mapper.readValue(factualResp, FactualMapper.class);

            int len = o.getFactualresp().getData().size();
            for (int i = 0; i < len; i++) {

                /*String address;
                String factual_id;
                String country;
                String latitude;
                String longitude;
                String postcode;
                String name;
                String region;
                String tel;

                */
                RESULTS += o.getFactualresp().getData().get(i).getAddress() + "\t"
                        + o.getFactualresp().getData().get(i).getFactual_id() + "\t"
                        + o.getFactualresp().getData().get(i).getCountry() + "\t"
                        + o.getFactualresp().getData().get(i).getLatitude() + "\t"
                        + o.getFactualresp().getData().get(i).getLongitude() + "\t"
                        + o.getFactualresp().getData().get(i).getPostcode() + "\t"
                        + o.getFactualresp().getData().get(i).getName() + "\t"
                        + o.getFactualresp().getData().get(i).getRegion() + "\t"
                        + o.getFactualresp().getData().get(i).getTel() + "\t"
                        + "\n";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return RESULTS;

    }


}
