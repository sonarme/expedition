package com.sonar.expedition.scrawler;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

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


    public String getFSQWorkplaceLatLong(String workplace, String citylocationLat, String citylocationLong) {
        workplace = workplace.replaceAll(" ", "%20");
        String url = "https://api.foursquare.com/v2/venues/search?ll=" + citylocationLat + "," + citylocationLong + "&query=" + workplace + "&client_id=NB45JIY4HBP3VY232KO12XGDAZGF4O3DKUOBRTGZ5REY50E1&client_secret=5NCZW0FWUCHCJ5VS35YDG20AYHGBC2H5Z1W2OIG13IUEDHNK&v=20120621";
        JSONObject jsonObject = new JSONObject();
        String latitude = "-1";
        String longitute = "-1";
        String postcode = "-1";
        try {
            //System.out.println(url);
            String getresp = getresponse(url);
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
                        //System.out.println(latitude);
                    }

                }
                //String prof = foursqRespList.getJSONObject(String.valueOf(i)).getJSONObject("location").getString("postalCode") ;
                //if(prof.contains("Professional") || prof.contains("Offices")){

                //}


            }
            //System.out.println("fs" + foursqRespList);
            return latitude + ":" + longitute + ":" + postcode;
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            return "0:0:0";
        }

    }

}
