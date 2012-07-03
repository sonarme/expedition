package com.sonar.expedition.scrawler.apis;

import java.util.ArrayList;

import com.sonar.expedition.scrawler.apis.FactualJsonData;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FactualResponse {
    @JsonProperty("data")
    ArrayList<FactualJsonData> data;
    @JsonProperty("included_rows")
    int included_rows;
    @JsonProperty("total_row_count")
    int total_row_count;


    @JsonProperty("data")
    public ArrayList<FactualJsonData> getData() {
        return data;
    }

    @JsonProperty("data")
    public void setData(ArrayList<FactualJsonData> data) {
        this.data = data;
    }

    @JsonProperty("included_rows")
    public int getIncluded_rows() {
        return included_rows;
    }

    @JsonProperty("included_rows")
    public void setIncluded_rows(int included_rows) {
        this.included_rows = included_rows;
    }


    @JsonProperty("total_row_count")
    public void setTotal_row_count(int total_row_count) {
        this.total_row_count = total_row_count;
    }

    @JsonProperty("total_row_count")
    public int getTotal_row_count() {
        return total_row_count;
    }

    @Override
    public String toString() {
        return "";
    }

}
