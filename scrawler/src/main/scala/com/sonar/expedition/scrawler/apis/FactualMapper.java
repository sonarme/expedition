package com.sonar.expedition.scrawler.apis;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FactualMapper {

    String version = "";
    String status = "";
    @JsonProperty("response")
    FactualResponse response = new FactualResponse();

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @JsonProperty("response")
    public FactualResponse getFactualresp() {
        return response;

    }

    @JsonProperty("response")
    public void setFactualresp(FactualResponse response) {
        this.response = response;
    }

    @Override
    public String toString() {
        return "";
    }

}
