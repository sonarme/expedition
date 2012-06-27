package com.sonar.expedition.scrawler ;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
@JsonIgnoreProperties(ignoreUnknown = true)
public class FactualResponse{
	@JsonProperty("data")
	ArrayList<FactualJsonData> data;
	@JsonProperty("included_rows")
	int included_rows;

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

	
	@Override
	public String toString()
	{
	return "";
	}

}
