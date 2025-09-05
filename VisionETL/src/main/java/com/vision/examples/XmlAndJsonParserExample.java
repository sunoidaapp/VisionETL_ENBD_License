package com.vision.examples;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class XmlAndJsonParserExample {
	public static void main(String[] args) {
		String xmlStr = "<root><xmlsample>XML</xmlsample></root>";
		String jsonStr = "{\"jsonsample\":\"JSON\"}";
		try {
			ObjectMapper jsonMapper = new ObjectMapper();
			SamplePOJO jsonObj = jsonMapper.readValue(jsonStr, SamplePOJO.class);
			// System.out.println(jsonObj.getSample());
			
			XmlMapper xmlMapper = new XmlMapper();
			SamplePOJO xmlObj = xmlMapper.readValue(xmlStr, SamplePOJO.class);
			// System.out.println(xmlObj.getSample());
			
		} catch (Exception e) {
			// e.printStackTrace();
		}
		
	}
}

@JsonIgnoreProperties(ignoreUnknown = true)
class SamplePOJO {
	@JsonProperty("jsonsample")
	@JsonAlias("xmlsample")
	private String sample;

	public String getSample() {
		return sample;
	}

	public void setSample(String sample) {
		this.sample = sample;
	}
}
