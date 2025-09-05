package com.vision.examples;

import java.util.Date;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class XmlParser_StartEndDateCheck {

	public static void main(String[] args) {
		Date now = new Date();
		System.out.println(now);
		String startDate = "19-Apr-2023";
		
		/*String xml = "<root><enddate>30-Apr-2023</enddate><postScriptID></postScriptID><preScriptID>AE01PRE</preScriptID>"
		+ "<startdate>19-Apr-2023</startdate><frequency>D</frequency><readinessScriptID>AE01</readinessScriptID></root>";*/

		String xml = "<root><postScriptID></postScriptID><preScriptID>AE01PRE</preScriptID>"
				+ "<frequency>D</frequency><readinessScriptID>AE01</readinessScriptID></root>";

		try {
			XmlMapper xmlMapper = new XmlMapper();
			ETLFeedTimeBasedPropVb1 timePropVb = xmlMapper.readValue(xml, ETLFeedTimeBasedPropVb1.class);
			System.out.println("StartDate Vb : " + startDate);
			System.out.println("StartDate Xml : " + timePropVb.getStartdate());
			System.out.println("Frequency : " + timePropVb.getFrequency());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

class ETLFeedTimeBasedPropVb1 {

	private String frequency = "";
	private String startdate = "";
	private String enddate = "";
	private String readinessScriptID = "";
	private String preScriptID = "";
	private String postScriptID = "";

	public String getFrequency() {
		return frequency;
	}

	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	public String getStartdate() {
		return startdate;
	}

	public void setStartdate(String startdate) {
		this.startdate = startdate;
	}

	public String getEnddate() {
		return enddate;
	}

	public void setEnddate(String enddate) {
		this.enddate = enddate;
	}

	public String getReadinessScriptID() {
		return readinessScriptID;
	}

	public void setReadinessScriptID(String readinessScriptID) {
		this.readinessScriptID = readinessScriptID;
	}

	public String getPreScriptID() {
		return preScriptID;
	}

	public void setPreScriptID(String preScriptID) {
		this.preScriptID = preScriptID;
	}

	public String getPostScriptID() {
		return postScriptID;
	}

	public void setPostScriptID(String postScriptID) {
		this.postScriptID = postScriptID;
	}

}
