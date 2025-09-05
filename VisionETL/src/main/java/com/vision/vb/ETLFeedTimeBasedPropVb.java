package com.vision.vb;

import java.io.Serializable;

public class ETLFeedTimeBasedPropVb implements Serializable {

	private String frequency = "";
	private ETLFeedTimeBasedInclutionsPropVb inclutions = null;
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

	public ETLFeedTimeBasedInclutionsPropVb getInclutions() {
		return inclutions;
	}

	public void setInclutions(ETLFeedTimeBasedInclutionsPropVb inclutions) {
		this.inclutions = inclutions;
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
