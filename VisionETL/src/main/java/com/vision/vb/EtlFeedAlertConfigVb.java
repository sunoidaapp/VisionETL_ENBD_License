package com.vision.vb;

import java.util.List;

public class EtlFeedAlertConfigVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String feedId = "";
	private String eventType = "";
	private String matrixID = "";
	private int feedAlertStatusNt = 1;
	private int feedAlertStatus =-1;
	public List<SmartSearchVb> smartSearchOpt = null;

	
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getLeBook() {
		return leBook;
	}
	public void setLeBook(String leBook) {
		this.leBook = leBook;
	}
	public String getCountryDesc() {
		return countryDesc;
	}
	public void setCountryDesc(String countryDesc) {
		this.countryDesc = countryDesc;
	}
	public String getLeBookDesc() {
		return leBookDesc;
	}
	public void setLeBookDesc(String leBookDesc) {
		this.leBookDesc = leBookDesc;
	}
	
	public String getFeedId() {
		return feedId;
	}
	public void setFeedId(String feedId) {
		this.feedId = feedId;
	}
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public String getMatrixID() {
		return matrixID;
	}
	public void setMatrixID(String matrixID) {
		this.matrixID = matrixID;
	}
	
	public int getFeedAlertStatusNt() {
		return feedAlertStatusNt;
	}
	public void setFeedAlertStatusNt(int feedAlertStatusNt) {
		this.feedAlertStatusNt = feedAlertStatusNt;
	}
	public int getFeedAlertStatus() {
		return feedAlertStatus;
	}
	public void setFeedAlertStatus(int feedAlertStatus) {
		this.feedAlertStatus = feedAlertStatus;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

}