package com.vision.vb;

import java.util.List;

public class EtlFeedAlertEventVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String eventType = "";
	private String eventDescription = "";
	private int eventAlertStatusNt = 1;
	private int eventAlertStatus =-1;
	List<SmartSearchVb> smartSearchOpt = null;
	List<EtlFeedAlertEventVb> alertEvent =null;
	
	
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
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public String getEventDescription() {
		return eventDescription;
	}
	public void setEventDescription(String eventDescription) {
		this.eventDescription = eventDescription;
	}
	public int getEventAlertStatusNt() {
		return eventAlertStatusNt;
	}
	public void setEventAlertStatusNt(int eventAlertStatusNt) {
		this.eventAlertStatusNt = eventAlertStatusNt;
	}
	public int getEventAlertStatus() {
		return eventAlertStatus;
	}
	public void setEventAlertStatus(int eventAlertStatus) {
		this.eventAlertStatus = eventAlertStatus;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
	public List<EtlFeedAlertEventVb> getalertEvent() {
		return alertEvent;
	}
	public void setalertEvent(List<EtlFeedAlertEventVb> alertEvent) {
		this.alertEvent = alertEvent;
	}

}