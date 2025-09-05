package com.vision.vb;

import java.util.List;

public class EtlFeedCategoryAlertConfigVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String categoryId = "";
	private String eventType = "";
	private String eventTypeDesc = "";
	private String matrixID = "";
	private String matrixDesc = "";
	private int categoryAlertStatusNt = 1;
	private int categoryAlertStatus =-1;
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
	public String getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
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
	public int getCategoryAlertStatusNt() {
		return categoryAlertStatusNt;
	}
	public void setCategoryAlertStatusNt(int categoryAlertStatusNt) {
		this.categoryAlertStatusNt = categoryAlertStatusNt;
	}
	public int getCategoryAlertStatus() {
		return categoryAlertStatus;
	}
	public void setCategoryAlertStatus(int categoryAlertStatus) {
		this.categoryAlertStatus = categoryAlertStatus;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
	public String getEventTypeDesc() {
		return eventTypeDesc;
	}
	public void setEventTypeDesc(String eventTypeDesc) {
		this.eventTypeDesc = eventTypeDesc;
	}
	public String getMatrixDesc() {
		return matrixDesc;
	}
	public void setMatrixDesc(String matrixDesc) {
		this.matrixDesc = matrixDesc;
	}

}