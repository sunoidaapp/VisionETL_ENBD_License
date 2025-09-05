package com.vision.vb;

import com.vision.util.ValidationUtil;

public class EtlFeedInjectionConfigVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private int injectionTypeAt = 2107;
	private String injectionType = "";
	private String benchMarkColumnId = null;
	private String benchMarkRule = null;
	private String firstTimeVarientFlag = "";
	private String firstInjectionType = "N";
	private int etlInjectionStatusNt = 2000 ;
	private int etlInjectionStatus = 0 ;
	
	public EtlFeedInjectionConfigVb() {
	}

	public EtlFeedInjectionConfigVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
	}
	
	public void setCountry(String country) {
		this.country = country; 
	}

	public String getCountry() {
		return country; 
	}
	public void setLeBook(String leBook) {
		this.leBook = leBook; 
	}

	public String getLeBook() {
		return leBook; 
	}
	public void setFeedId(String feedId) {
		this.feedId = feedId; 
	}

	public String getFeedId() {
		return feedId; 
	}
	public void setInjectionTypeAt(int injectionTypeAt) {
		this.injectionTypeAt = injectionTypeAt; 
	}

	public int getInjectionTypeAt() {
		return injectionTypeAt; 
	}
	public void setInjectionType(String injectionType) {
		this.injectionType = injectionType; 
	}

	public String getInjectionType() {
		return injectionType; 
	}
	public void setBenchMarkColumnId(String benchMarkColumnId) {
		this.benchMarkColumnId = ValidationUtil.isValid(benchMarkColumnId) ? benchMarkColumnId : null;
	}

	public String getBenchMarkColumnId() {
		return benchMarkColumnId; 
	}
	public void setBenchMarkRule(String benchMarkRule) {
		this.benchMarkRule = ValidationUtil.isValid(benchMarkRule) ? benchMarkRule : null;
	}

	public String getBenchMarkRule() {
		return benchMarkRule; 
	}
	public void setFirstTimeVarientFlag(String firstTimeVarientFlag) {
		this.firstTimeVarientFlag = firstTimeVarientFlag; 
	}

	public String getFirstTimeVarientFlag() {
		return firstTimeVarientFlag; 
	}

	public void setFirstInjectionType(String firstInjectionType) {
		this.firstInjectionType = ValidationUtil.isValid(firstInjectionType) ? firstInjectionType : null;
	}

	public String getFirstInjectionType() {
		return firstInjectionType; 
	}
	public void setEtlInjectionStatusNt(int etlInjectionStatusNt) {
		this.etlInjectionStatusNt = etlInjectionStatusNt; 
	}

	public int getEtlInjectionStatusNt() {
		return etlInjectionStatusNt; 
	}
	public void setEtlInjectionStatus(int etlInjectionStatus) {
		this.etlInjectionStatus = etlInjectionStatus; 
	}

	public int getEtlInjectionStatus() {
		return etlInjectionStatus; 
	}


}