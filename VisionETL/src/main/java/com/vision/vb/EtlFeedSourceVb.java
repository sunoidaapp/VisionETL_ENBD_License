package com.vision.vb;

public class EtlFeedSourceVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String sourceConnectorId = "";
	private int feedSourceStatusNt = 2000 ;
	private int feedSourceStatus = 0 ;
	
	public EtlFeedSourceVb() {}
	
	
	public EtlFeedSourceVb(String country, String leBook, String feedId) {
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
	public void setSourceConnectorId(String sourceConnectorId) {
		this.sourceConnectorId = sourceConnectorId; 
	}

	public String getSourceConnectorId() {
		return sourceConnectorId; 
	}
	public void setFeedSourceStatusNt(int feedSourceStatusNt) {
		this.feedSourceStatusNt = feedSourceStatusNt; 
	}

	public int getFeedSourceStatusNt() {
		return feedSourceStatusNt; 
	}
	public void setFeedSourceStatus(int feedSourceStatus) {
		this.feedSourceStatus = feedSourceStatus; 
	}

	public int getFeedSourceStatus() {
		return feedSourceStatus; 
	}


}