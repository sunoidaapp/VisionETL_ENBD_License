package com.vision.vb;

public class EtlFeedLoadMappingVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String tranColumnId = "";
	private String targetColumnId = "";
	private int feedMappingStatusNt = 2000 ;
	private int feedMappingStatus = 0 ;
	
	public EtlFeedLoadMappingVb() {}

	public EtlFeedLoadMappingVb(String country, String leBook, String feedId) {
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
	public void setTranColumnId(String tranColumnId) {
		this.tranColumnId = tranColumnId; 
	}

	public String getTranColumnId() {
		return tranColumnId; 
	}
	public void setTargetColumnId(String targetColumnId) {
		this.targetColumnId = targetColumnId; 
	}

	public String getTargetColumnId() {
		return targetColumnId; 
	}
	public void setFeedMappingStatusNt(int feedMappingStatusNt) {
		this.feedMappingStatusNt = feedMappingStatusNt; 
	}

	public int getFeedMappingStatusNt() {
		return feedMappingStatusNt; 
	}
	public void setFeedMappingStatus(int feedMappingStatus) {
		this.feedMappingStatus = feedMappingStatus; 
	}

	public int getFeedMappingStatus() {
		return feedMappingStatus; 
	}


}