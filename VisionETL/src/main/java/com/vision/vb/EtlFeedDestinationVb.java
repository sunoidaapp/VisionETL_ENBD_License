package com.vision.vb;

public class EtlFeedDestinationVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String destinationConnectorId = "";
	public String getStagingContext() {
		return stagingContext;
	}

	public void setStagingContext(String stagingContext) {
		this.stagingContext = stagingContext;
	}

	private String destinationContext = "";
	private int feedTransformStatusNt = 2000 ;
	private int feedTransformStatus = 0 ;
	private String stagingContext = "";

	public EtlFeedDestinationVb() {}

	public EtlFeedDestinationVb(String country, String leBook, String feedId) {
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
	public void setDestinationConnectorId(String destinationConnectorId) {
		this.destinationConnectorId = destinationConnectorId; 
	}

	public String getDestinationConnectorId() {
		return destinationConnectorId; 
	}
	public void setDestinationContext(String destinationContext) {
		this.destinationContext = destinationContext; 
	}

	public String getDestinationContext() {
		return destinationContext; 
	}
	public void setFeedTransformStatusNt(int feedTransformStatusNt) {
		this.feedTransformStatusNt = feedTransformStatusNt; 
	}

	public int getFeedTransformStatusNt() {
		return feedTransformStatusNt; 
	}
	public void setFeedTransformStatus(int feedTransformStatus) {
		this.feedTransformStatus = feedTransformStatus; 
	}

	public int getFeedTransformStatus() {
		return feedTransformStatus; 
	}


}