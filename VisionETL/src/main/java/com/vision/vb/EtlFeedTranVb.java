package com.vision.vb;

import java.util.List;

public class EtlFeedTranVb extends CommonVb{
	
	private String country = "";
	private String leBook = "";
	private String feedId = "";
	
	private List<EtlFeedTranNodeVb> transformationNodeInputList = null;

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

	public String getFeedId() {
		return feedId;
	}

	public void setFeedId(String feedId) {
		this.feedId = feedId;
	}

	public List<EtlFeedTranNodeVb> getTransformationNodeInputList() {
		return transformationNodeInputList;
	}

	public void setTransformationNodeInputList(List<EtlFeedTranNodeVb> transformationNodeInputList) {
		this.transformationNodeInputList = transformationNodeInputList;
	}
	
}