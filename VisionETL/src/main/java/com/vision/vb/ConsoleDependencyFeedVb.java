package com.vision.vb;

import java.io.Serializable;
import java.util.List;

public class ConsoleDependencyFeedVb implements Serializable {
	
	/*"FeedName": "FeedC2",
    "FeedId": "C2",
    "Parent": "C1,A3,B3",
    "Status": "S",
    "duration": "",
    "categoryId": "Cat_C",
    "sessionId": "12345"*/
	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String feedName = "";
	private String sessionId = "";
	private String feedCategory = "";
	private List<ConsoleDependencyFeedVb> parentFeedList = null;
	private String parentUniqIdCollection = "";
	private String mainTableStatus = "";
	private String processControlStatus = "";
	private String level = "";
	private String dependencyFlag = "";
	
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
	public String getFeedName() {
		return feedName;
	}
	public void setFeedName(String feedName) {
		this.feedName = feedName;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getFeedCategory() {
		return feedCategory;
	}
	public void setFeedCategory(String feedCategory) {
		this.feedCategory = feedCategory;
	}
	public List<ConsoleDependencyFeedVb> getParentFeedList() {
		return parentFeedList;
	}

	public void setParentFeedList(List<ConsoleDependencyFeedVb> parentFeedList) {
		this.parentFeedList = parentFeedList;
	}
	public String getParentUniqIdCollection() {
		return parentUniqIdCollection;
	}
	public void setParentUniqIdCollection(String parentUniqIdCollection) {
		this.parentUniqIdCollection = parentUniqIdCollection;
	}
	public String getMainTableStatus() {
		return mainTableStatus;
	}
	public void setMainTableStatus(String mainTableStatus) {
		this.mainTableStatus = mainTableStatus;
	}
	public String getProcessControlStatus() {
		return processControlStatus;
	}
	public void setProcessControlStatus(String processControlStatus) {
		this.processControlStatus = processControlStatus;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}
	public String getDependencyFlag() {
		return dependencyFlag;
	}
	public void setDependencyFlag(String dependencyFlag) {
		this.dependencyFlag = dependencyFlag;
	}
}
