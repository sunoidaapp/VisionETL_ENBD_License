package com.vision.vb;

import java.util.List;

public class EtlFeedCategoryAccessVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String categoryId = "";
	private int userGroupAt = 1 ;
	private String userGroup = "";
	private int userProfileAt = 2 ;
	private String userProfile = "";
	private int visionId ;
	private String writeFlag = "";
	private int feedSourceStatusNt = 1 ;
	private int feedSourceStatus = -1 ;
	List<SmartSearchVb> smartSearchOpt = null;

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
	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId; 
	}

	public String getCategoryId() {
		return categoryId; 
	}
	public void setUserGroupAt(int userGroupAt) {
		this.userGroupAt = userGroupAt; 
	}

	public int getUserGroupAt() {
		return userGroupAt; 
	}
	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup; 
	}

	public String getUserGroup() {
		return userGroup; 
	}
	public void setUserProfileAt(int userProfileAt) {
		this.userProfileAt = userProfileAt; 
	}

	public int getUserProfileAt() {
		return userProfileAt; 
	}
	public void setUserProfile(String userProfile) {
		this.userProfile = userProfile; 
	}

	public String getUserProfile() {
		return userProfile; 
	}
	public void setVisionId(int visionId) {
		this.visionId = visionId; 
	}

	public int getVisionId() {
		return visionId; 
	}
	public void setWriteFlag(String writeFlag) {
		this.writeFlag = writeFlag; 
	}

	public String getWriteFlag() {
		return writeFlag; 
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
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

}