package com.vision.vb;

import java.util.List;

public class EtlFeedAuditTrailVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String feedId = "";
	private String feedName = "";
	private String feedDescription = "";
	private String feedCategory = "";
	private String processDate = "";
	private String sessionId = "";
	private int currentProcessAt = 2112;
	private String currentProcess = "";
	private String auditDesc = "";
	private int auditSeq;
	private String  auditDescDetail= "";
	private String notificationFlag = "Y";
	private int etlAuditStatusNt = 2000 ;
	private int etlAuditStatus = -1 ;
	List<SmartSearchVb> smartSearchOpt = null;
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
	public String getFeedName() {
		return feedName;
	}
	public void setFeedName(String feedName) {
		this.feedName = feedName;
	}
	public String getFeedDescription() {
		return feedDescription;
	}
	public void setFeedDescription(String feedDescription) {
		this.feedDescription = feedDescription;
	}
	public String getFeedCategory() {
		return feedCategory;
	}
	public void setFeedCategory(String feedCategory) {
		this.feedCategory = feedCategory;
	}
	public String getProcessDate() {
		return processDate;
	}
	public void setProcessDate(String processDate) {
		this.processDate = processDate;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public int getCurrentProcessAt() {
		return currentProcessAt;
	}
	public void setCurrentProcessAt(int currentProcessAt) {
		this.currentProcessAt = currentProcessAt;
	}
	public String getCurrentProcess() {
		return currentProcess;
	}
	public void setCurrentProcess(String currentProcess) {
		this.currentProcess = currentProcess;
	}
	public String getAuditDesc() {
		return auditDesc;
	}
	public void setAuditDesc(String auditDesc) {
		this.auditDesc = auditDesc;
	}
	public int getAuditSeq() {
		return auditSeq;
	}
	public void setAuditSeq(int auditSeq) {
		this.auditSeq = auditSeq;
	}
	public String getAuditDescDetail() {
		return auditDescDetail;
	}
	public void setAuditDescDetail(String auditDescDetail) {
		this.auditDescDetail = auditDescDetail;
	}
	public String getNotificationFlag() {
		return notificationFlag;
	}
	public void setNotificationFlag(String notificationFlag) {
		this.notificationFlag = notificationFlag;
	}
	public int getEtlAuditStatusNt() {
		return etlAuditStatusNt;
	}
	public void setEtlAuditStatusNt(int etlAuditStatusNt) {
		this.etlAuditStatusNt = etlAuditStatusNt;
	}
	public int getEtlAuditStatus() {
		return etlAuditStatus;
	}
	public void setEtlAuditStatus(int etlAuditStatus) {
		this.etlAuditStatus = etlAuditStatus;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
	
	
	
}