package com.vision.vb;

import java.util.ArrayList;
import java.util.List;

public class OperationalConsoleVb extends CommonVb {

	private String feedDescription = "";
	private int currentProcessAt = 2112;
	private String auditDesc = "";
	private int auditSeq;
	private String  auditDescDetail= "";
	private String notificationFlag = "Y";
	private int etlAuditStatusNt = 2000 ;
	private int etlAuditStatus = -1 ;
	List<SmartSearchVb> smartSearchOpt = null;
	private String etlSourceConnectorId = "";
	private String destinationConnectorId = "";
	private String lastExtractionDate = "";
	private int batchSize = 0;
	private String effectiveDate = "";
	private String feedInsertionType = "A";
	private String alertType = "C";

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String[] feedIdArr = null;
	private String feedId = "";
	private String feedName = "";
	private String parentFeedId = "";
	private String ParentFeedName = "";
	private String ParentFeedCategory = "";
	private String mainTableStatus = "";
	private String mainTableStatusDesc = "";
	private String processControlStatusDesc = "";
	private String processControlStatus = "";

	private String feedType = "";
	private String feedTypeDesc = "";
	private String debugFlag = "N";
	private String grid = "1";
	private String duration = "";

	private String connectorId = "";
	private String feedCategory = "";
	private String feedCategoryDesc = "";
	private String sessionId = "";
	private String nextScheduleDate = "";
	private String processDate = "";
	private int etlProcessStatusNT = 2000;
	private String etlProcessStatus = "";
	private String etlProcessStatusDesc = "";

	private String currentProcess = "";
	private String currentProcessDesc = "";
	private List<OperationalConsoleVb> childs = new ArrayList<OperationalConsoleVb>(0);
	private String timeZone = "";

	private int versionNumber = 0;
	private int completionSuccnt = 0;
	private int completionErrcnt = 0;
	private int completionYtpcnt = 0;
	private int completionInpcnt = 0;
	
	private int triggeredBy = 0;
	private int terminatedBy = 0;
	private int reinitiatedBy = 0;
	private int currentProcessAT = 2112;
	private int completionStatusAT = 2113;
	private String completionStatus = "";
	private String completionStatusDesc = "";
	private int eventTypeAt = 2118;
	private String eventType = "M";
	private int scheduleTypeAt = 2108;
	private String scheduleType = "";
	private String dependencyFeedId = "";

	private int alertStatusAT = 2114;
	private String alertStatus = "";

	private String autoRefreshTime="";
	private int iterationCount = 0;
	private String startTime = "";
	private String endTime = "";
	private String promptValue0="";
	private String promptValue1="";
	private String promptValue2="";
	private String promptValue3="";
	private String promptValue4="";
	private String promptValue5="";
	private String promptValue6="";
	private String promptValue7="";
	private String promptValue8="";
	private String promptValue9="";
	private String promptValue10="";
	private String queryId="";
	
	private ETLFeedTimeBasedPropVb timeBasedScheduleProp = null;
	private ETLFeedEventBasedPropVb eventBasedScheduleProp = null;
	private String readinessScriptID = "";
	private String preScriptID = "";
	private String postScriptID = "";
	
	private int terminateCnt = 0;
	private int holdCnt = 0;
	private String dependencyFlag = "";
	private String isParentFlag = "Y";

	public String getQueryId() {
		return queryId;
	}
	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}
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
	public String getFeedCategory() {
		return feedCategory;
	}
	public void setFeedCategory(String feedCategory) {
		this.feedCategory = feedCategory;
	}
	public String getFeedCategoryDesc() {
		return feedCategoryDesc;
	}
	public void setFeedCategoryDesc(String feedCategoryDesc) {
		this.feedCategoryDesc = feedCategoryDesc;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getNextScheduleDate() {
		return nextScheduleDate;
	}
	public void setNextScheduleDate(String nextScheduleDate) {
		this.nextScheduleDate = nextScheduleDate;
	}
	public String getProcessDate() {
		return processDate;
	}
	public void setProcessDate(String processDate) {
		this.processDate = processDate;
	}
	public String getEtlProcessStatus() {
		return etlProcessStatus;
	}
	public void setEtlProcessStatus(String etlProcessStatus) {
		this.etlProcessStatus = etlProcessStatus;
	}
	public String getCurrentProcess() {
		return currentProcess;
	}
	public void setCurrentProcess(String currentProcess) {
		this.currentProcess = currentProcess;
	}
	public int getCompletionSuccnt() {
		return completionSuccnt;
	}
	public void setCompletionSuccnt(int completionSuccnt) {
		this.completionSuccnt = completionSuccnt;
	}
	public int getCompletionErrcnt() {
		return completionErrcnt;
	}
	public void setCompletionErrcnt(int completionErrcnt) {
		this.completionErrcnt = completionErrcnt;
	}
	public int getCompletionYtpcnt() {
		return completionYtpcnt;
	}
	public void setCompletionYtpcnt(int completionYtpcnt) {
		this.completionYtpcnt = completionYtpcnt;
	}
	public int getCompletionInpcnt() {
		return completionInpcnt;
	}
	public void setCompletionInpcnt(int completionInpcnt) {
		this.completionInpcnt = completionInpcnt;
	}
	public String getAutoRefreshTime() {
		return autoRefreshTime;
	}
	public void setAutoRefreshTime(String autoRefreshTime) {
		this.autoRefreshTime = autoRefreshTime;
	}
	public String getEtlProcessStatusDesc() {
		return etlProcessStatusDesc;
	}
	public void setEtlProcessStatusDesc(String etlProcessStatusDesc) {
		this.etlProcessStatusDesc = etlProcessStatusDesc;
	}
	public String getCurrentProcessDesc() {
		return currentProcessDesc;
	}
	public void setCurrentProcessDesc(String currentProcessDesc) {
		this.currentProcessDesc = currentProcessDesc;
	}
	
	public int getIterationCount() {
		return iterationCount;
	}
	public void setIterationCount(int iterationCount) {
		this.iterationCount = iterationCount;
	}

	public int getCurrentProcessAT() {
		return currentProcessAT;
	}
	public void setCurrentProcessAT(int currentProcessAT) {
		this.currentProcessAT = currentProcessAT;
	}
	public int getCompletionStatusAT() {
		return completionStatusAT;
	}
	public void setCompletionStatusAT(int completionStatusAT) {
		this.completionStatusAT = completionStatusAT;
	}
	public String getCompletionStatus() {
		return completionStatus;
	}
	public void setCompletionStatus(String completionStatus) {
		this.completionStatus = completionStatus;
	}
	public int getTriggeredBy() {
		return triggeredBy;
	}
	public void setTriggeredBy(int triggeredBy) {
		this.triggeredBy = triggeredBy;
	}
	public int getTerminatedBy() {
		return terminatedBy;
	}
	public void setTerminatedBy(int terminatedBy) {
		this.terminatedBy = terminatedBy;
	}
	public int getReinitiatedBy() {
		return reinitiatedBy;
	}
	public void setReinitiatedBy(int reinitiatedBy) {
		this.reinitiatedBy = reinitiatedBy;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public int getAlertStatusAT() {
		return alertStatusAT;
	}
	public void setAlertStatusAT(int alertStatusAT) {
		this.alertStatusAT = alertStatusAT;
	}
	public String getAlertStatus() {
		return alertStatus;
	}
	public void setAlertStatus(String alertStatus) {
		this.alertStatus = alertStatus;
	}
	public int getEtlProcessStatusNT() {
		return etlProcessStatusNT;
	}
	public void setEtlProcessStatusNT(int etlProcessStatusNT) {
		this.etlProcessStatusNT = etlProcessStatusNT;
	}
	public int getEventTypeAt() {
		return eventTypeAt;
	}
	public void setEventTypeAt(int eventTypeAt) {
		this.eventTypeAt = eventTypeAt;
	}
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public int getScheduleTypeAt() {
		return scheduleTypeAt;
	}
	public void setScheduleTypeAt(int scheduleTypeAt) {
		this.scheduleTypeAt = scheduleTypeAt;
	}
	public String getScheduleType() {
		return scheduleType;
	}
	public void setScheduleType(String scheduleType) {
		this.scheduleType = scheduleType;
	}
	public String getDependencyFeedId() {
		return dependencyFeedId;
	}
	public void setDependencyFeedId(String dependencyFeedId) {
		this.dependencyFeedId = dependencyFeedId;
	}
	public List<OperationalConsoleVb> getChilds() {
		return childs;
	}
	public void setChilds(List<OperationalConsoleVb> childs) {
		this.childs = childs;
	}
	public String getTimeZone() {
		return timeZone;
	}
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}
	public String getFeedType() {
		return feedType;
	}
	public void setFeedType(String feedType) {
		this.feedType = feedType;
	}
	public String getFeedName() {
		return feedName;
	}
	public void setFeedName(String feedName) {
		this.feedName = feedName;
	}
	public String getFeedTypeDesc() {
		return feedTypeDesc;
	}
	public void setFeedTypeDesc(String feedTypeDesc) {
		this.feedTypeDesc = feedTypeDesc;
	}
	public String getDebugFlag() {
		return debugFlag;
	}
	public void setDebugFlag(String debugFlag) {
		this.debugFlag = debugFlag;
	}
	public String getGrid() {
		return grid;
	}
	public void setGrid(String grid) {
		this.grid = grid;
	}
	public String getConnectorId() {
		return connectorId;
	}
	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}
	public int getVersionNumber() {
		return versionNumber;
	}
	public void setVersionNumber(int versionNumber) {
		this.versionNumber = versionNumber;
	}
	public String getPromptValue0() {
		return promptValue0;
	}
	public void setPromptValue0(String promptValue0) {
		this.promptValue0 = promptValue0;
	}
	public String getPromptValue1() {
		return promptValue1;
	}
	public void setPromptValue1(String promptValue1) {
		this.promptValue1 = promptValue1;
	}
	public String getPromptValue2() {
		return promptValue2;
	}
	public void setPromptValue2(String promptValue2) {
		this.promptValue2 = promptValue2;
	}
	public String getPromptValue3() {
		return promptValue3;
	}
	public void setPromptValue3(String promptValue3) {
		this.promptValue3 = promptValue3;
	}
	public String getPromptValue4() {
		return promptValue4;
	}
	public void setPromptValue4(String promptValue4) {
		this.promptValue4 = promptValue4;
	}
	public String getPromptValue5() {
		return promptValue5;
	}
	public void setPromptValue5(String promptValue5) {
		this.promptValue5 = promptValue5;
	}
	public String getPromptValue6() {
		return promptValue6;
	}
	public void setPromptValue6(String promptValue6) {
		this.promptValue6 = promptValue6;
	}
	public String getPromptValue7() {
		return promptValue7;
	}
	public void setPromptValue7(String promptValue7) {
		this.promptValue7 = promptValue7;
	}
	public String getPromptValue8() {
		return promptValue8;
	}
	public void setPromptValue8(String promptValue8) {
		this.promptValue8 = promptValue8;
	}
	public String getPromptValue9() {
		return promptValue9;
	}
	public void setPromptValue9(String promptValue9) {
		this.promptValue9 = promptValue9;
	}
	public String getPromptValue10() {
		return promptValue10;
	}
	public void setPromptValue10(String promptValue10) {
		this.promptValue10 = promptValue10;
	}
	public String getFeedDescription() {
		return feedDescription;
	}
	public void setFeedDescription(String feedDescription) {
		this.feedDescription = feedDescription;
	}
	public int getCurrentProcessAt() {
		return currentProcessAt;
	}
	public void setCurrentProcessAt(int currentProcessAt) {
		this.currentProcessAt = currentProcessAt;
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
	public String getCompletionStatusDesc() {
		return completionStatusDesc;
	}
	public void setCompletionStatusDesc(String completionStatusDesc) {
		this.completionStatusDesc = completionStatusDesc;
	}
	public String getEtlSourceConnectorId() {
		return etlSourceConnectorId;
	}
	public void setEtlSourceConnectorId(String etlSourceConnectorId) {
		this.etlSourceConnectorId = etlSourceConnectorId;
	}
	public String getDestinationConnectorId() {
		return destinationConnectorId;
	}
	public void setDestinationConnectorId(String destinationConnectorId) {
		this.destinationConnectorId = destinationConnectorId;
	}

	public String[] getFeedIdArr() {
		return feedIdArr;
	}
	public void setFeedIdArr(String[] feedIdArr) {
		this.feedIdArr = feedIdArr;
	}
	public String getFeedId() {
		return feedId;
	}
	public void setFeedId(String feedId) {
		this.feedId = feedId;
	}
	
	public String getDuration() {
		return duration;
	}
	public void setDuration(String duration) {
		this.duration = duration;
	}
	public String getLastExtractionDate() {
		return lastExtractionDate;
	}
	public void setLastExtractionDate(String lastExtractionDate) {
		this.lastExtractionDate = lastExtractionDate;
	}
	public int getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	public String getEffectiveDate() {
		return effectiveDate;
	}
	public void setEffectiveDate(String effectiveDate) {
		this.effectiveDate = effectiveDate;
	}
	public String getFeedInsertionType() {
		return feedInsertionType;
	}
	public void setFeedInsertionType(String feedInsertionType) {
		this.feedInsertionType = feedInsertionType;
	}
	public String getParentFeedId() {
		return parentFeedId;
	}
	public void setParentFeedId(String parentFeedId) {
		this.parentFeedId = parentFeedId;
	}
	public String getParentFeedName() {
		return ParentFeedName;
	}
	public void setParentFeedName(String parentFeedName) {
		ParentFeedName = parentFeedName;
	}
	public String getParentFeedCategory() {
		return ParentFeedCategory;
	}
	public void setParentFeedCategory(String parentFeedCategory) {
		ParentFeedCategory = parentFeedCategory;
	}
	public String getMainTableStatus() {
		return mainTableStatus;
	}
	public void setMainTableStatus(String mainTableStatus) {
		this.mainTableStatus = mainTableStatus;
	}
	public String getMainTableStatusDesc() {
		return mainTableStatusDesc;
	}
	public void setMainTableStatusDesc(String mainTableStatusDesc) {
		this.mainTableStatusDesc = mainTableStatusDesc;
	}
	public String getProcessControlStatusDesc() {
		return processControlStatusDesc;
	}
	public void setProcessControlStatusDesc(String processControlStatusDesc) {
		this.processControlStatusDesc = processControlStatusDesc;
	}
	public String getProcessControlStatus() {
		return processControlStatus;
	}
	public void setProcessControlStatus(String processControlStatus) {
		this.processControlStatus = processControlStatus;
	}
	public String getAlertType() {
		return alertType;
	}
	public void setAlertType(String alertType) {
		this.alertType = alertType;
	}
	public ETLFeedTimeBasedPropVb getTimeBasedScheduleProp() {
		return timeBasedScheduleProp;
	}
	public void setTimeBasedScheduleProp(ETLFeedTimeBasedPropVb timeBasedScheduleProp) {
		this.timeBasedScheduleProp = timeBasedScheduleProp;
	}
	public ETLFeedEventBasedPropVb getEventBasedScheduleProp() {
		return eventBasedScheduleProp;
	}
	public void setEventBasedScheduleProp(ETLFeedEventBasedPropVb eventBasedScheduleProp) {
		this.eventBasedScheduleProp = eventBasedScheduleProp;
	}
	public String getReadinessScriptID() {
		return readinessScriptID;
	}
	public void setReadinessScriptID(String readinessScriptID) {
		this.readinessScriptID = readinessScriptID;
	}
	public String getPreScriptID() {
		return preScriptID;
	}
	public void setPreScriptID(String preScriptID) {
		this.preScriptID = preScriptID;
	}
	public String getPostScriptID() {
		return postScriptID;
	}
	public void setPostScriptID(String postScriptID) {
		this.postScriptID = postScriptID;
	}
	public int getTerminateCnt() {
		return terminateCnt;
	}
	public void setTerminateCnt(int terminateCnt) {
		this.terminateCnt = terminateCnt;
	}
	public int getHoldCnt() {
		return holdCnt;
	}
	public void setHoldCnt(int holdCnt) {
		this.holdCnt = holdCnt;
	}
	public String getDependencyFlag() {
		return dependencyFlag;
	}
	public void setDependencyFlag(String dependencyFlag) {
		this.dependencyFlag = dependencyFlag;
	}
	public String getIsParentFlag() {
		return isParentFlag;
	}
	public void setIsParentFlag(String isParentFlag) {
		this.isParentFlag = isParentFlag;
	}
}