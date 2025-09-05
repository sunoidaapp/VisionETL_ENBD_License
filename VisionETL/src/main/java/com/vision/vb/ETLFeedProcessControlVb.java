package com.vision.vb;

import java.util.Map;

public class ETLFeedProcessControlVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String feedName = "";
	private String processDate = "";
	private String sessionId = "";
	private String nextScheduleDate = "";
	private int triggeredBy = 0;
	private int terminatedBy = 0;
	private int reinitiatedBy = 0;
	private int currentProcessAt = 2112;
	private String currentProcess = "";
	private int iterationCount = 0;
	private int completionStatusAT = 2113;
	private String completionStatus = "";
	private int alertStatusAT = 0;
	private String alertStatus = "";
	private int etlProcessStatusNT = 2000;
	private int etlprocessStatus = 0;
	private int eventTypeAT = 2118;
	private String eventType = "";
	private String dependencyFeedId = "";
	private String feedCategory = "";
	private String sourceConnectorId = "";
	private String destinationConnectorId = "";
	private String scheduleType = "";
	private int scheduleTypeAT = 2108;
	private String debugFlag = "";
	private int versionNumber;
	private String readinessScriptID = "";
	
	private String startTime = "";
	private String endTime = "";
	private String readinessStartTime = "";
	private String readinessEndTime = "";
	private String preScriptStartTime = "";
	private String preScriptEndTime = "";
	private String extractionStartTime = "";
	private String extractionEndTime = "";
	private String transformationStartTime = "";
	private String transformationEndTime = "";
	private String loadingStartTime = "";
	private String loadingEndTime = "";
	private String postScriptStartTime = "";
	private String postScriptEndTime = "";
	private String timeZone = "";
	
	private String preScriptID = "";
	private String postScriptID = "";
	
	private String lastExtractionDate = "";
	private String injectionType = "";
	private String firstTimeVarientFlag = "";
	private String firstInjectionType = "";
	private String lastExtractionDateFormatted = "";
	private String processCtrlNextScheduleDate = "";

	private String feedType = "";
	private Map<String, ETLAlertVb> alertVbMap = null;
	private String alertSystemContent = "";
	private String alertFlag = "";
	private String alertType = "";
	
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

	public String getNextScheduleDate() {
		return nextScheduleDate;
	}

	public void setNextScheduleDate(String nextScheduleDate) {
		this.nextScheduleDate = nextScheduleDate;
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

	public int getIterationCount() {
		return iterationCount;
	}

	public void setIterationCount(int iterationCount) {
		this.iterationCount = iterationCount;
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

	public int getEtlprocessStatus() {
		return etlprocessStatus;
	}

	public void setEtlprocessStatus(int etlprocessStatus) {
		this.etlprocessStatus = etlprocessStatus;
	}

	public int getEventTypeAT() {
		return eventTypeAT;
	}

	public void setEventTypeAT(int eventTypeAT) {
		this.eventTypeAT = eventTypeAT;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getDependencyFeedId() {
		return dependencyFeedId;
	}

	public void setDependencyFeedId(String dependencyFeedId) {
		this.dependencyFeedId = dependencyFeedId;
	}

	public String getFeedCategory() {
		return feedCategory;
	}

	public void setFeedCategory(String feedCategory) {
		this.feedCategory = feedCategory;
	}

	public String getSourceConnectorId() {
		return sourceConnectorId;
	}

	public void setSourceConnectorId(String sourceConnectorId) {
		this.sourceConnectorId = sourceConnectorId;
	}

	public String getDestinationConnectorId() {
		return destinationConnectorId;
	}

	public void setDestinationConnectorId(String destinationConnectorId) {
		this.destinationConnectorId = destinationConnectorId;
	}
	
	public String getScheduleType() {
		return scheduleType;
	}

	public void setScheduleType(String scheduleType) {
		this.scheduleType = scheduleType;
	}

	public int getScheduleTypeAT() {
		return scheduleTypeAT;
	}

	public void setScheduleTypeAT(int scheduleTypeAT) {
		this.scheduleTypeAT = scheduleTypeAT;
	}

	public String getDebugFlag() {
		return debugFlag;
	}

	public void setDebugFlag(String debugFlag) {
		this.debugFlag = debugFlag;
	}

	public int getVersionNumber() {
		return versionNumber;
	}

	public void setVersionNumber(int versionNumber) {
		this.versionNumber = versionNumber;
	}

	public String getReadinessScriptID() {
		return readinessScriptID;
	}

	public void setReadinessScriptID(String readinessScriptID) {
		this.readinessScriptID = readinessScriptID;
	}

	public String getReadinessStartTime() {
		return readinessStartTime;
	}

	public void setReadinessStartTime(String readinessStartTime) {
		this.readinessStartTime = readinessStartTime;
	}

	public String getReadinessEndTime() {
		return readinessEndTime;
	}

	public void setReadinessEndTime(String readinessEndTime) {
		this.readinessEndTime = readinessEndTime;
	}

	public String getPreScriptStartTime() {
		return preScriptStartTime;
	}

	public void setPreScriptStartTime(String preScriptStartTime) {
		this.preScriptStartTime = preScriptStartTime;
	}

	public String getPreScriptEndTime() {
		return preScriptEndTime;
	}

	public void setPreScriptEndTime(String preScriptEndTime) {
		this.preScriptEndTime = preScriptEndTime;
	}

	public String getExtractionStartTime() {
		return extractionStartTime;
	}

	public void setExtractionStartTime(String extractionStartTime) {
		this.extractionStartTime = extractionStartTime;
	}

	public String getExtractionEndTime() {
		return extractionEndTime;
	}

	public void setExtractionEndTime(String extractionEndTime) {
		this.extractionEndTime = extractionEndTime;
	}

	public String getTransformationStartTime() {
		return transformationStartTime;
	}

	public void setTransformationStartTime(String transformationStartTime) {
		this.transformationStartTime = transformationStartTime;
	}

	public String getTransformationEndTime() {
		return transformationEndTime;
	}

	public void setTransformationEndTime(String transformationEndTime) {
		this.transformationEndTime = transformationEndTime;
	}

	public String getLoadingStartTime() {
		return loadingStartTime;
	}

	public void setLoadingStartTime(String loadingStartTime) {
		this.loadingStartTime = loadingStartTime;
	}

	public String getLoadingEndTime() {
		return loadingEndTime;
	}

	public void setLoadingEndTime(String loadingEndTime) {
		this.loadingEndTime = loadingEndTime;
	}

	public String getPostScriptStartTime() {
		return postScriptStartTime;
	}

	public void setPostScriptStartTime(String postScriptStartTime) {
		this.postScriptStartTime = postScriptStartTime;
	}

	public String getPostScriptEndTime() {
		return postScriptEndTime;
	}

	public void setPostScriptEndTime(String postScriptEndTime) {
		this.postScriptEndTime = postScriptEndTime;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
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

	public String getLastExtractionDate() {
		return lastExtractionDate;
	}

	public void setLastExtractionDate(String lastExtractionDate) {
		this.lastExtractionDate = lastExtractionDate;
	}

	public String getInjectionType() {
		return injectionType;
	}

	public void setInjectionType(String injectionType) {
		this.injectionType = injectionType;
	}

	public String getFirstTimeVarientFlag() {
		return firstTimeVarientFlag;
	}

	public void setFirstTimeVarientFlag(String firstTimeVarientFlag) {
		this.firstTimeVarientFlag = firstTimeVarientFlag;
	}

	public String getFirstInjectionType() {
		return firstInjectionType;
	}

	public void setFirstInjectionType(String firstInjectionType) {
		this.firstInjectionType = firstInjectionType;
	}

	public String getLastExtractionDateFormatted() {
		return lastExtractionDateFormatted;
	}

	public void setLastExtractionDateFormatted(String lastExtractionDateFormatted) {
		this.lastExtractionDateFormatted = lastExtractionDateFormatted;
	}

	public String getProcessCtrlNextScheduleDate() {
		return processCtrlNextScheduleDate;
	}

	public void setProcessCtrlNextScheduleDate(String processCtrlNextScheduleDate) {
		this.processCtrlNextScheduleDate = processCtrlNextScheduleDate;
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

	public Map<String, ETLAlertVb> getAlertVbMap() {
		return alertVbMap;
	}

	public void setAlertVbMap(Map<String, ETLAlertVb> alertVbMap) {
		this.alertVbMap = alertVbMap;
	}

	public String getAlertSystemContent() {
		return alertSystemContent;
	}

	public void setAlertSystemContent(String alertSystemContent) {
		this.alertSystemContent = alertSystemContent;
	}

	public String getAlertFlag() {
		return alertFlag;
	}

	public void setAlertFlag(String alertFlag) {
		this.alertFlag = alertFlag;
	}

	public String getAlertType() {
		return alertType;
	}

	public void setAlertType(String alertType) {
		this.alertType = alertType;
	}

}