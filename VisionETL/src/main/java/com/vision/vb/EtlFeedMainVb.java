package com.vision.vb;

import java.util.List;

public class EtlFeedMainVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String feedId = "";	
	
	private String nodeId = "";
	private String dependencyFeedId = "";
	private String sessionId = "";
	private String feedName = "";
	private String feedDescription = "";
	private String effectiveDate = "";
	private String lastExtractionDate = "";
	private int completionStatusAt = 2113 ;
	private String completionStatus = "";
	private String completionStatusDesc = "";
	private int batchSize = 0;

	private int feedTypeAt = 2104 ;
	private String feedType = "";
	private String feedTypeDesc = "";
	private String feedCategory = "";
	private String feedCategoryDesc = "";
	private String alertMechanism = "";
	private String sampleData = "";
	private String privilageMechanism = "";
	private int feedStatusNt = 2000 ;
	private int feedStatus = -1 ;
	List<SmartSearchVb> smartSearchOpt = null;
	private String connectorId = "";
	private String tableId = "";
	private int tabId = 0;
	private String columnHeaders = "";
	List<String> rowData= null;
	List<EtlFeedMainVb> categoryFeedlst = null;
	private String sampleDataRuleContext = "";
	private String customSampleDataRuleContext = "";
	private String isValidType = "Y";	

	private List<EtlFeedSourceVb> etlFeedSourceList = null;

	private List<ETLFeedTablesVb> etlFeedTablesList = null;
	
	private List<ETLFeedColumnsVb> etlFeedColumnsList = null;
	
	private List<EtlFeedRelationVb> etlFeedRelationList = null;
	
	private List<EtlExtractionSummaryFieldsVb> etlExtractionSummaryFieldsList = null; 
	
	private List<EtlFeedTranVb> etlFeedTranList = null;

	private List<EtlFeedTransformationVb> etlFeedTransformationList = null;
	
	private List<EtlTransformedColumnsVb> etlTransformedColumnsList = null;
		
	private List<EtlFeedDestinationVb> etlFeedDestinationList = null; 
	
	private List<EtlTargetColumnsVb> etlTargetColumnsList = null;

	private List<EtlFeedLoadMappingVb> etlFeedLoadMappingList = null;
	
	private List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList = null;
		
	private List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList = null;
			
	private List<EtlFeedAlertConfigVb> etlFeedAlertConfiglst =null;

	private List<EtlFeedTranNodeVb> etlFeedNodeList = null;
	
	private List<EtlFeedTranColumnVb> etlFeedTranColumnList = null;
	
	private int visionId = 0; // VISION_ID -- for delete Transformation Session
	
	private String distinctFlag = "";
	private String timeZone = "";
	
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
	public void setFeedName(String feedName) {
		this.feedName = feedName; 
	}

	public String getFeedName() {
		return feedName; 
	}
	public void setFeedDescription(String feedDescription) {
		this.feedDescription = feedDescription; 
	}

	public String getFeedDescription() {
		return feedDescription; 
	}
	public void setEffectiveDate(String effectiveDate) {
		this.effectiveDate = effectiveDate; 
	}

	public String getEffectiveDate() {
		return effectiveDate; 
	}
	public void setLastExtractionDate(String lastExtractionDate) {
		this.lastExtractionDate = lastExtractionDate; 
	}

	public String getLastExtractionDate() {
		return lastExtractionDate; 
	}
	public void setFeedTypeAt(int feedTypeAt) {
		this.feedTypeAt = feedTypeAt; 
	}

	public int getFeedTypeAt() {
		return feedTypeAt; 
	}
	public void setFeedType(String feedType) {
		this.feedType = feedType; 
	}

	public String getFeedType() {
		return feedType; 
	}
	public void setFeedCategory(String feedCategory) {
		this.feedCategory = feedCategory; 
	}

	public String getFeedCategory() {
		return feedCategory; 
	}
	public void setAlertMechanism(String alertMechanism) {
		this.alertMechanism = alertMechanism; 
	}

	public String getAlertMechanism() {
		return alertMechanism; 
	}
	public void setPrivilageMechanism(String privilageMechanism) {
		this.privilageMechanism = privilageMechanism; 
	}

	public String getPrivilageMechanism() {
		return privilageMechanism; 
	}
	public void setFeedStatusNt(int feedStatusNt) {
		this.feedStatusNt = feedStatusNt; 
	}

	public int getFeedStatusNt() {
		return feedStatusNt; 
	}
	public void setFeedStatus(int feedStatus) {
		this.feedStatus = feedStatus; 
	}

	public int getFeedStatus() {
		return feedStatus; 
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

	public String getFeedTypeDesc() {
		return feedTypeDesc;
	}

	public void setFeedTypeDesc(String feedTypeDesc) {
		this.feedTypeDesc = feedTypeDesc;
	}

	public String getFeedCategoryDesc() {
		return feedCategoryDesc;
	}

	public void setFeedCategoryDesc(String feedCategoryDesc) {
		this.feedCategoryDesc = feedCategoryDesc;
	}

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

	public List<EtlFeedSourceVb> getEtlFeedSourceList() {
		return etlFeedSourceList;
	}

	public void setEtlFeedSourceList(List<EtlFeedSourceVb> etlFeedSourceList) {
		this.etlFeedSourceList = etlFeedSourceList;
	}

	public List<ETLFeedTablesVb> getEtlFeedTablesList() {
		return etlFeedTablesList;
	}

	public void setEtlFeedTablesList(List<ETLFeedTablesVb> etlFeedTablesList) {
		this.etlFeedTablesList = etlFeedTablesList;
	}

	public List<ETLFeedColumnsVb> getEtlFeedColumnsList() {
		return etlFeedColumnsList;
	}

	public void setEtlFeedColumnsList(List<ETLFeedColumnsVb> etlFeedColumnsList) {
		this.etlFeedColumnsList = etlFeedColumnsList;
	}

	public List<EtlFeedRelationVb> getEtlFeedRelationList() {
		return etlFeedRelationList;
	}

	public void setEtlFeedRelationList(List<EtlFeedRelationVb> etlFeedRelationList) {
		this.etlFeedRelationList = etlFeedRelationList;
	}

	public List<EtlExtractionSummaryFieldsVb> getEtlExtractionSummaryFieldsList() {
		return etlExtractionSummaryFieldsList;
	}

	public void setEtlExtractionSummaryFieldsList(List<EtlExtractionSummaryFieldsVb> etlExtractionSummaryFieldsList) {
		this.etlExtractionSummaryFieldsList = etlExtractionSummaryFieldsList;
	}

	public List<EtlFeedTransformationVb> getEtlFeedTransformationList() {
		return etlFeedTransformationList;
	}

	public void setEtlFeedTransformationList(List<EtlFeedTransformationVb> etlFeedTransformationList) {
		this.etlFeedTransformationList = etlFeedTransformationList;
	}

	public List<EtlTransformedColumnsVb> getEtlTransformedColumnsList() {
		return etlTransformedColumnsList;
	}

	public void setEtlTransformedColumnsList(List<EtlTransformedColumnsVb> etlTransformedColumnsList) {
		this.etlTransformedColumnsList = etlTransformedColumnsList;
	}

	public List<EtlFeedDestinationVb> getEtlFeedDestinationList() {
		return etlFeedDestinationList;
	}

	public void setEtlFeedDestinationList(List<EtlFeedDestinationVb> etlFeedDestinationList) {
		this.etlFeedDestinationList = etlFeedDestinationList;
	}

	public List<EtlTargetColumnsVb> getEtlTargetColumnsList() {
		return etlTargetColumnsList;
	}

	public void setEtlTargetColumnsList(List<EtlTargetColumnsVb> etlTargetColumnsList) {
		this.etlTargetColumnsList = etlTargetColumnsList;
	}

	public List<EtlFeedLoadMappingVb> getEtlFeedLoadMappingList() {
		return etlFeedLoadMappingList;
	}

	public void setEtlFeedLoadMappingList(List<EtlFeedLoadMappingVb> etlFeedLoadMappingList) {
		this.etlFeedLoadMappingList = etlFeedLoadMappingList;
	}

	public List<EtlFeedInjectionConfigVb> getEtlFeedInjectionConfigList() {
		return etlFeedInjectionConfigList;
	}

	public void setEtlFeedInjectionConfigList(List<EtlFeedInjectionConfigVb> etlFeedInjectionConfigList) {
		this.etlFeedInjectionConfigList = etlFeedInjectionConfigList;
	}

	public List<EtlFeedScheduleConfigVb> getEtlFeedScheduleConfigList() {
		return etlFeedScheduleConfigList;
	}

	public void setEtlFeedScheduleConfigList(List<EtlFeedScheduleConfigVb> etlFeedScheduleConfigList) {
		this.etlFeedScheduleConfigList = etlFeedScheduleConfigList;
	}

	public List<String> getRowData() {
		return rowData;
	}

	public void setRowData(List<String> rowData) {
		this.rowData = rowData;
	}

	public String getColumnHeaders() {
		return columnHeaders;
	}

	public void setColumnHeaders(String columnHeaders) {
		this.columnHeaders = columnHeaders;
	}

	public int getTabId() {
		return tabId;
	}

	public void setTabId(int tabId) {
		this.tabId = tabId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public List<EtlFeedMainVb> getCategoryFeedlst() {
		return categoryFeedlst;
	}

	public void setCategoryFeedlst(List<EtlFeedMainVb> categoryFeedlst) {
		this.categoryFeedlst = categoryFeedlst;
	}

	public List<EtlFeedTranVb> getEtlFeedTranList() {
		return etlFeedTranList;
	}

	public void setEtlFeedTranList(List<EtlFeedTranVb> etlFeedTranList) {
		this.etlFeedTranList = etlFeedTranList;
	}

	/*public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}*/

	public String getDependencyFeedId() {
		return dependencyFeedId;
	}

	public void setDependencyFeedId(String dependencyFeedId) {
		this.dependencyFeedId = dependencyFeedId;
	}

	public String getSampleData() {
		return sampleData;
	}

	public void setSampleData(String sampleData) {
		this.sampleData = sampleData;
	}

	public String getSampleDataRuleContext() {
		return sampleDataRuleContext;
	}

	public void setSampleDataRuleContext(String sampleDataRuleContext) {
		this.sampleDataRuleContext = sampleDataRuleContext;
	}

	public String getCustomSampleDataRuleContext() {
		return customSampleDataRuleContext;
	}

	public void setCustomSampleDataRuleContext(String customSampleDataRuleContext) {
		this.customSampleDataRuleContext = customSampleDataRuleContext;
	}

	public int getCompletionStatusAt() {
		return completionStatusAt;
	}

	public void setCompletionStatusAt(int completionStatusAt) {
		this.completionStatusAt = completionStatusAt;
	}

	public String getCompletionStatus() {
		return completionStatus;
	}

	public void setCompletionStatus(String completionStatus) {
		this.completionStatus = completionStatus;
	}

	public String getCompletionStatusDesc() {
		return completionStatusDesc;
	}

	public void setCompletionStatusDesc(String completionStatusDesc) {
		this.completionStatusDesc = completionStatusDesc;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public String getIsValidType() {
		return isValidType;
	}

	public void setIsValidType(String isValidType) {
		this.isValidType = isValidType;
	}

	public List<EtlFeedAlertConfigVb> getEtlFeedAlertConfiglst() {
		return etlFeedAlertConfiglst;
	}

	public void setEtlFeedAlertConfiglst(List<EtlFeedAlertConfigVb> etlFeedAlertConfiglst) {
		this.etlFeedAlertConfiglst = etlFeedAlertConfiglst;
	}

	public String getConnectorId() {
		return connectorId;
	}

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public String getTableId() {
		return tableId;
	}

	public void setTableId(String tableId) {
		this.tableId = tableId;
	}
	
	public List<EtlFeedTranNodeVb> getEtlFeedNodeList() {
		return etlFeedNodeList;
	}

	public void setEtlFeedNodeList(List<EtlFeedTranNodeVb> etlFeedNodeList) {
		this.etlFeedNodeList = etlFeedNodeList;
	}

	public List<EtlFeedTranColumnVb> getEtlFeedTranColumnList() {
		return etlFeedTranColumnList;
	}

	public void setEtlFeedTranColumnList(List<EtlFeedTranColumnVb> etlFeedTranColumnList) {
		this.etlFeedTranColumnList = etlFeedTranColumnList;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public int getVisionId() {
		return visionId;
	}

	public void setVisionId(int visionId) {
		this.visionId = visionId;
	}

	public String getDistinctFlag() {
		return distinctFlag;
	}

	public void setDistinctFlag(String distinctFlag) {
		this.distinctFlag = distinctFlag;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}
	
}