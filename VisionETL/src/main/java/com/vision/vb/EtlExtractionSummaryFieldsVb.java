package com.vision.vb;

public class EtlExtractionSummaryFieldsVb extends CommonVb{
	
	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String tableId = "";
	private int colId = 0;
	private long userId = 0 ;
	private String colName = "";
	private String colDataType = "";
	private String alias = "";
	private String conditionOperator = "";
	private String value1 = "";
	private String value2 = "";
	private String displayFlag = "";
	private String sortType = "";
	private int sortOrder = 0 ;
	private String aggFunction = "";
	private String groupBy = "";
	private String colDisplayType = "";
	private int etlFieldsStatusNt = 2000 ;
	private int etlFieldsStatus = 0 ;
	private String experssionText = "";
	private String joinCondition = "";
	private String dynamicStartFlag = "";
	private String dynamicEndFlag = "";
	private String dynamicStartDate = "";
	private String dynamicEndDate = "";
	private String dynamicStartOperator = "";
	private String dynamicEndOperator = "";
	private String dynamicDateFormat = "";
	//private String sampleDataRuleContext = "";
	
	private String summaryCriteria = "";
	private String summaryValue1 = "";
	private String summaryValue2 = "";

	private String summaryStartFlag = "";
	private String summaryEndFlag = "";
	private String summaryStartDate = "";
	private String summaryEndDate = "";
	private String summaryStartOperator ="";
	private String summaryEndOperator = "";

	private String dateFormat = "";
	public String formatTypeDesc = "";
	private String dateFormattingSyntax = "";
	private String dateConversionSyntax = "";
	private String sourceConnectorId = "";
	private String tableName = "";
	private String tableAliasName = "";
	private String tableType = "";
	private String javaFormatDesc ="";
	
	private String sampleDataRule = "";
	private String sampleDataCustomRule= ""; 
	
	private int linkedColumnId;
	
	public EtlExtractionSummaryFieldsVb() {}

	public EtlExtractionSummaryFieldsVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
	}
	
	
	public String getSummaryCriteria() {
		return summaryCriteria;
	}

	public void setSummaryCriteria(String summaryCriteria) {
		this.summaryCriteria = summaryCriteria;
	}

	public String getSummaryValue1() {
		return summaryValue1;
	}

	public void setSummaryValue1(String summaryValue1) {
		this.summaryValue1 = summaryValue1;
	}

	public String getSummaryValue2() {
		return summaryValue2;
	}

	public void setSummaryValue2(String summaryValue2) {
		this.summaryValue2 = summaryValue2;
	}

	public String getSummaryStartFlag() {
		return summaryStartFlag;
	}

	public void setSummaryStartFlag(String summaryStartFlag) {
		this.summaryStartFlag = summaryStartFlag;
	}

	public String getSummaryEndFlag() {
		return summaryEndFlag;
	}

	public void setSummaryEndFlag(String summaryEndFlag) {
		this.summaryEndFlag = summaryEndFlag;
	}

	public String getSummaryStartDate() {
		return summaryStartDate;
	}

	public void setSummaryStartDate(String summaryStartDate) {
		this.summaryStartDate = summaryStartDate;
	}

	public String getSummaryEndDate() {
		return summaryEndDate;
	}

	public void setSummaryEndDate(String summaryEndDate) {
		this.summaryEndDate = summaryEndDate;
	}

	public String getSummaryStartOperator() {
		return summaryStartOperator;
	}

	public void setSummaryStartOperator(String summaryStartOperator) {
		this.summaryStartOperator = summaryStartOperator;
	}

	public String getSummaryEndOperator() {
		return summaryEndOperator;
	}

	public void setSummaryEndOperator(String summaryEndOperator) {
		this.summaryEndOperator = summaryEndOperator;
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
	public void setTableId(String tableId) {
		this.tableId = tableId; 
	}

	public String getTableId() {
		return tableId; 
	}
	public void setColId(int colId) {
		this.colId = colId; 
	}

	public int getColId() {
		return colId; 
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public void setColName(String colName) {
		this.colName = colName; 
	}

	public String getColName() {
		return colName; 
	}
	public void setAlias(String alias) {
		this.alias = alias; 
	}

	public String getAlias() {
		return alias; 
	}
	public void setConditionOperator(String conditionOperator) {
		this.conditionOperator = conditionOperator; 
	}

	public String getConditionOperator() {
		return conditionOperator; 
	}
	public void setValue1(String value1) {
		this.value1 = value1; 
	}

	public String getValue1() {
		return value1; 
	}
	public void setValue2(String value2) {
		this.value2 = value2; 
	}

	public String getValue2() {
		return value2; 
	}
	public void setDisplayFlag(String displayFlag) {
		this.displayFlag = displayFlag; 
	}

	public String getDisplayFlag() {
		return displayFlag; 
	}
	public void setSortType(String sortType) {
		this.sortType = sortType; 
	}

	public String getSortType() {
		return sortType; 
	}
	public void setSortOrder(int sortOrder) {
		this.sortOrder = sortOrder; 
	}

	public int getSortOrder() {
		return sortOrder; 
	}
	public void setAggFunction(String aggFunction) {
		this.aggFunction = aggFunction; 
	}

	public String getAggFunction() {
		return aggFunction; 
	}
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy; 
	}

	public String getGroupBy() {
		return groupBy; 
	}

		public String getColDisplayType() {
		return colDisplayType;
	}

	public void setColDisplayType(String colDisplayType) {
		this.colDisplayType = colDisplayType;
	}

		public void setEtlFieldsStatusNt(int etlFieldsStatusNt) {
		this.etlFieldsStatusNt = etlFieldsStatusNt; 
	}

	public int getEtlFieldsStatusNt() {
		return etlFieldsStatusNt; 
	}
	public void setEtlFieldsStatus(int etlFieldsStatus) {
		this.etlFieldsStatus = etlFieldsStatus; 
	}

	public int getEtlFieldsStatus() {
		return etlFieldsStatus; 
	}
	public void setExperssionText(String experssionText) {
		this.experssionText = experssionText; 
	}

	public String getExperssionText() {
		return experssionText; 
	}
		public void setJoinCondition(String joinCondition) {
		this.joinCondition = joinCondition; 
	}

	public String getJoinCondition() {
		return joinCondition; 
	}
	public void setDynamicStartFlag(String dynamicStartFlag) {
		this.dynamicStartFlag = dynamicStartFlag; 
	}

	public String getDynamicStartFlag() {
		return dynamicStartFlag; 
	}
	public void setDynamicEndFlag(String dynamicEndFlag) {
		this.dynamicEndFlag = dynamicEndFlag; 
	}

	public String getDynamicEndFlag() {
		return dynamicEndFlag; 
	}
	public void setDynamicStartDate(String dynamicStartDate) {
		this.dynamicStartDate = dynamicStartDate; 
	}

	public String getDynamicStartDate() {
		return dynamicStartDate; 
	}
	public void setDynamicEndDate(String dynamicEndDate) {
		this.dynamicEndDate = dynamicEndDate; 
	}

	public String getDynamicEndDate() {
		return dynamicEndDate; 

	}
	
	public String getDynamicStartOperator() {
		return dynamicStartOperator;
	}

	public void setDynamicStartOperator(String dynamicStartOperator) {
		this.dynamicStartOperator = dynamicStartOperator;
	}

	public String getDynamicEndOperator() {
		return dynamicEndOperator;
	}

	public void setDynamicEndOperator(String dynamicEndOperator) {
		this.dynamicEndOperator = dynamicEndOperator;
	}

	public void setDynamicDateFormat(String dynamicDateFormat) {
		this.dynamicDateFormat = dynamicDateFormat; 
	}

	public String getDynamicDateFormat() {
		return dynamicDateFormat; 
	}
	/*public void setSampleDataRuleContext(String sampleDataRuleContext) {
		this.sampleDataRuleContext = sampleDataRuleContext; 
	}

	public String getSampleDataRuleContext() {
		return sampleDataRuleContext; 
	}*/

	public String getSourceConnectorId() {
		return sourceConnectorId;
	}

	public void setSourceConnectorId(String sourceConnectorId) {
		this.sourceConnectorId = sourceConnectorId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableAliasName() {
		return tableAliasName;
	}

	public void setTableAliasName(String tableAliasName) {
		this.tableAliasName = tableAliasName;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public String getJavaFormatDesc() {
		return javaFormatDesc;
	}

	public void setJavaFormatDesc(String javaFormatDesc) {
		this.javaFormatDesc = javaFormatDesc;
	}
	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getFormatTypeDesc() {
		return formatTypeDesc;
	}

	public void setFormatTypeDesc(String formatTypeDesc) {
		this.formatTypeDesc = formatTypeDesc;
	}

	public String getDateFormattingSyntax() {
		return dateFormattingSyntax;
	}

	public void setDateFormattingSyntax(String dateFormattingSyntax) {
		this.dateFormattingSyntax = dateFormattingSyntax;
	}

	public String getDateConversionSyntax() {
		return dateConversionSyntax;
	}

	public void setDateConversionSyntax(String dateConversionSyntax) {
		this.dateConversionSyntax = dateConversionSyntax;
	}

	public String getColDataType() {
		return colDataType;
	}

	public void setColDataType(String colDataType) {
		this.colDataType = colDataType;
	}

	public String getSampleDataRule() {
		return sampleDataRule;
	}

	public void setSampleDataRule(String sampleDataRule) {
		this.sampleDataRule = sampleDataRule;
	}

	public String getSampleDataCustomRule() {
		return sampleDataCustomRule;
	}

	public void setSampleDataCustomRule(String sampleDataCustomRule) {
		this.sampleDataCustomRule = sampleDataCustomRule;
	}

	public int getLinkedColumnId() {
		return linkedColumnId;
	}

	public void setLinkedColumnId(int linkedColumnId) {
		this.linkedColumnId = linkedColumnId;
	}

}