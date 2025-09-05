package com.vision.vb;

import com.vision.util.ValidationUtil;

public class EtlFeedTranColumnVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String nodeId = "";
	private String columnId = "";
	private String columnName = "";
	private String columnDesc = "";
	private String derivedColumnFlag = "N";
	private String expressionTypeAT = "2125";
	private String expressionType = "";
	private String expressionText = null;
	private String aggFunction = null;
	private String aggExpressionText = null;
	private String groupByFlag = "N";
	private String inputFlag = "N";
	private String parentNodeId = null;
	private String parentColumnId = null;
	private String outputFlag = "N";
	private String dataTypeAT = "2127";
	private String dataType = "";
	private String size = null;
	private String scale = null;
	private String numberFormatNT = "1102";
	private String numberFormat = null;
	private String dateFormatNT = "1103";
	private String dateFormat = null;
	private int columnStatusNt = 2000;
	private int columnStatus = 0;
	private String sessionId = "";

	private int columnSortOrder = 0;
	private String slabId = null;
	private String aliasName = "";

	private String linkedColumn = null;
	private String joinOperator = null;
	private String filterCriteria = null;
	private String filterValue1 = null;
	private String filterValue2 = null;

	// private String materialisedColumnId = ""; //column dropped and added in  etl_extraction_summary_fields

	private String outputColumnName = null;
	/* copyParentNodeId -> Used to save Parent_Node_ID info when the actual 'parentNodeId'object is replace with group_UID for Full run process of transformation */
	private String copyParentNodeId = null;

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

	public String getColumnId() {
		return columnId;
	}

	public void setColumnId(String columnId) {
		this.columnId = columnId;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnDesc() {
		return columnDesc;
	}

	public void setColumnDesc(String columnDesc) {
		this.columnDesc = columnDesc;
	}

	public String getDerivedColumnFlag() {
		return derivedColumnFlag;
	}

	public void setDerivedColumnFlag(String derivedColumnFlag) {
		this.derivedColumnFlag = derivedColumnFlag;
	}

	public String getExpressionTypeAT() {
		return expressionTypeAT;
	}

	public void setExpressionTypeAT(String expressionTypeAT) {
		this.expressionTypeAT = expressionTypeAT;
	}

	public String getExpressionType() {
		return expressionType;
	}

	public void setExpressionType(String expressionType) {
		this.expressionType = expressionType;
	}

	public String getExpressionText() {
		return expressionText;
	}

	public void setExpressionText(String expressionText) {
		this.expressionText = ValidationUtil.isValid(expressionText) ? expressionText : null;
	}

	public String getAggFunction() {
		return aggFunction;
	}

	public void setAggFunction(String aggFunction) {
		this.aggFunction = ValidationUtil.isValid(aggFunction) ? aggFunction : null;
	}

	public String getAggExpressionText() {
		return aggExpressionText;
	}

	public void setAggExpressionText(String aggExpressionText) {
		this.aggExpressionText = ValidationUtil.isValid(aggExpressionText) ? aggExpressionText : null;
	}

	public String getGroupByFlag() {
		return groupByFlag;
	}

	public void setGroupByFlag(String groupByFlag) {
		this.groupByFlag = groupByFlag;
	}

	public String getInputFlag() {
		return inputFlag;
	}

	public void setInputFlag(String inputFlag) {
		this.inputFlag = inputFlag;
	}

	public String getParentNodeId() {
		return parentNodeId;
	}

	public void setParentNodeId(String parentNodeId) {
		this.parentNodeId = ValidationUtil.isValid(parentNodeId) ? parentNodeId : null;
	}

	public String getParentColumnId() {
		return parentColumnId;
	}

	public void setParentColumnId(String parentColumnId) {
		this.parentColumnId = ValidationUtil.isValid(parentColumnId) ? parentColumnId : null;
	}

	public String getOutputFlag() {
		return outputFlag;
	}

	public void setOutputFlag(String outputFlag) {
		this.outputFlag = outputFlag;
	}

	public String getDataTypeAT() {
		return dataTypeAT;
	}

	public void setDataTypeAT(String dataTypeAT) {
		this.dataTypeAT = dataTypeAT;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = ValidationUtil.isValid(size) ? size : null;
	}

	public String getScale() {
		return scale;
	}

	public void setScale(String scale) {
		this.scale = ValidationUtil.isValid(scale) ? scale : null;
	}

	public String getNumberFormatNT() {
		return numberFormatNT;
	}

	public void setNumberFormatNT(String numberFormatNT) {
		this.numberFormatNT = numberFormatNT;
	}

	public String getNumberFormat() {
		return numberFormat;
	}

	public void setNumberFormat(String numberFormat) {
		this.numberFormat = ValidationUtil.isValid(numberFormat) ? numberFormat : null;
	}

	public String getDateFormatNT() {
		return dateFormatNT;
	}

	public void setDateFormatNT(String dateFormatNT) {
		this.dateFormatNT = dateFormatNT;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = ValidationUtil.isValid(dateFormat) ? dateFormat : null;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public int getColumnStatusNt() {
		return columnStatusNt;
	}

	public void setColumnStatusNt(int columnStatusNt) {
		this.columnStatusNt = columnStatusNt;
	}

	public int getColumnStatus() {
		return columnStatus;
	}

	public void setColumnStatus(int columnStatus) {
		this.columnStatus = columnStatus;
	}

	public int getColumnSortOrder() {
		return columnSortOrder;
	}

	public void setColumnSortOrder(int columnSortOrder) {
		this.columnSortOrder = columnSortOrder;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getSlabId() {
		return slabId;
	}

	public void setSlabId(String slabId) {
		this.slabId = ValidationUtil.isValid(slabId) ? slabId : null;
	}

	public String getAliasName() {
		return aliasName;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}

	public String getLinkedColumn() {
		return linkedColumn;
	}

	public void setLinkedColumn(String linkedColumn) {
		this.linkedColumn = ValidationUtil.isValid(linkedColumn) ? linkedColumn : null;
	}

	public String getJoinOperator() {
		return joinOperator;
	}

	public void setJoinOperator(String joinOperator) {
		this.joinOperator = ValidationUtil.isValid(joinOperator) ? joinOperator : null;
	}

	public String getFilterCriteria() {
		return filterCriteria;
	}

	public void setFilterCriteria(String filterCriteria) {
		this.filterCriteria = ValidationUtil.isValid(filterCriteria) ? filterCriteria : null;
	}

	public String getFilterValue1() {
		return filterValue1;
	}

	public void setFilterValue1(String filterValue1) {
		this.filterValue1 = ValidationUtil.isValid(filterValue1) ? filterValue1 : null;
	}

	public String getFilterValue2() {
		return filterValue2;
	}

	public void setFilterValue2(String filterValue2) {
		this.filterValue2 = ValidationUtil.isValid(filterValue2) ? filterValue2 : null;
	}

	public String getOutputColumnName() {
		return outputColumnName;
	}

	public void setOutputColumnName(String outputColumnName) {
		this.outputColumnName = ValidationUtil.isValid(outputColumnName) ? outputColumnName : null;
	}

	public String getCopyParentNodeId() {
		return copyParentNodeId;
	}

	public void setCopyParentNodeId(String copyParentNodeId) {
		this.copyParentNodeId = copyParentNodeId;
	}

}