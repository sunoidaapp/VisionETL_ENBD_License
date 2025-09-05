package com.vision.vb;

import java.util.List;

public class EtlForQueryReportVb extends CommonVb {
	
	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private Integer baseTableId = null;
	private String hashVariableScript = "";
	private List<EtlExtractionSummaryFieldsVb> reportFields = null;
	private String tableName="";
	
	private String select = "";
	private String where = "";
	private String groupBy = "";
	private String orderBy = "";
	
	private int feedStatus = 0 ;
	
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
	public Integer getBaseTableId() {
		return baseTableId;
	}
	public void setBaseTableId(Integer baseTableId) {
		this.baseTableId = baseTableId;
	}
	public String getHashVariableScript() {
		return hashVariableScript;
	}
	public void setHashVariableScript(String hashVariableScript) {
		this.hashVariableScript = hashVariableScript;
	}
	public List<EtlExtractionSummaryFieldsVb> getReportFields() {
		return reportFields;
	}
	public void setReportFields(List<EtlExtractionSummaryFieldsVb> reportFields) {
		this.reportFields = reportFields;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getSelect() {
		return select;
	}
	public void setSelect(String select) {
		this.select = select;
	}
	public String getWhere() {
		return where;
	}
	public void setWhere(String where) {
		this.where = where;
	}
	public String getGroupBy() {
		return groupBy;
	}
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}
	public String getOrderBy() {
		return orderBy;
	}
	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}
	public int getFeedStatus() {
		return feedStatus;
	}
	public void setFeedStatus(int feedStatus) {
		this.feedStatus = feedStatus;
	}

}