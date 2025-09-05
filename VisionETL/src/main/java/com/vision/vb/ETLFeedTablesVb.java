package com.vision.vb;

import java.util.List;

public class ETLFeedTablesVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private int tableId ;
	private String sourceConnectorId = "";
	private String tableName = "";
	private String tableAliasName = "";
	private int tableSortOrder = 0;
	private int tableTypeAt = 2103;
	private String tableType = "";
	private String tableTypeDesc = "";
	private String queryId = "";
	private String partitionRequiredFlag = "Y";
	private String customPartitionColumnFlag = "N";
	private String partitionColumnName = "";
	private String baseTableFlag = "N";
	private String accessControlFlag = "";
	private String accessControl = "";
	private int feedTableStatusNt = 2000;
	private int feedTableStatus = 0 ;
	
	private String databaseType = "";
	private String databaseConnectivityDetails = "";
	private List<ETLFeedColumnsVb> children = null;

	public ETLFeedTablesVb() {}
	
	public ETLFeedTablesVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
	}
	
	
	public String getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public String getDatabaseConnectivityDetails() {
		return databaseConnectivityDetails;
	}

	public void setDatabaseConnectivityDetails(String databaseConnectivityDetails) {
		this.databaseConnectivityDetails = databaseConnectivityDetails;
	}

	
	public List<ETLFeedColumnsVb> getChildren() {
		return children;
	}

	public void setChildren(List<ETLFeedColumnsVb> children) {
		this.children = children;
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
	public void setTableId(int tableId) {
		this.tableId = tableId; 
	}

	public int getTableId() {
		return tableId; 
	}
	public void setSourceConnectorId(String sourceConnectorId) {
		this.sourceConnectorId = sourceConnectorId; 
	}

	public String getSourceConnectorId() {
		return sourceConnectorId; 
	}
	public void setTableName(String tableName) {
		this.tableName = tableName; 
	}

	public String getTableName() {
		return tableName; 
	}
	public void setTableAliasName(String tableAliasName) {
		this.tableAliasName = tableAliasName; 
	}

	public String getTableAliasName() {
		return tableAliasName; 
	}
	public void setTableSortOrder(int tableSortOrder) {
		this.tableSortOrder = tableSortOrder; 
	}

	public int getTableSortOrder() {
		return tableSortOrder; 
	}
	public void setTableTypeAt(int tableTypeAt) {
		this.tableTypeAt = tableTypeAt; 
	}

	public int getTableTypeAt() {
		return tableTypeAt; 
	}
	public void setTableType(String tableType) {
		this.tableType = tableType; 
	}

	public String getTableType() {
		return tableType; 
	}
	public void setQueryId(String queryId) {
		this.queryId = queryId; 
	}

	public String getQueryId() {
		return queryId; 
	}
	public void setCustomPartitionColumnFlag(String customPartitionColumnFlag) {
		this.customPartitionColumnFlag = customPartitionColumnFlag; 
	}

	public String getCustomPartitionColumnFlag() {
		return customPartitionColumnFlag; 
	}
	public void setPartitionColumnName(String partitionColumnName) {
		this.partitionColumnName = partitionColumnName; 
	}

	public String getPartitionColumnName() {
		return partitionColumnName; 
	}
	public void setBaseTableFlag(String baseTableFlag) {
		this.baseTableFlag = baseTableFlag; 
	}

	public String getBaseTableFlag() {
		return baseTableFlag; 
	}
	public void setAccessControlFlag(String accessControlFlag) {
		this.accessControlFlag = accessControlFlag; 
	}

	public String getAccessControlFlag() {
		return accessControlFlag; 
	}
	public void setAccessControl(String accessControl) {
		this.accessControl = accessControl; 
	}

	public String getAccessControl() {
		return accessControl; 
	}
	public void setFeedTableStatusNt(int feedTableStatusNt) {
		this.feedTableStatusNt = feedTableStatusNt; 
	}

	public int getFeedTableStatusNt() {
		return feedTableStatusNt; 
	}
	public void setFeedTableStatus(int feedTableStatus) {
		this.feedTableStatus = feedTableStatus; 
	}

	public int getFeedTableStatus() {
		return feedTableStatus; 
	}

	public String getTableTypeDesc() {
		return tableTypeDesc;
	}

	public void setTableTypeDesc(String tableTypeDesc) {
		this.tableTypeDesc = tableTypeDesc;
	}

	public String getPartitionRequiredFlag() {
		return partitionRequiredFlag;
	}

	public void setPartitionRequiredFlag(String partitionRequiredFlag) {
		this.partitionRequiredFlag = partitionRequiredFlag;
	}


}