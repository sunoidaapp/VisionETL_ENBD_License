package com.vision.vb;

public class EtlTargetColumnsVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String targetColumnId = "";
	private String targetColumnName = "";
	private int targetColumnSortOrder = 0 ;
	private int columnDatatypeAt = 2104 ;
	private String columnDatatype = "";
	private int columnLength = 0 ;
	private int dateFormatAt = 2105 ;
	private String dateFormat = "";
	private String primaryKeyFlag = "N";
	private String partitionColumnFlag = "";
	private int feedColumnStatusNt = 2000 ;
	private int feedColumnStatus = 0 ;
	
	public EtlTargetColumnsVb() {}

	public EtlTargetColumnsVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
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
	public void setTargetColumnId(String targetColumnId) {
		this.targetColumnId = targetColumnId; 
	}

	public String getTargetColumnId() {
		return targetColumnId; 
	}
	public void setTargetColumnName(String targetColumnName) {
		this.targetColumnName = targetColumnName; 
	}

	public String getTargetColumnName() {
		return targetColumnName; 
	}
	public void setTargetColumnSortOrder(int targetColumnSortOrder) {
		this.targetColumnSortOrder = targetColumnSortOrder; 
	}

	public int getTargetColumnSortOrder() {
		return targetColumnSortOrder; 
	}
	public void setColumnDatatypeAt(int columnDatatypeAt) {
		this.columnDatatypeAt = columnDatatypeAt; 
	}

	public int getColumnDatatypeAt() {
		return columnDatatypeAt; 
	}
	public void setColumnDatatype(String columnDatatype) {
		this.columnDatatype = columnDatatype; 
	}

	public String getColumnDatatype() {
		return columnDatatype; 
	}
	public void setColumnLength(int columnLength) {
		this.columnLength = columnLength; 
	}

	public int getColumnLength() {
		return columnLength; 
	}
	public void setDateFormatAt(int dateFormatAt) {
		this.dateFormatAt = dateFormatAt; 
	}

	public int getDateFormatAt() {
		return dateFormatAt; 
	}
	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat; 
	}

	public String getDateFormat() {
		return dateFormat; 
	}
	public void setPrimaryKeyFlag(String primaryKeyFlag) {
		this.primaryKeyFlag = primaryKeyFlag; 
	}

	public String getPrimaryKeyFlag() {
		return primaryKeyFlag; 
	}
	public void setPartitionColumnFlag(String partitionColumnFlag) {
		this.partitionColumnFlag = partitionColumnFlag; 
	}

	public String getPartitionColumnFlag() {
		return partitionColumnFlag; 
	}
	public void setFeedColumnStatusNt(int feedColumnStatusNt) {
		this.feedColumnStatusNt = feedColumnStatusNt; 
	}

	public int getFeedColumnStatusNt() {
		return feedColumnStatusNt; 
	}
	public void setFeedColumnStatus(int feedColumnStatus) {
		this.feedColumnStatus = feedColumnStatus; 
	}

	public int getFeedColumnStatus() {
		return feedColumnStatus; 
	}


}