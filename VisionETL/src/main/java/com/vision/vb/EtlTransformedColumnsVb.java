package com.vision.vb;

public class EtlTransformedColumnsVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String tranColumnId = "";
	private String tranColumnName = "";
	private String tranColumnDescription = "";
	private int tranColumnSortOrder = 0;
	private int columnDatatypeAt = 2104;
	private String columnDatatype = "";
	private int columnLength = 0;
	private String primaryKeyFlag = "N";
	private String dateKeyFlag = "N";
	private String partitionColumnFlag = "";
/*	private String filterContext = "";*/
	private int feedColumnStatusNt = 2000;
	private int feedColumnStatus = 0;
	
	/*private String sampleDataRule = "";
	private String sampleDataCustomRule= ""; */
	
	public EtlTransformedColumnsVb() {}

	public EtlTransformedColumnsVb(String country, String leBook, String feedId) {
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
	public void setTranColumnId(String tranColumnId) {
		this.tranColumnId = tranColumnId; 
	}

	public String getTranColumnId() {
		return tranColumnId; 
	}
	public void setTranColumnName(String tranColumnName) {
		this.tranColumnName = tranColumnName; 
	}

	public String getTranColumnName() {
		return tranColumnName; 
	}
	public void setTranColumnDescription(String tranColumnDescription) {
		this.tranColumnDescription = tranColumnDescription; 
	}

	public String getTranColumnDescription() {
		return tranColumnDescription; 
	}
	public void setTranColumnSortOrder(int tranColumnSortOrder) {
		this.tranColumnSortOrder = tranColumnSortOrder; 
	}

	public int getTranColumnSortOrder() {
		return tranColumnSortOrder; 
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

	public String getDateKeyFlag() {
		return dateKeyFlag;
	}

	public void setDateKeyFlag(String dateKeyFlag) {
		this.dateKeyFlag = dateKeyFlag;
	}

}