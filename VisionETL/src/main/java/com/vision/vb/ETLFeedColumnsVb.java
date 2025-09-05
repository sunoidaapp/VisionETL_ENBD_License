package com.vision.vb;

public class ETLFeedColumnsVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String sourceConnectorId = "";
	private String tableId = "";
	private String columnId = "";
	private String columnName = "";
	private String columnAliasName = "";

	private int columnSortOrder = 0;
	private int columnDataTypeAT = 2104;
	private String columnDataType = "";
	private String columnLength = "";
	private int dateFormatNT = 1103;
	private String dateFormat = "";
	public String formatTypeDesc = "";
	private String javaFormatDesc = "";
	private String dateFormattingSyntax = "";
	private String dateConversionSyntax = "";

	private String primaryKeyFlag = "N";
	private String partitionColumnFlag = "N";
	private String filterContext = "";
	private int feedColumnStatusNT = 2000;
	private int feedColumnStatus = 0;

	private int colExperssionTypeAt = 2003;
	private String colExperssionType = "";
	private String experssionText = "";

	public ETLFeedColumnsVb() {
	}

	public ETLFeedColumnsVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
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

	public String getFeedId() {
		return feedId;
	}

	public void setFeedId(String feedId) {
		this.feedId = feedId;
	}

	public String getSourceConnectorId() {
		return sourceConnectorId;
	}

	public void setSourceConnectorId(String sourceConnectorId) {
		this.sourceConnectorId = sourceConnectorId;
	}

	public String getTableId() {
		return tableId;
	}

	public void setTableId(String tableId) {
		this.tableId = tableId;
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

	public String getColumnAliasName() {
		return columnAliasName;
	}

	public void setColumnAliasName(String columnAliasName) {
		this.columnAliasName = columnAliasName;
	}

	public int getColumnSortOrder() {
		return columnSortOrder;
	}

	public void setColumnSortOrder(int columnSortOrder) {
		this.columnSortOrder = columnSortOrder;
	}

	public int getColumnDataTypeAT() {
		return columnDataTypeAT;
	}

	public void setColumnDataTypeAT(int columnDataTypeAT) {
		this.columnDataTypeAT = columnDataTypeAT;
	}

	public String getColumnDataType() {
		return columnDataType;
	}

	public void setColumnDataType(String columnDataType) {
		this.columnDataType = columnDataType;
	}

	public String getColumnLength() {
		return columnLength;
	}

	public void setColumnLength(String columnLength) {
		this.columnLength = columnLength;
	}

	public int getDateFormatNT() {
		return dateFormatNT;
	}

	public void setDateFormatNT(int dateFormatNT) {
		this.dateFormatNT = dateFormatNT;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getPrimaryKeyFlag() {
		return primaryKeyFlag;
	}

	public void setPrimaryKeyFlag(String primaryKeyFlag) {
		this.primaryKeyFlag = primaryKeyFlag;
	}

	public String getPartitionColumnFlag() {
		return partitionColumnFlag;
	}

	public void setPartitionColumnFlag(String partitionColumnFlag) {
		this.partitionColumnFlag = partitionColumnFlag;
	}

	public String getFilterContext() {
		return filterContext;
	}

	public void setFilterContext(String filterContext) {
		this.filterContext = filterContext;
	}

	public int getFeedColumnStatusNT() {
		return feedColumnStatusNT;
	}

	public void setFeedColumnStatusNT(int feedColumnStatusNT) {
		this.feedColumnStatusNT = feedColumnStatusNT;
	}

	public int getFeedColumnStatus() {
		return feedColumnStatus;
	}

	public void setFeedColumnStatus(int feedColumnStatus) {
		this.feedColumnStatus = feedColumnStatus;
	}

	public int getColExperssionTypeAt() {
		return colExperssionTypeAt;
	}

	public void setColExperssionTypeAt(int colExperssionTypeAt) {
		this.colExperssionTypeAt = colExperssionTypeAt;
	}

	public String getColExperssionType() {
		return colExperssionType;
	}

	public void setColExperssionType(String colExperssionType) {
		this.colExperssionType = colExperssionType;
	}

	public String getExperssionText() {
		return experssionText;
	}

	public void setExperssionText(String experssionText) {
		this.experssionText = experssionText;
	}

	public String getFormatTypeDesc() {
		return formatTypeDesc;
	}

	public void setFormatTypeDesc(String formatTypeDesc) {
		this.formatTypeDesc = formatTypeDesc;
	}

	public String getJavaFormatDesc() {
		return javaFormatDesc;
	}

	public void setJavaFormatDesc(String javaFormatDesc) {
		this.javaFormatDesc = javaFormatDesc;
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

}