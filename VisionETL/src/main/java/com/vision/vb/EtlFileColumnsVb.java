package com.vision.vb;

public class EtlFileColumnsVb extends CommonVb {

	private String connectorId = "";
	private String viewName = "";
	private int columnId = 0;
	private String columnName = "";
	private int columnSortOrder = 0;
	private int columnDatatypeAt = 2014;
	private String columnDatatype = "";
	private int columnLength = 0;
	private int dateFormatNt = 1103;
	private String dateFormat = "";
	private int numberFormatNt = 1102;
	private String numberFormat = "";
	private String primaryKeyFlag = "";
	private String partitionColumnFlag = "";
	private int fileColumnStatusNt = 1;
	private int fileColumnStatus = 0;

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public String getConnectorId() {
		return connectorId;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	public String getViewName() {
		return viewName;
	}

	public void setColumnId(int columnId) {
		this.columnId = columnId;
	}

	public int getColumnId() {
		return columnId;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnSortOrder(int columnSortOrder) {
		this.columnSortOrder = columnSortOrder;
	}

	public int getColumnSortOrder() {
		return columnSortOrder;
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

	public void setDateFormatNt(int dateFormatNt) {
		this.dateFormatNt = dateFormatNt;
	}

	public int getDateFormatNt() {
		return dateFormatNt;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setNumberFormatNt(int numberFormatNt) {
		this.numberFormatNt = numberFormatNt;
	}

	public int getNumberFormatNt() {
		return numberFormatNt;
	}

	public void setNumberFormat(String numberFormat) {
		this.numberFormat = numberFormat;
	}

	public String getNumberFormat() {
		return numberFormat;
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

	public void setFileColumnStatusNt(int fileColumnStatusNt) {
		this.fileColumnStatusNt = fileColumnStatusNt;
	}

	public int getFileColumnStatusNt() {
		return fileColumnStatusNt;
	}

	public void setFileColumnStatus(int fileColumnStatus) {
		this.fileColumnStatus = fileColumnStatus;
	}

	public int getFileColumnStatus() {
		return fileColumnStatus;
	}

}