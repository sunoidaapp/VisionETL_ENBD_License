package com.vision.vb;

import java.util.List;

public class EtlMqColumnsVb extends CommonVb {

	private String connectorId = "";
	private String queryId = "";
	private int columnId = 0;
	private String columnName = "";
	private int columnSortOrder = 0;
	private int columnDatatypeAt = 2014;
	private String columnDatatype = "";
	private int columnLength = 0;
	private int dateFormatNt = 1103;
	private int dateFormat;
	private int numberFormatNt = 1102;
	private int numberFormat;
	private String dateFormatNtDesc = "";
	private String numberFormatNtDesc = "";
	private String primaryKeyFlag = "";
	private String partitionColumnFlag = "";
	private int mqColumnStatusNt = 1;
	private int mqColumnStatus = 0;
	public List<SmartSearchVb> smartSearchOpt = null;

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public String getConnectorId() {
		return connectorId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public String getQueryId() {
		return queryId;
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

	public void setDateFormat(int dateFormat) {
		this.dateFormat = dateFormat;
	}

	public int getDateFormat() {
		return dateFormat;
	}

	public void setNumberFormat(int numberFormat) {
		this.numberFormat = numberFormat;
	}

	public int getNumberFormat() {
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

	public int getMqColumnStatusNt() {
		return mqColumnStatusNt;
	}

	public void setMqColumnStatusNt(int mqColumnStatusNt) {
		this.mqColumnStatusNt = mqColumnStatusNt;
	}

	public int getMqColumnStatus() {
		return mqColumnStatus;
	}

	public void setMqColumnStatus(int mqColumnStatus) {
		this.mqColumnStatus = mqColumnStatus;
	}

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

	public int getDateFormatNt() {
		return dateFormatNt;
	}

	public void setDateFormatNt(int dateFormatNt) {
		this.dateFormatNt = dateFormatNt;
	}

	public int getNumberFormatNt() {
		return numberFormatNt;
	}

	public void setNumberFormatNt(int numberFormatNt) {
		this.numberFormatNt = numberFormatNt;
	}

	public String getDateFormatNtDesc() {
		return dateFormatNtDesc;
	}

	public void setDateFormatNtDesc(String dateFormatNtDesc) {
		this.dateFormatNtDesc = dateFormatNtDesc;
	}

	public String getNumberFormatNtDesc() {
		return numberFormatNtDesc;
	}

	public void setNumberFormatNtDesc(String numberFormatNtDesc) {
		this.numberFormatNtDesc = numberFormatNtDesc;
	}

}