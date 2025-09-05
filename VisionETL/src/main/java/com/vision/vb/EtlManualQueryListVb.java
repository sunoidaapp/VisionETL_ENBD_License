package com.vision.vb;

import java.util.List;

public class EtlManualQueryListVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String connectorId = "";
	private String queryId = "";
	private String queryDescription = "";
	private String preScript1 = "";
	private String preScript2 = "";
	private String preScript3 = "";
	private String sqlQuery1 = "";
	private String sqlQuery2 = "";
	private String sqlQuery3 = "";
	private String validatedFlag = "";
	private int queryStatusNt = 0 ;
	private int queryStatus = 0 ;
	public List<SmartSearchVb> smartSearchOpt=null;
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
	public void setQueryDescription(String queryDescription) {
		this.queryDescription = queryDescription; 
	}

	public String getQueryDescription() {
		return queryDescription; 
	}
	public void setPreScript1(String preScript1) {
		this.preScript1 = preScript1; 
	}

	public String getPreScript1() {
		return preScript1; 
	}
	public void setPreScript2(String preScript2) {
		this.preScript2 = preScript2; 
	}

	public String getPreScript2() {
		return preScript2; 
	}
	public void setPreScript3(String preScript3) {
		this.preScript3 = preScript3; 
	}

	public String getPreScript3() {
		return preScript3; 
	}
	public void setSqlQuery1(String sqlQuery1) {
		this.sqlQuery1 = sqlQuery1; 
	}

	public String getSqlQuery1() {
		return sqlQuery1; 
	}
	public void setSqlQuery2(String sqlQuery2) {
		this.sqlQuery2 = sqlQuery2; 
	}

	public String getSqlQuery2() {
		return sqlQuery2; 
	}
	public void setSqlQuery3(String sqlQuery3) {
		this.sqlQuery3 = sqlQuery3; 
	}

	public String getSqlQuery3() {
		return sqlQuery3; 
	}
	public void setValidatedFlag(String validatedFlag) {
		this.validatedFlag = validatedFlag; 
	}

	public String getValidatedFlag() {
		return validatedFlag; 
	}

	public int getQueryStatusNt() {
		return queryStatusNt;
	}

	public void setQueryStatusNt(int queryStatusNt) {
		this.queryStatusNt = queryStatusNt;
	}

	public int getQueryStatus() {
		return queryStatus;
	}

	public void setQueryStatus(int queryStatus) {
		this.queryStatus = queryStatus;
	}

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}


}