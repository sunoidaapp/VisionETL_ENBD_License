package com.vision.vb;

import java.util.List;

public class EtlMqTableVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";

	private String connectorId = "";
	private String queryId = "";
	private String queryDescription = "";
	private String query = "";
	private String validatedFlag = "N";
	private int queryCategoryNt = 2008;
	private int queryCategory = 0;
	private int queryType = 0;
	private String queryCategoryNtDesc = "";
	private int queryStatusNt = 1;
	private int queryStatus = 0 ;
	
	public List<SmartSearchVb> smartSearchOpt=null;
	private List<EtlMqColumnsVb> etlMqColumnsList =null;
	
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
	public void setQuery(String query) {
		this.query = query; 
	}

	public String getQuery() {
		return query; 
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

	public List<EtlMqColumnsVb> getEtlMqColumnsList() {
		return etlMqColumnsList;
	}

	public void setEtlMqColumnsList(List<EtlMqColumnsVb> etlMqColumnsList) {
		this.etlMqColumnsList = etlMqColumnsList;
	}

	public int getQueryCategoryNt() {
		return queryCategoryNt;
	}

	public void setQueryCategoryNt(int queryCategoryNt) {
		this.queryCategoryNt = queryCategoryNt;
	}

	public int getQueryCategory() {
		return queryCategory;
	}

	public void setQueryCategory(int queryCategory) {
		this.queryCategory = queryCategory;
	}

	public String getQueryCategoryNtDesc() {
		return queryCategoryNtDesc;
	}

	public void setQueryCategoryNtDesc(String queryCategoryNtDesc) {
		this.queryCategoryNtDesc = queryCategoryNtDesc;
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

	public String getCountryDesc() {
		return countryDesc;
	}

	public void setCountryDesc(String countryDesc) {
		this.countryDesc = countryDesc;
	}

	public String getLeBookDesc() {
		return leBookDesc;
	}

	public void setLeBookDesc(String leBookDesc) {
		this.leBookDesc = leBookDesc;
	}

	public int getQueryType() {
		return queryType;
	}

	public void setQueryType(int queryType) {
		this.queryType = queryType;
	}
	
}