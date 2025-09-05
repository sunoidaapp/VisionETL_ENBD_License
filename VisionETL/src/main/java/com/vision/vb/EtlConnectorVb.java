package com.vision.vb;

import java.util.List;

public class EtlConnectorVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private int connectorTypeAt = 2100 ;
	private String connectorType = "";
	private String connectorTypeDesc = "";
	private String connectorId = "";
	private String connectionName = "";
	private String connectorScripts = "";
	private int endpointTypeAt = 2101 ;
	private String endpointType = "";
	private String endpointTypeDesc = "";
	private int connectionStatusNt = 1 ; 
	private int connectionStatus = 0 ;
	private String tableName = "";
	private String tableType = "";
	private String dbLinkFlag = "N";
	private String feedType = "";

	
	private String macroVar = ""; 
	//private String macroVarScript = "";  // change to connectorScripts
	private int sortOrder = 1;
	private int scriptTypeAt=1083;
	private String scriptType;
	private String macroVarType = ""; 
	private String tagValue = "";
	private String tagName = "";
	private String displayName = "";
	private String maskedFlag = "";
	private String encryption = "";
	private String description = "";
	private Object[] dynamicScript;
	private String validFlag = "";
	private String mandatoryFlag = "N";
	private int tagTypeNt = 2011;
	private int tagType = 3;
	public List<SmartSearchVb> smartSearchOpt=null;
	private Integer tableSize=0;
	private String delimiter = "";
	private char headerCheck;
	private String fileName = "";
	private String extension = "";
	private String stgQuery1 = "";
	private String stgQuery2 = "";
	private String stgQuery3 = "";
	private String sqlQuery = "";
	private String postQuery = "";
	private String queryId = "";
	private List<EtlMqColumnsVb> etlMqColumnsList =null; // VALIDATE-API
	private List<EtlFileUploadAreaVb> children =null; // VALIDATE-API
	private String tableIndicator="";

	
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
	public void setConnectorTypeAt(int connectorTypeAt) {
		this.connectorTypeAt = connectorTypeAt; 
	}

	public int getConnectorTypeAt() {
		return connectorTypeAt; 
	}
	public void setConnectorType(String connectorType) {
		this.connectorType = connectorType; 
	}

	public String getConnectorType() {
		return connectorType; 
	}
	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId; 
	}

	public String getConnectorId() {
		return connectorId; 
	}
	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName; 
	}

	public String getConnectionName() {
		return connectionName; 
	}
	public void setConnectorScripts(String connectorScripts) {
		this.connectorScripts = connectorScripts; 
	}

	public String getConnectorScripts() {
		return connectorScripts; 
	}
	public void setEndpointTypeAt(int endpointTypeAt) {
		this.endpointTypeAt = endpointTypeAt; 
	}

	public int getEndpointTypeAt() {
		return endpointTypeAt; 
	}
	public void setEndpointType(String endpointType) {
		this.endpointType = endpointType; 
	}

	public String getEndpointType() {
		return endpointType; 
	}

	public String getConnectorTypeDesc() {
		return connectorTypeDesc;
	}

	public void setConnectorTypeDesc(String connectorTypeDesc) {
		this.connectorTypeDesc = connectorTypeDesc;
	}

	public String getEndpointTypeDesc() {
		return endpointTypeDesc;
	}

	public void setEndpointTypeDesc(String endpointTypeDesc) {
		this.endpointTypeDesc = endpointTypeDesc;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getMacroVar() {
		return macroVar;
	}

	public void setMacroVar(String macroVar) {
		this.macroVar = macroVar;
	}

	/*public String getMacroVarScript() {
		return macroVarScript;
	}

	public void setMacroVarScript(String macroVarScript) {
		this.macroVarScript = macroVarScript;
	}*/

	public int getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(int sortOrder) {
		this.sortOrder = sortOrder;
	}

	public int getScriptTypeAt() {
		return scriptTypeAt;
	}

	public void setScriptTypeAt(int scriptTypeAt) {
		this.scriptTypeAt = scriptTypeAt;
	}

	public String getScriptType() {
		return scriptType;
	}

	public void setScriptType(String scriptType) {
		this.scriptType = scriptType;
	}

	public String getMacroVarType() {
		return macroVarType;
	}

	public void setMacroVarType(String macroVarType) {
		this.macroVarType = macroVarType;
	}

	public String getTagValue() {
		return tagValue;
	}

	public void setTagValue(String tagValue) {
		this.tagValue = tagValue;
	}

	public String getTagName() {
		return tagName;
	}

	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getMaskedFlag() {
		return maskedFlag;
	}

	public void setMaskedFlag(String maskedFlag) {
		this.maskedFlag = maskedFlag;
	}

	public String getEncryption() {
		return encryption;
	}

	public void setEncryption(String encryption) {
		this.encryption = encryption;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Object[] getDynamicScript() {
		return dynamicScript;
	}

	public void setDynamicScript(Object[] dynamicScript) {
		this.dynamicScript = dynamicScript;
	}

	public String getValidFlag() {
		return validFlag;
	}

	public void setValidFlag(String validFlag) {
		this.validFlag = validFlag;
	}


	public String getMandatoryFlag() {
		return mandatoryFlag;
	}

	public void setMandatoryFlag(String mandatoryFlag) {
		this.mandatoryFlag = mandatoryFlag;
	}

	public int getTagTypeNt() {
		return tagTypeNt;
	}

	public void setTagTypeNt(int tagTypeNt) {
		this.tagTypeNt = tagTypeNt;
	}

	public int getTagType() {
		return tagType;
	}

	public void setTagType(int tagType) {
		this.tagType = tagType;
	}

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public char getHeaderCheck() {
		return headerCheck;
	}

	public void setHeaderCheck(char headerCheck) {
		this.headerCheck = headerCheck;
	}

	public Integer getTableSize() {
		return tableSize;
	}

	public void setTableSize(Integer tableSize) {
		this.tableSize = tableSize;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public int getConnectionStatusNt() {
		return connectionStatusNt;
	}

	public void setConnectionStatusNt(int connectionStatusNt) {
		this.connectionStatusNt = connectionStatusNt;
	}

	public int getConnectionStatus() {
		return connectionStatus;
	}

	public void setConnectionStatus(int connectionStatus) {
		this.connectionStatus = connectionStatus;
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

	public String getStgQuery1() {
		return stgQuery1;
	}

	public void setStgQuery1(String stgQuery1) {
		this.stgQuery1 = stgQuery1;
	}

	public String getStgQuery2() {
		return stgQuery2;
	}

	public void setStgQuery2(String stgQuery2) {
		this.stgQuery2 = stgQuery2;
	}

	public String getStgQuery3() {
		return stgQuery3;
	}

	public void setStgQuery3(String stgQuery3) {
		this.stgQuery3 = stgQuery3;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public String getPostQuery() {
		return postQuery;
	}

	public void setPostQuery(String postQuery) {
		this.postQuery = postQuery;
	}

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public List<EtlMqColumnsVb> getEtlMqColumnsList() {
		return etlMqColumnsList;
	}

	public void setEtlMqColumnsList(List<EtlMqColumnsVb> etlMqColumnsList) {
		this.etlMqColumnsList = etlMqColumnsList;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public String getDbLinkFlag() {
		return dbLinkFlag;
	}

	public void setDbLinkFlag(String dbLinkFlag) {
		this.dbLinkFlag = dbLinkFlag;
	}

	public String getFeedType() {
		return feedType;
	}

	public void setFeedType(String feedType) {
		this.feedType = feedType;
	}

	public List<EtlFileUploadAreaVb> getChildren() {
		return children;
	}

	public void setChildren(List<EtlFileUploadAreaVb> children) {
		this.children = children;
	}
	
	public String getTableIndicator() {
		return tableIndicator;
	}
	public void setTableIndicator(String tableIndicator) {
		this.tableIndicator = tableIndicator;
	}
}