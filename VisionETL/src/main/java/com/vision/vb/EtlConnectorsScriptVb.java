package com.vision.vb;

import java.util.List;

public class EtlConnectorsScriptVb extends CommonVb{


	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";

	private String connectorId = "";
	private String scriptId = "";
	private String scriptDesc = "";
	private int scriptTypeAt = 2122;
	private String scriptType = "";
	private String scriptTypeDesc = "";
	private int executionTypeAt = 2123;
	private String executionType = "";
	private String executionTypeDesc = "";
	private String validatedFlag = "N";
	private String script = "";
	private int scriptStatusNt = 1; 
	private int scriptStatus = 0;
	public List<SmartSearchVb> smartSearchOpt=null;

	
	
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
	public String getScriptTypeDesc() {
		return scriptTypeDesc;
	}
	public void setScriptTypeDesc(String scriptTypeDesc) {
		this.scriptTypeDesc = scriptTypeDesc;
	}
	public String getExecutionTypeDesc() {
		return executionTypeDesc;
	}
	public void setExecutionTypeDesc(String executionTypeDesc) {
		this.executionTypeDesc = executionTypeDesc;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
	public String getConnectorId() {
		return connectorId;
	}
	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}
	public String getScriptId() {
		return scriptId;
	}
	public void setScriptId(String scriptId) {
		this.scriptId = scriptId;
	}
	public String getScriptDesc() {
		return scriptDesc;
	}
	public void setScriptDesc(String scriptDesc) {
		this.scriptDesc = scriptDesc;
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
	public int getExecutionTypeAt() {
		return executionTypeAt;
	}
	public void setExecutionTypeAt(int executionTypeAt) {
		this.executionTypeAt = executionTypeAt;
	}
	public String getExecutionType() {
		return executionType;
	}
	public void setExecutionType(String executionType) {
		this.executionType = executionType;
	}
	public String getValidatedFlag() {
		return validatedFlag;
	}
	public void setValidatedFlag(String validatedFlag) {
		this.validatedFlag = validatedFlag;
	}
	public String getScript() {
		return script;
	}
	public void setScript(String script) {
		this.script = script;
	}
	public int getScriptStatusNt() {
		return scriptStatusNt;
	}
	public void setScriptStatusNt(int scriptStatusNt) {
		this.scriptStatusNt = scriptStatusNt;
	}
	public int getScriptStatus() {
		return scriptStatus;
	}
	public void setScriptStatus(int scriptStatus) {
		this.scriptStatus = scriptStatus;
	}
	
	}
