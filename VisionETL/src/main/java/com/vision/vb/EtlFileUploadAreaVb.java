package com.vision.vb;

import java.util.List;

public class EtlFileUploadAreaVb extends CommonVb {

	private String connectorId = "";
	private String fileId = "";
	private String makerId="";
	private String fileName = "";
	private int fileFormatAt = 2116;
	private String fileFormat = "";
	private String fileFormatDesc = "";
	private int fileTypeAt = 2117;
	private String fileType = "";
	private String fileTypeDesc = "";
	private String ftpDetailScript = "";
	private String fileContext = "";
	private int fileStatusNt = 1;
	private int fileStatus = 0;
	public List<SmartSearchVb> smartSearchOpt = null;
	public List<EtlFileTableVb> fileTableList = null;


	/* Upload Details */

	private String hostName = "";
	private int port = 22;
	private String userName = "";
	private String password = "";
	private String fileLocation = "";  //FTP
	private String serverType = "";    // Windows/Unix [W/U] 
	private String connectionType = ""; // SFTP or FTP
	private Object[] dynamicScript;
	private String tagValue = "";
	private String tagName = "";
	private String displayName = "";
	private String maskedFlag = "";
	private String encryption = "";
	private String description = "";
	private String mandatoryFlag = "N";
	private int tagTypeNt = 2011;
	private int tagType = 3;
	

	/* Upload Details */

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public String getConnectorId() {
		return connectorId;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}

	public String getFileId() {
		return fileId;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileFormatAt(int fileFormatAt) {
		this.fileFormatAt = fileFormatAt;
	}

	public int getFileFormatAt() {
		return fileFormatAt;
	}

	public void setFileFormat(String fileFormat) {
		this.fileFormat = fileFormat;
	}

	public String getFileFormat() {
		return fileFormat;
	}

	public void setFileTypeAt(int fileTypeAt) {
		this.fileTypeAt = fileTypeAt;
	}

	public int getFileTypeAt() {
		return fileTypeAt;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFtpDetailScript(String ftpDetailScript) {
		this.ftpDetailScript = ftpDetailScript;
	}

	public String getFtpDetailScript() {
		return ftpDetailScript;
	}

	public void setFileStatusNt(int fileStatusNt) {
		this.fileStatusNt = fileStatusNt;
	}

	public int getFileStatusNt() {
		return fileStatusNt;
	}

	public void setFileStatus(int fileStatus) {
		this.fileStatus = fileStatus;
	}

	public int getFileStatus() {
		return fileStatus;
	}

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

	public String getFileFormatDesc() {
		return fileFormatDesc;
	}

	public void setFileFormatDesc(String fileFormatDesc) {
		this.fileFormatDesc = fileFormatDesc;
	}

	public String getFileTypeDesc() {
		return fileTypeDesc;
	}

	public void setFileTypeDesc(String fileTypeDesc) {
		this.fileTypeDesc = fileTypeDesc;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getFileLocation() {
		return fileLocation;
	}

	public void setFileLocation(String fileLocation) {
		this.fileLocation = fileLocation;
	}

	public Object[] getDynamicScript() {
		return dynamicScript;
	}

	public void setDynamicScript(Object[] dynamicScript) {
		this.dynamicScript = dynamicScript;
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

	public String getServerType() {
		return serverType;
	}

	public void setServerType(String serverType) {
		this.serverType = serverType;
	}

	public String getConnectionType() {
		return connectionType;
	}

	public void setConnectionType(String connectionType) {
		this.connectionType = connectionType;
	}

	public String getFileContext() {
		return fileContext;
	}

	public void setFileContext(String fileContext) {
		this.fileContext = fileContext;
	}

	public List<EtlFileTableVb> getFileTableList() {
		return fileTableList;
	}

	public void setFileTableList(List<EtlFileTableVb> fileTableList) {
		this.fileTableList = fileTableList;
	}

	public String getMakerId() {
		return makerId;
	}

	public void setMakerId(String makerId) {
		this.makerId = makerId;
	}
	
}