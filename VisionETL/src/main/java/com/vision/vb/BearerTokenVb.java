package com.vision.vb;

public class BearerTokenVb extends CommonVb {
	private String rSessionId = "";
	private String bSessionId = "";
	private String visionId = "";
	private String bearerToken = "";
	private String ipAddress = "";
	private String hostname = "";
	private String macAddress = "";
	private int tokenStatusNT = 8001;
	private int tokenStatus = 0;
	private String tokenCreatedDate = "";
	private String validTill = "";
	private String dateLastUtilized = "";
	private String applicationAccessAT = "";
	private String applicationAccess = "";
	private String comments = "";
	private int recordStatusNT = 1;
	private int recordStatus = 0;
	private int utilizationCount;

	public String getrSessionId() {
		return rSessionId;
	}

	public void setrSessionId(String rSessionId) {
		this.rSessionId = rSessionId;
	}

	public String getbSessionId() {
		return bSessionId;
	}

	public void setbSessionId(String bSessionId) {
		this.bSessionId = bSessionId;
	}

	public String getVisionId() {
		return visionId;
	}

	public void setVisionId(String visionId) {
		this.visionId = visionId;
	}

	public String getBearerToken() {
		return bearerToken;
	}

	public void setBearerToken(String bearerToken) {
		this.bearerToken = bearerToken;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getMacAddress() {
		return macAddress;
	}

	public void setMacAddress(String macAddress) {
		this.macAddress = macAddress;
	}

	public int getTokenStatusNT() {
		return tokenStatusNT;
	}

	public void setTokenStatusNT(int tokenStatusNT) {
		this.tokenStatusNT = tokenStatusNT;
	}

	public int getTokenStatus() {
		return tokenStatus;
	}

	public void setTokenStatus(int tokenStatus) {
		this.tokenStatus = tokenStatus;
	}

	public String getTokenCreatedDate() {
		return tokenCreatedDate;
	}

	public void setTokenCreatedDate(String tokenCreatedDate) {
		this.tokenCreatedDate = tokenCreatedDate;
	}

	public String getValidTill() {
		return validTill;
	}

	public void setValidTill(String validTill) {
		this.validTill = validTill;
	}

	public String getDateLastUtilized() {
		return dateLastUtilized;
	}

	public void setDateLastUtilized(String dateLastUtilized) {
		this.dateLastUtilized = dateLastUtilized;
	}

	public String getApplicationAccessAT() {
		return applicationAccessAT;
	}

	public void setApplicationAccessAT(String applicationAccessAT) {
		this.applicationAccessAT = applicationAccessAT;
	}

	public String getApplicationAccess() {
		return applicationAccess;
	}

	public void setApplicationAccess(String applicationAccess) {
		this.applicationAccess = applicationAccess;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public int getRecordStatusNT() {
		return recordStatusNT;
	}

	public void setRecordStatusNT(int recordStatusNT) {
		this.recordStatusNT = recordStatusNT;
	}

	public int getRecordStatus() {
		return recordStatus;
	}

	public void setRecordStatus(int recordStatus) {
		this.recordStatus = recordStatus;
	}

	public int getUtilizationCount() {
		return utilizationCount;
	}

	public void setUtilizationCount(int utilizationCount) {
		this.utilizationCount = utilizationCount;
	}
}
