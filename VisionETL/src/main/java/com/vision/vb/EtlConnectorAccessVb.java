package com.vision.vb;

import java.util.List;

public class EtlConnectorAccessVb extends CommonVb{

	private String connectorId = "";
	private int userGroupAt = 1 ;
	private String userGroup = "";
	private int userProfileAt = 2 ;
	private String userProfile = "";
	private int visionId ;
	private String writeFlag = "";
	private int dataConnectorStatusNt = 1 ;
	private int dataConnectorStatus = -1 ;
	List<SmartSearchVb> smartSearchOpt = null;

	public void setconnectorId(String connectorId) {
		this.connectorId = connectorId; 
	}

	public String getconnectorId() {
		return connectorId; 
	}
	public void setUserGroupAt(int userGroupAt) {
		this.userGroupAt = userGroupAt; 
	}

	public int getUserGroupAt() {
		return userGroupAt; 
	}
	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup; 
	}

	public String getUserGroup() {
		return userGroup; 
	}
	public void setUserProfileAt(int userProfileAt) {
		this.userProfileAt = userProfileAt; 
	}

	public int getUserProfileAt() {
		return userProfileAt; 
	}
	public void setUserProfile(String userProfile) {
		this.userProfile = userProfile; 
	}

	public String getUserProfile() {
		return userProfile; 
	}
	public void setVisionId(int visionId) {
		this.visionId = visionId; 
	}

	public int getVisionId() {
		return visionId; 
	}
	public void setWriteFlag(String writeFlag) {
		this.writeFlag = writeFlag; 
	}

	public String getWriteFlag() {
		return writeFlag; 
	}

	public String getConnectorId() {
		return connectorId;
	}

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public int getDataConnectorStatusNt() {
		return dataConnectorStatusNt;
	}

	public void setDataConnectorStatusNt(int dataConnectorStatusNt) {
		this.dataConnectorStatusNt = dataConnectorStatusNt;
	}

	public int getDataConnectorStatus() {
		return dataConnectorStatus;
	}

	public void setDataConnectorStatus(int dataConnectorStatus) {
		this.dataConnectorStatus = dataConnectorStatus;
	}

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

}