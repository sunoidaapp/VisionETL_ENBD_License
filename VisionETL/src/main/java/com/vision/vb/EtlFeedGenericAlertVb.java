package com.vision.vb;

import java.util.List;

public class EtlFeedGenericAlertVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String categoryId = "";
	private String lvl1AlertMailId = "";
	private String lvl1AlertDescription = "";

	private String lvl2AlertMailId = "";
	private String lvl2AlertDescription = "";

	private String lvl3AlertMailId = "";
	private String lvl3AlertDescription = "";

	private String terminatedByFlag = "N";
	private String reinitiatedByFlag = "N";
	private String completionStatusFlag = "N";
	private String startTimeFlag = "N";
	private String endTimeFlag = "N";
	
	private int etlAuditStatusNt = 1;
	private int etlAuditStatus =-1;
	List<SmartSearchVb> smartSearchOpt = null;
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
	public String getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}
	public String getLvl1AlertMailId() {
		return lvl1AlertMailId;
	}
	public void setLvl1AlertMailId(String lvl1AlertMailId) {
		this.lvl1AlertMailId = lvl1AlertMailId;
	}
	public String getLvl1AlertDescription() {
		return lvl1AlertDescription;
	}
	public void setLvl1AlertDescription(String lvl1AlertDescription) {
		this.lvl1AlertDescription = lvl1AlertDescription;
	}
	public String getLvl2AlertMailId() {
		return lvl2AlertMailId;
	}
	public void setLvl2AlertMailId(String lvl2AlertMailId) {
		this.lvl2AlertMailId = lvl2AlertMailId;
	}
	public String getLvl2AlertDescription() {
		return lvl2AlertDescription;
	}
	public void setLvl2AlertDescription(String lvl2AlertDescription) {
		this.lvl2AlertDescription = lvl2AlertDescription;
	}
	public String getLvl3AlertMailId() {
		return lvl3AlertMailId;
	}
	public void setLvl3AlertMailId(String lvl3AlertMailId) {
		this.lvl3AlertMailId = lvl3AlertMailId;
	}
	public String getLvl3AlertDescription() {
		return lvl3AlertDescription;
	}
	public void setLvl3AlertDescription(String lvl3AlertDescription) {
		this.lvl3AlertDescription = lvl3AlertDescription;
	}
	public String getTerminatedByFlag() {
		return terminatedByFlag;
	}
	public void setTerminatedByFlag(String terminatedByFlag) {
		this.terminatedByFlag = terminatedByFlag;
	}
	public String getReinitiatedByFlag() {
		return reinitiatedByFlag;
	}
	public void setReinitiatedByFlag(String reinitiatedByFlag) {
		this.reinitiatedByFlag = reinitiatedByFlag;
	}
	public String getCompletionStatusFlag() {
		return completionStatusFlag;
	}
	public void setCompletionStatusFlag(String completionStatusFlag) {
		this.completionStatusFlag = completionStatusFlag;
	}
	public String getStartTimeFlag() {
		return startTimeFlag;
	}
	public void setStartTimeFlag(String startTimeFlag) {
		this.startTimeFlag = startTimeFlag;
	}
	public String getEndTimeFlag() {
		return endTimeFlag;
	}
	public void setEndTimeFlag(String endTimeFlag) {
		this.endTimeFlag = endTimeFlag;
	}
	public int getEtlAuditStatusNt() {
		return etlAuditStatusNt;
	}
	public void setEtlAuditStatusNt(int etlAuditStatusNt) {
		this.etlAuditStatusNt = etlAuditStatusNt;
	}
	public int getEtlAuditStatus() {
		return etlAuditStatus;
	}
	public void setEtlAuditStatus(int etlAuditStatus) {
		this.etlAuditStatus = etlAuditStatus;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}


}