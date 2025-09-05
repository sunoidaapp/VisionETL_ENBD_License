
package com.vision.vb;

import java.util.List;

public class EtlFeedCategoryVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String categoryId = "";
	private String categoryDescription = "";
	private int categoryStatusNt = 1;
	private int categoryStatus =-1;
	List<SmartSearchVb> smartSearchOpt = null;
	List<EtlFeedCategoryDepedencyVb> etlCategoryDeplst =null;
	List<EtlFeedCategoryAlertConfigVb> etlCategoryAlertConfiglst =null;
	private String channelTypeSMS = "N";
	private String channelTypeEmail = "N";
	private String alertFlag = "N";


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

	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}

	public String getCategoryId() {
		return categoryId;
	}

	public void setCategoryDescription(String categoryDescription) {
		this.categoryDescription = categoryDescription;
	}

	public String getCategoryDescription() {
		return categoryDescription;
	}

	public void setCategoryStatusNt(int categoryStatusNt) {
		this.categoryStatusNt = categoryStatusNt;
	}

	public int getCategoryStatusNt() {
		return categoryStatusNt;
	}

	public void setCategoryStatus(int categoryStatus) {
		this.categoryStatus = categoryStatus;
	}

	public int getCategoryStatus() {
		return categoryStatus;
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

	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}

	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}

	public List<EtlFeedCategoryDepedencyVb> getEtlCategoryDeplst() {
		return etlCategoryDeplst;
	}

	public void setEtlCategoryDeplst(List<EtlFeedCategoryDepedencyVb> etlCategoryDeplst) {
		this.etlCategoryDeplst = etlCategoryDeplst;
	}

	public List<EtlFeedCategoryAlertConfigVb> getEtlCategoryAlertConfiglst() {
		return etlCategoryAlertConfiglst;
	}

	public void setEtlCategoryAlertConfiglst(List<EtlFeedCategoryAlertConfigVb> etlCategoryAlertConfiglst) {
		this.etlCategoryAlertConfiglst = etlCategoryAlertConfiglst;
	}

	public String getChannelTypeSMS() {
		return channelTypeSMS;
	}

	public void setChannelTypeSMS(String channelTypeSMS) {
		this.channelTypeSMS = channelTypeSMS;
	}

	public String getChannelTypeEmail() {
		return channelTypeEmail;
	}

	public void setChannelTypeEmail(String channelTypeEmail) {
		this.channelTypeEmail = channelTypeEmail;
	}

	public String getAlertFlag() {
		return alertFlag;
	}

	public void setAlertFlag(String alertFlag) {
		this.alertFlag = alertFlag;
	}

}