package com.vision.vb;

public class EtlFeedCategoryDepedencyVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String categoryId = "";
	private String dependentCategory = "";
	private String dependentCategoryDesc = "";
	private int categoryDepStatusNt = 1;
	private int categoryDepStatus =-1;

	
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
	public String getDependentCategory() {
		return dependentCategory;
	}
	public void setDependentCategory(String dependentCategory) {
		this.dependentCategory = dependentCategory;
	}
	public int getCategoryDepStatusNt() {
		return categoryDepStatusNt;
	}
	public void setCategoryDepStatusNt(int categoryDepStatusNt) {
		this.categoryDepStatusNt = categoryDepStatusNt;
	}
	public int getCategoryDepStatus() {
		return categoryDepStatus;
	}
	public void setCategoryDepStatus(int categoryDepStatus) {
		this.categoryDepStatus = categoryDepStatus;
	}
	public String getDependentCategoryDesc() {
		return dependentCategoryDesc;
	}
	public void setDependentCategoryDesc(String dependentCategoryDesc) {
		this.dependentCategoryDesc = dependentCategoryDesc;
	}
	

}