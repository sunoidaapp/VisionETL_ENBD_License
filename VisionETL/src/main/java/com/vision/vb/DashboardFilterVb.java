package com.vision.vb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class DashboardFilterVb implements Serializable{

	private static final long serialVersionUID = 1L;
	private String filterRefCode = "";
	private String filterRefXml = "";
	private String filterLabel = "";
	private String filterType = "";
	private String filterDateFormat = "";
	private String filterDefaultValue = "";
	private String filterSourceId = "";
	private int filterCnt = 0;
	private String specificTab = "";
	LinkedHashMap<String,String> filterSourceVal = new LinkedHashMap<String,String>();
	private String multiSelect = "";
	
	public String getFilterRefCode() {
		return filterRefCode;
	}
	public void setFilterRefCode(String filterRefCode) {
		this.filterRefCode = filterRefCode;
	}
	public String getFilterRefXml() {
		return filterRefXml;
	}
	public void setFilterRefXml(String filterRefXml) {
		this.filterRefXml = filterRefXml;
	}
	public String getFilterLabel() {
		return filterLabel;
	}
	public void setFilterLabel(String filterLabel) {
		this.filterLabel = filterLabel;
	}
	public String getFilterType() {
		return filterType;
	}
	public void setFilterType(String filterType) {
		this.filterType = filterType;
	}
	public LinkedHashMap<String, String> getFilterSourceVal() {
		return filterSourceVal;
	}
	public void setFilterSourceVal(LinkedHashMap<String, String> filterSourceVal) {
		this.filterSourceVal = filterSourceVal;
	}
	public int getFilterCnt() {
		return filterCnt;
	}
	public void setFilterCnt(int filterCnt) {
		this.filterCnt = filterCnt;
	}
	public String getFilterDateFormat() {
		return filterDateFormat;
	}
	public void setFilterDateFormat(String filterDateFormat) {
		this.filterDateFormat = filterDateFormat;
	}
	public String getFilterDefaultValue() {
		return filterDefaultValue;
	}
	public void setFilterDefaultValue(String filterDefaultValue) {
		this.filterDefaultValue = filterDefaultValue;
	}
	public String getFilterSourceId() {
		return filterSourceId;
	}
	public void setFilterSourceId(String filterSourceId) {
		this.filterSourceId = filterSourceId;
	}
	public String getSpecificTab() {
		return specificTab;
	}
	public void setSpecificTab(String specificTab) {
		this.specificTab = specificTab;
	}
	public String getMultiSelect() {
		return multiSelect;
	}
	public void setMultiSelect(String multiSelect) {
		this.multiSelect = multiSelect;
	}
}
