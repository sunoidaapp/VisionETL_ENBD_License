package com.vision.vb;

import java.util.List;

public class ETLForQueryReportFieldsWrapperVb {

	private EtlForQueryReportVb mainModel;
	private List<EtlExtractionSummaryFieldsVb> reportFields  = null;
	private String[] hashArray = null;
	private String[] hashValueArray = null;
	private List<WidgetFilterVb> widgetFilterList = null;
	private String dataViewName = null;
	
	public EtlForQueryReportVb getMainModel() {
		return mainModel;
	}
	public void setMainModel(EtlForQueryReportVb mainModel) {
		this.mainModel = mainModel;
	}
	public List<EtlExtractionSummaryFieldsVb> getReportFields() {
		return reportFields;
	}
	public void setReportFields(List<EtlExtractionSummaryFieldsVb> reportFields) {
		this.reportFields = reportFields;
	}
	public String[] getHashArray() {
		return hashArray;
	}
	public void setHashArray(String[] hashArray) {
		this.hashArray = hashArray;
	}
	public String[] getHashValueArray() {
		return hashValueArray;
	}
	public void setHashValueArray(String[] hashValueArray) {
		this.hashValueArray = hashValueArray;
	}

	public List<WidgetFilterVb> getWidgetFilterList() {
		return widgetFilterList;
	}

	public void setWidgetFilterList(List<WidgetFilterVb> widgetFilterList) {
		this.widgetFilterList = widgetFilterList;
	}
	public String getDataViewName() {
		return dataViewName;
	}
	public void setDataViewName(String dataViewName) {
		this.dataViewName = dataViewName;
	}
}