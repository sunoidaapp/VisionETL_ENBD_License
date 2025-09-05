package com.vision.vb;

import java.util.List;

public class ExportWidgetVb {
	private int cols;
	private int rows;
	private int x;
	private int y;
	private boolean showTitle;
	private String chartType = "";
	private String widgetId = "";
	private String queryId = "";
	private int subDashboardId;
	private String description = "";
	private String filterContext = "";
	private String filterContextJson = "";
	private String widgetContext = "";
	private String widgetContextJson = "";
	private String chartName = "";
	private List<WidgetFilterVb> widgetFilterList = null;
	private String dataViewName = "";
	private String dataSource = "";
	private boolean respFlag;
	private String imagePath = "";

	public int getCols() {
		return cols;
	}

	public void setCols(int cols) {
		this.cols = cols;
	}

	public int getRows() {
		return rows;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}

	public int getX() {
		return x;
	}

	public void setX(int x) {
		this.x = x;
	}

	public int getY() {
		return y;
	}

	public void setY(int y) {
		this.y = y;
	}

	public boolean isShowTitle() {
		return showTitle;
	}

	public void setShowTitle(boolean showTitle) {
		this.showTitle = showTitle;
	}

	public String getChartType() {
		return chartType;
	}

	public void setChartType(String chartType) {
		this.chartType = chartType;
	}

	public String getWidgetId() {
		return widgetId;
	}

	public void setWidgetId(String widgetId) {
		this.widgetId = widgetId;
	}

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public int getSubDashboardId() {
		return subDashboardId;
	}

	public void setSubDashboardId(int subDashboardId) {
		this.subDashboardId = subDashboardId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getFilterContext() {
		return filterContext;
	}

	public void setFilterContext(String filterContext) {
		this.filterContext = filterContext;
	}

	public String getFilterContextJson() {
		return filterContextJson;
	}

	public void setFilterContextJson(String filterContextJson) {
		this.filterContextJson = filterContextJson;
	}

	public String getWidgetContext() {
		return widgetContext;
	}

	public void setWidgetContext(String widgetContext) {
		this.widgetContext = widgetContext;
	}

	public String getWidgetContextJson() {
		return widgetContextJson;
	}

	public void setWidgetContextJson(String widgetContextJson) {
		this.widgetContextJson = widgetContextJson;
	}

	public String getChartName() {
		return chartName;
	}

	public void setChartName(String chartName) {
		this.chartName = chartName;
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

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public boolean isRespFlag() {
		return respFlag;
	}

	public void setRespFlag(boolean respFlag) {
		this.respFlag = respFlag;
	}

	public String getImagePath() {
		return imagePath;
	}

	public void setImagePath(String imagePath) {
		this.imagePath = imagePath;
	}

}