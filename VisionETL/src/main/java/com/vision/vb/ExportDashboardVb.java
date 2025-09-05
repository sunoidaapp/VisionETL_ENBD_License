package com.vision.vb;

import java.util.List;

public class ExportDashboardVb {
	private String dashboardId = "";
	private String dashboardName = "";
	private String dashboardDesc = "";
	private List<ExportWidgetVb> children = null;
	
	private String reportId = "";
	private String subReportId = "";
	private String gridLevel = "";
	private String sessionId = "";
	private int maxColspan = 0;
	private int levelIndexCt = 0;

	public String getDashboardId() {
		return dashboardId;
	}
	public void setDashboardId(String dashboardId) {
		this.dashboardId = dashboardId;
	}
	public String getDashboardName() {
		return dashboardName;
	}
	public void setDashboardName(String dashboardName) {
		this.dashboardName = dashboardName;
	}
	public String getDashboardDesc() {
		return dashboardDesc;
	}
	public void setDashboardDesc(String dashboardDesc) {
		this.dashboardDesc = dashboardDesc;
	}
	public List<ExportWidgetVb> getChildren() {
		return children;
	}
	public void setChildren(List<ExportWidgetVb> children) {
		this.children = children;
	}
	public String getReportId() {
		return reportId;
	}
	public void setReportId(String reportId) {
		this.reportId = reportId;
	}
	public String getSubReportId() {
		return subReportId;
	}
	public void setSubReportId(String subReportId) {
		this.subReportId = subReportId;
	}
	public String getGridLevel() {
		return gridLevel;
	}
	public void setGridLevel(String gridLevel) {
		this.gridLevel = gridLevel;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public int getMaxColspan() {
		return maxColspan;
	}
	public void setMaxColspan(int maxColspan) {
		this.maxColspan = maxColspan;
	}
	public int getLevelIndexCt() {
		return levelIndexCt;
	}
	public void setLevelIndexCt(int levelIndexCt) {
		this.levelIndexCt = levelIndexCt;
	}
	
	
}
