package com.vision.vb;

import java.io.Serializable;
import java.util.List;


public class DashboardVb extends CommonVb implements Serializable{

	private static final long serialVersionUID = 1L;
	private String dashboardId = "";
	private String dashboardName = "";
	private String totalTabs = "";
	private String dataSourceRef = "";
	private String filterFlag = "";
	private String filterRefCode = "";
	private int	dashboar_ThemeAT = 0;
	private String dashboard_Theme = "";
	
	private List<DashboardFilterVb> dashboardFilterlst = null; 
	private List<DashboardTabVb> dashboardTabs = null;

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

	public List<DashboardTabVb> getDashboardTabs() {
		return dashboardTabs;
	}

	public void setDashboardTabs(List<DashboardTabVb> dashboardTabs) {
		this.dashboardTabs = dashboardTabs;
	}
	public String getTotalTabs() {
		return totalTabs;
	}

	public void setTotalTabs(String totalTabs) {
		this.totalTabs = totalTabs;
	}

	public String getDataSourceRef() {
		return dataSourceRef;
	}

	public void setDataSourceRef(String dataSourceRef) {
		this.dataSourceRef = dataSourceRef;
	}

	public String getFilterFlag() {
		return filterFlag;
	}

	public void setFilterFlag(String filterFlag) {
		this.filterFlag = filterFlag;
	}

	public String getFilterRefCode() {
		return filterRefCode;
	}

	public void setFilterRefCode(String filterRefCode) {
		this.filterRefCode = filterRefCode;
	}

	public List<DashboardFilterVb> getDashboardFilterlst() {
		return dashboardFilterlst;
	}

	public void setDashboardFilterlst(List<DashboardFilterVb> dashboardFilterlst) {
		this.dashboardFilterlst = dashboardFilterlst;
	}

	public int getDashboar_ThemeAT() {
		return dashboar_ThemeAT;
	}

	public void setDashboar_ThemeAT(int dashboar_ThemeAT) {
		this.dashboar_ThemeAT = dashboar_ThemeAT;
	}

	public String getDashboard_Theme() {
		return dashboard_Theme;
	}

	public void setDashboard_Theme(String dashboard_Theme) {
		this.dashboard_Theme = dashboard_Theme;
	}

	
	
}
