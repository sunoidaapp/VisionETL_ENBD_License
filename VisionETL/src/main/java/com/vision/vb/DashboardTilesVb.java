package com.vision.vb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class DashboardTilesVb implements Serializable{

	private static final long serialVersionUID = 1L;
	private String dashboardId = "";
	private String tabId = "";
	private String tileId= "";
	private String tileCaption= "";
	private int tileSequence= 0;
	private int detailTileSequence= 0;
	private String tileType= "";
	private String columns= "";
	private Boolean drillDownFlag = false;
	private String queryId= "";
	private String drillDownRef= "";
	private String propId = "";
	private String propValue = "";
	private String chartType = "";
	private String propertyAttr = "";
	private int placeHolderCnt = 0;
	private String doubleWidthFlag ="N";
	private String subTiles = "N";
	private String subSequence = "1";
	String tileDataSet = "";
	String chartDataSet = "";
	List gridColumnSet = null;
	List gridDataSet = null;
	List gridColumnFormats = null;
	
	private String promptValue1 ="";
	private String promptValue2 ="";
	private String promptValue3 ="";
	private String promptValue4 ="";
	private String promptValue5 ="";
	private String promptValue6 ="";
	private String promptValue7 ="";
	private String promptValue8 ="";
	private String promptValue9 ="";
	private String promptValue10 ="";
	private List<DashboardTilesVb> childTileslst = new ArrayList<DashboardTilesVb>();
	private List<DashboardTilesVb> drillDownlst = new ArrayList<DashboardTilesVb>();
	
	private String subTileId= "";
	private List<DashboardTilesVb> subTileslst = new ArrayList<DashboardTilesVb>();
	private int	transitionEffectAT = 0;
	private String transitionEffect = "";
	private int	themeAT = 0;
	private String theme = "";
	
	public String getTileType() {
		return tileType;
	}
	public void setTileType(String tileType) {
		this.tileType = tileType;
	}
	public String getTileId() {
		return tileId;
	}
	public void setTileId(String tileId) {
		this.tileId = tileId;
	}
	public String getTileCaption() {
		return tileCaption;
	}
	public void setTileCaption(String tileCaption) {
		this.tileCaption = tileCaption;
	}
	public int getTileSequence() {
		return tileSequence;
	}
	public void setTileSequence(int tileSequence) {
		this.tileSequence = tileSequence;
	}
	public Boolean getDrillDownFlag() {
		return drillDownFlag;
	}
	public void setDrillDownFlag(Boolean drillDownFlag) {
		this.drillDownFlag = drillDownFlag;
	}
	public String getQueryId() {
		return queryId;
	}
	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}
	public String getPropId() {
		return propId;
	}
	public void setPropId(String propId) {
		this.propId = propId;
	}
	public String getPropValue() {
		return propValue;
	}
	public void setPropValue(String propValue) {
		this.propValue = propValue;
	}
	public String getDrillDownRef() {
		return drillDownRef;
	}
	public void setDrillDownRef(String drillDownRef) {
		this.drillDownRef = drillDownRef;
	}
	public String getChartType() {
		return chartType;
	}
	public void setChartType(String chartType) {
		this.chartType = chartType;
	}
	public List<DashboardTilesVb> getDrillDownlst() {
		return drillDownlst;
	}
	public void setDrillDownlst(List<DashboardTilesVb> drillDownlst) {
		this.drillDownlst = drillDownlst;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getPropertyAttr() {
		return propertyAttr;
	}
	public void setPropertyAttr(String propertyAttr) {
		this.propertyAttr = propertyAttr;
	}
	public int getDetailTileSequence() {
		return detailTileSequence;
	}
	public void setDetailTileSequence(int detailTileSequence) {
		this.detailTileSequence = detailTileSequence;
	}
	public int getPlaceHolderCnt() {
		return placeHolderCnt;
	}
	public void setPlaceHolderCnt(int placeHolderCnt) {
		this.placeHolderCnt = placeHolderCnt;
	}
	public String getTileDataSet() {
		return tileDataSet;
	}
	public void setTileDataSet(String tileDataSet) {
		this.tileDataSet = tileDataSet;
	}
	public String getChartDataSet() {
		return chartDataSet;
	}
	public void setChartDataSet(String chartDataSet) {
		this.chartDataSet = chartDataSet;
	}
	public String getDoubleWidthFlag() {
		return doubleWidthFlag;
	}
	public void setDoubleWidthFlag(String doubleWidthFlag) {
		this.doubleWidthFlag = doubleWidthFlag;
	}
	public List getGridDataSet() {
		return gridDataSet;
	}
	public void setGridDataSet(List gridDataSet) {
		this.gridDataSet = gridDataSet;
	}
	public List getGridColumnSet() {
		return gridColumnSet;
	}
	public void setGridColumnSet(List gridColumnSet) {
		this.gridColumnSet = gridColumnSet;
	}
	public List getGridColumnFormats() {
		return gridColumnFormats;
	}
	public void setGridColumnFormats(List gridColumnFormats) {
		this.gridColumnFormats = gridColumnFormats;
	}
	public String getPromptValue1() {
		return promptValue1;
	}
	public void setPromptValue1(String promptValue1) {
		this.promptValue1 = promptValue1;
	}
	public String getPromptValue2() {
		return promptValue2;
	}
	public void setPromptValue2(String promptValue2) {
		this.promptValue2 = promptValue2;
	}
	public String getPromptValue3() {
		return promptValue3;
	}
	public void setPromptValue3(String promptValue3) {
		this.promptValue3 = promptValue3;
	}
	public String getPromptValue4() {
		return promptValue4;
	}
	public void setPromptValue4(String promptValue4) {
		this.promptValue4 = promptValue4;
	}
	public String getPromptValue5() {
		return promptValue5;
	}
	public void setPromptValue5(String promptValue5) {
		this.promptValue5 = promptValue5;
	}
	public String getPromptValue6() {
		return promptValue6;
	}
	public void setPromptValue6(String promptValue6) {
		this.promptValue6 = promptValue6;
	}
	public String getPromptValue7() {
		return promptValue7;
	}
	public void setPromptValue7(String promptValue7) {
		this.promptValue7 = promptValue7;
	}
	public String getPromptValue8() {
		return promptValue8;
	}
	public void setPromptValue8(String promptValue8) {
		this.promptValue8 = promptValue8;
	}
	public String getPromptValue9() {
		return promptValue9;
	}
	public void setPromptValue9(String promptValue9) {
		this.promptValue9 = promptValue9;
	}
	public String getPromptValue10() {
		return promptValue10;
	}
	public void setPromptValue10(String promptValue10) {
		this.promptValue10 = promptValue10;
	}
	public List<DashboardTilesVb> getChildTileslst() {
		return childTileslst;
	}
	public void setChildTileslst(List<DashboardTilesVb> childTileslst) {
		this.childTileslst = childTileslst;
	}
	public String getDashboardId() {
		return dashboardId;
	}
	public void setDashboardId(String dashboardId) {
		this.dashboardId = dashboardId;
	}
	public String getTabId() {
		return tabId;
	}
	public void setTabId(String tabId) {
		this.tabId = tabId;
	}
	public String getSubTiles() {
		return subTiles;
	}
	public void setSubTiles(String subTiles) {
		this.subTiles = subTiles;
	}
	public String getSubSequence() {
		return subSequence;
	}
	public void setSubSequence(String subSequence) {
		this.subSequence = subSequence;
	}
	public String getSubTileId() {
		return subTileId;
	}
	public void setSubTileId(String subTileId) {
		this.subTileId = subTileId;
	}
	public List<DashboardTilesVb> getSubTileslst() {
		return subTileslst;
	}
	public void setSubTileslst(List<DashboardTilesVb> subTileslst) {
		this.subTileslst = subTileslst;
	}
	public int getTransitionEffectAT() {
		return transitionEffectAT;
	}
	public void setTransitionEffectAT(int transitionEffectAT) {
		this.transitionEffectAT = transitionEffectAT;
	}
	public String getTransitionEffect() {
		return transitionEffect;
	}
	public void setTransitionEffect(String transitionEffect) {
		this.transitionEffect = transitionEffect;
	}
	public int getThemeAT() {
		return themeAT;
	}
	public void setThemeAT(int themeAT) {
		this.themeAT = themeAT;
	}
	public String getTheme() {
		return theme;
	}
	public void setTheme(String theme) {
		this.theme = theme;
	}
	
}
