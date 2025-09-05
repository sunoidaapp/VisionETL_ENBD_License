package com.vision.vb;

import java.util.List;

public class EtlFeedTranGroupVb implements Comparable<EtlFeedTranGroupVb>{
	
	private String UID = ""; //Concat groupID & groupEO
	private double groupEO; //Final Execution Order for Spark Engine - Will be used by "Comparable" interface to sort
	private String groupID = "";
	private int executionOrder;
	private String parentUID = "";
	private String transformationSql = "";
	private String transSqlExecutable = "";
	private String viewName = ""; //Identical to UID
	private List<EtlFeedTranNodeVb> nodeList = null;
	private String transformationType = ""; //A/P -> Active/Passive
	private String transformationID = "";
	private List<EtlFeedTranGroupVb> parentGroupUIDList = null;
	private String transSqlAppend = ""; //Aggregation -> group by | Filter -> where condition
	
	public EtlFeedTranGroupVb(){}
	
	public EtlFeedTranGroupVb(String uID) {
		UID = uID;
	}

	@Override
	public int compareTo(EtlFeedTranGroupVb vb) {
//		double groupEO_Main = Double.parseDouble(groupEO.replaceAll("_", "."));
//		double groupEO_Detail = Double.parseDouble((vb.groupEO).replaceAll("_", "."));
		if (groupEO == vb.groupEO)
			return 0;
		else if (groupEO > vb.groupEO)
			return 1;
		else
			return -1;
	}
	
	public String getUID() {
		return UID;
	}
	public void setUID(String uID) {
		UID = uID;
	}
	public double getGroupEO() {
		return groupEO;
	}
	public void setGroupEO(double groupEO) {
		this.groupEO = groupEO;
	}
	public String getGroupID() {
		return groupID;
	}
	public void setGroupID(String groupID) {
		this.groupID = groupID;
	}
	public int getExecutionOrder() {
		return executionOrder;
	}
	public void setExecutionOrder(int executionOrder) {
		this.executionOrder = executionOrder;
	}
	public String getParentUID() {
		return parentUID;
	}
	public void setParentUID(String parentUID) {
		this.parentUID = parentUID;
	}
	public String getTransformationSql() {
		return transformationSql;
	}
	public void setTransformationSql(String transformationSql) {
		this.transformationSql = transformationSql;
	}
	public String getTransSqlExecutable() {
		return transSqlExecutable;
	}
	public void setTransSqlExecutable(String transSqlExecutable) {
		this.transSqlExecutable = transSqlExecutable;
	}
	public String getViewName() {
		return viewName;
	}
	public void setViewName(String viewName) {
		this.viewName = viewName;
	}
	public List<EtlFeedTranNodeVb> getNodeList() {
		return nodeList;
	}
	public void setNodeList(List<EtlFeedTranNodeVb> nodeList) {
		this.nodeList = nodeList;
	}
	public String getTransformationType() {
		return transformationType;
	}
	public void setTransformationType(String transformationType) {
		this.transformationType = transformationType;
	}

	public String getTransformationID() {
		return transformationID;
	}

	public void setTransformationID(String transformationID) {
		this.transformationID = transformationID;
	}

	public List<EtlFeedTranGroupVb> getParentGroupUIDList() {
		return parentGroupUIDList;
	}

	public void setParentGroupUIDList(List<EtlFeedTranGroupVb> parentGroupUIDList) {
		this.parentGroupUIDList = parentGroupUIDList;
	}

	public String getTransSqlAppend() {
		return transSqlAppend;
	}

	public void setTransSqlAppend(String transSqlAppend) {
		this.transSqlAppend = transSqlAppend;
	}
	
}
