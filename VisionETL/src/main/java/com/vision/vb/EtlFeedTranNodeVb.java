package com.vision.vb;

import java.util.List;

public class EtlFeedTranNodeVb extends CommonVb implements Comparable<EtlFeedTranNodeVb> {

	private String nodeId = "";
	private String nodeName = "";
	private String nodeDesc = "";
	private String xAxis = "";
	private String yAxis = "";
	private String transformationId = "";
	private int transformationStatusNt = 2000;
	private int transformationStatus = -1;

	private List<EtlFeedTranNodeVb> parentTransformationList = null;
	private List<EtlFeedTranNodeVb> childTransformationList = null;
	private List<EtlFeedTranColumnVb> tranColumnList = null;

	private String minimizeFlag = "";
	private String methodName = "";
	private int executionOrder;
	private String transformationSql = "";
	private String viewName = "";
	private String transSqlExecutable = "";
	
	private String nodeChange = "";
	private String transSqlAppend = ""; //Aggregation -> group by | Filter -> where condition
	
	private String transformationType = "";
	private String joinType = ""; 
	private String groupId = "";
	private String customFlag = "";
	private String customExpression = ""; // for Free Sql text
	boolean isFilterCustomFlag = false;
	private String groupUID = "";
	
	public EtlFeedTranNodeVb(String nodeId) {
		super();
		this.nodeId = nodeId;
	}
	
	@Override
	public int compareTo(EtlFeedTranNodeVb vb) {
		if (executionOrder == vb.executionOrder)
			return 0;
		else if (executionOrder < vb.executionOrder)
			return 1;
		else
			return -1;
	}

	public EtlFeedTranNodeVb(String nodeId, String nodeName, String nodeDesc, String xAxis, String yAxis,
			String transformationId, List<EtlFeedTranNodeVb> parentTransformationList,
			List<EtlFeedTranNodeVb> childTransformationList, int transformationStatus, String minimizeFlag,
			String groupId, String joinType, String customFlag, String customExpression) {
		super();
		this.nodeId = nodeId;
		this.nodeName = nodeName;
		this.nodeDesc = nodeDesc;
		this.xAxis = xAxis;
		this.yAxis = yAxis;
		this.transformationId = transformationId;
		this.parentTransformationList = parentTransformationList;
		this.childTransformationList = childTransformationList;
		this.transformationStatus = transformationStatus;
		this.minimizeFlag = minimizeFlag;
		this.groupId = groupId;
		this.joinType = joinType;
		this.customFlag = customFlag;
		this.customExpression = customExpression;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getNodeDesc() {
		return nodeDesc;
	}

	public void setNodeDesc(String nodeDesc) {
		this.nodeDesc = nodeDesc;
	}

	public String getxAxis() {
		return xAxis;
	}

	public void setxAxis(String xAxis) {
		this.xAxis = xAxis;
	}

	public String getyAxis() {
		return yAxis;
	}

	public void setyAxis(String yAxis) {
		this.yAxis = yAxis;
	}

	public String getTransformationId() {
		return transformationId;
	}

	public void setTransformationId(String transformationId) {
		this.transformationId = transformationId;
	}

	public int getTransformationStatusNt() {
		return transformationStatusNt;
	}

	public void setTransformationStatusNt(int transformationStatusNt) {
		this.transformationStatusNt = transformationStatusNt;
	}

	public int getTransformationStatus() {
		return transformationStatus;
	}

	public void setTransformationStatus(int transformationStatus) {
		this.transformationStatus = transformationStatus;
	}

	public List<EtlFeedTranNodeVb> getParentTransformationList() {
		return parentTransformationList;
	}

	public void setParentTransformationList(List<EtlFeedTranNodeVb> parentTransformationList) {
		this.parentTransformationList = parentTransformationList;
	}

	public List<EtlFeedTranNodeVb> getChildTransformationList() {
		return childTransformationList;
	}

	public void setChildTransformationList(List<EtlFeedTranNodeVb> childTransformationList) {
		this.childTransformationList = childTransformationList;
	}

	public List<EtlFeedTranColumnVb> getTranColumnList() {
		return tranColumnList;
	}

	public void setTranColumnList(List<EtlFeedTranColumnVb> tranColumnList) {
		this.tranColumnList = tranColumnList;
	}

	public String getMinimizeFlag() {
		return minimizeFlag;
	}

	public void setMinimizeFlag(String minimizeFlag) {
		this.minimizeFlag = minimizeFlag;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public int getExecutionOrder() {
		return executionOrder;
	}

	public void setExecutionOrder(int executionOrder) {
		this.executionOrder = executionOrder;
	}

	public String getTransformationSql() {
		return transformationSql;
	}

	public void setTransformationSql(String transformationSql) {
		this.transformationSql = transformationSql;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	public String getTransSqlExecutable() {
		return transSqlExecutable;
	}

	public void setTransSqlExecutable(String transSqlExecutable) {
		this.transSqlExecutable = transSqlExecutable;
	}

	public String getNodeChange() {
		return nodeChange;
	}

	public void setNodeChange(String nodeChange) {
		this.nodeChange = nodeChange;
	}

	public String getTransSqlAppend() {
		return transSqlAppend;
	}

	public void setTransSqlAppend(String transSqlAppend) {
		this.transSqlAppend = transSqlAppend;
	}

	public String getTransformationType() {
		return transformationType;
	}

	public void setTransformationType(String transformationType) {
		this.transformationType = transformationType;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getCustomFlag() {
		return customFlag;
	}

	public void setCustomFlag(String customFlag) {
		this.customFlag = customFlag;
	}

	public String getCustomExpression() {
		return customExpression;
	}

	public void setCustomExpression(String customExpression) {
		this.customExpression = customExpression;
	}

	public boolean isFilterCustomFlag() {
		return isFilterCustomFlag;
	}

	public void setFilterCustomFlag(boolean isFilterCustomFlag) {
		this.isFilterCustomFlag = isFilterCustomFlag;
	}

	public String getGroupUID() {
		return groupUID;
	}

	public void setGroupUID(String groupUID) {
		this.groupUID = groupUID;
	}

}