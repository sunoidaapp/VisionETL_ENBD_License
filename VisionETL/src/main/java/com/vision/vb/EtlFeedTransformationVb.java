package com.vision.vb;

public class EtlFeedTransformationVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String nodeId = "";
	private String parentNodeId = "";
	private String childNodeId = "";
	private String nodeName = "";
	private String nodeDesc = "";
	private String transformationId = "";
	private String xAxis = "";
	private String yAxis = "";
	private int transformationStatusNt = 2000;
	private int transformationStatus = 0;
	private String minimizeFlag = "";

	private String joinType = "";
	private String groupId = "";
	private String customFlag = "";
	private String customExpression = ""; // for Free Sql text
	
	public EtlFeedTransformationVb() {}

	public EtlFeedTransformationVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
	}
	

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

	public String getFeedId() {
		return feedId;
	}

	public void setFeedId(String feedId) {
		this.feedId = feedId;
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

	public String getParentNodeId() {
		return parentNodeId;
	}

	public void setParentNodeId(String parentNodeId) {
		this.parentNodeId = parentNodeId;
	}

	public String getChildNodeId() {
		return childNodeId;
	}

	public void setChildNodeId(String childNodeId) {
		this.childNodeId = childNodeId;
	}

	public String getMinimizeFlag() {
		return minimizeFlag;
	}

	public void setMinimizeFlag(String minimizeFlag) {
		this.minimizeFlag = minimizeFlag;
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

}