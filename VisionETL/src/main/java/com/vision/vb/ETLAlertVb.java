package com.vision.vb;

import java.io.Serializable;
import java.util.List;

public class ETLAlertVb implements Serializable {

	private String channelTypeSms = "";
	private String channelTypeEmail = "";
	private String eventType = "";
	private List<String> matrixIdLst = null;
	private String condition = "";
	private String conditionExecutionType = "";
	private String conditionValidationType = "";
	private String conditionPassValue = "";
	private String conditionValidCriteria = "";
	private String subject = "";
	private String content = "";
	
	public String getChannelTypeSms() {
		return channelTypeSms;
	}
	public void setChannelTypeSms(String channelTypeSms) {
		this.channelTypeSms = channelTypeSms;
	}
	public String getChannelTypeEmail() {
		return channelTypeEmail;
	}
	public void setChannelTypeEmail(String channelTypeEmail) {
		this.channelTypeEmail = channelTypeEmail;
	}
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public List<String> getMatrixIdLst() {
		return matrixIdLst;
	}
	public void setMatrixIdLst(List<String> matrixIdLst) {
		this.matrixIdLst = matrixIdLst;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	public String getConditionExecutionType() {
		return conditionExecutionType;
	}
	public void setConditionExecutionType(String conditionExecutionType) {
		this.conditionExecutionType = conditionExecutionType;
	}
	public String getConditionValidationType() {
		return conditionValidationType;
	}
	public void setConditionValidationType(String conditionValidationType) {
		this.conditionValidationType = conditionValidationType;
	}
	public String getConditionPassValue() {
		return conditionPassValue;
	}
	public void setConditionPassValue(String conditionPassValue) {
		this.conditionPassValue = conditionPassValue;
	}
	public String getConditionValidCriteria() {
		return conditionValidCriteria;
	}
	public void setConditionValidCriteria(String conditionValidCriteria) {
		this.conditionValidCriteria = conditionValidCriteria;
	}
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}

}
