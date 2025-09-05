package com.vision.vb;

import java.io.Serializable;

public class ETLFeedEventBasedPropVb implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String type = "";
	/*private DependencyVb dependency = null;
	private ETLReadinessVb readinessVb = null;*/
	private String readinessScriptID = "";
	private String preScriptID = "";
	private String postScriptID = "";
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	/*public DependencyVb getDependency() {
		return dependency;
	}
	public void setDependency(DependencyVb dependency) {
		this.dependency = dependency;
	}
	public ETLReadinessVb getReadinessVb() {
		return readinessVb;
	}
	public void setReadinessVb(ETLReadinessVb readinessVb) {
		this.readinessVb = readinessVb;
	}*/
	public String getReadinessScriptID() {
		return readinessScriptID;
	}
	public void setReadinessScriptID(String readinessScriptID) {
		this.readinessScriptID = readinessScriptID;
	}
	public String getPreScriptID() {
		return preScriptID;
	}
	public void setPreScriptID(String preScriptID) {
		this.preScriptID = preScriptID;
	}
	public String getPostScriptID() {
		return postScriptID;
	}
	public void setPostScriptID(String postScriptID) {
		this.postScriptID = postScriptID;
	}

}