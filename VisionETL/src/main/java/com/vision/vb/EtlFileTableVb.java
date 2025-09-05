package com.vision.vb;

import java.util.List;

public class EtlFileTableVb extends CommonVb {

	private String connectorId = "";
	private String viewName = "";
	private String fileId = "";
	private String sheetName = "";
	private int fileStatusNt = 1;
	private int fileStatus = 0;
	public List<EtlFileColumnsVb> fileColumnsList = null;

	public void setConnectorId(String connectorId) {
		this.connectorId = connectorId;
	}

	public String getConnectorId() {
		return connectorId;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	public String getViewName() {
		return viewName;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}

	public String getFileId() {
		return fileId;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setFileStatusNt(int fileStatusNt) {
		this.fileStatusNt = fileStatusNt;
	}

	public int getFileStatusNt() {
		return fileStatusNt;
	}

	public void setFileStatus(int fileStatus) {
		this.fileStatus = fileStatus;
	}

	public int getFileStatus() {
		return fileStatus;
	}

	public List<EtlFileColumnsVb> getFileColumnsList() {
		return fileColumnsList;
	}

	public void setFileColumnsList(List<EtlFileColumnsVb> fileColumnsList) {
		this.fileColumnsList = fileColumnsList;
	}

}