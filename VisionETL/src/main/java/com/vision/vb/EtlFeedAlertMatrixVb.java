package com.vision.vb;

import java.util.List;

public class EtlFeedAlertMatrixVb extends CommonVb {

	private String country = "";
	private String leBook = "";
	private String countryDesc = "";
	private String leBookDesc = "";
	private String matrixID = "";
	private String matrixDescription = "";
	private int matrixAlertStatusNt = 1;
	private int matrixAlertStatus =-1;
	List<SmartSearchVb> smartSearchOpt = null;
	List<EtlFeedAlertMatrixVb> alertMatrix =null;
	
	
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
	public String getCountryDesc() {
		return countryDesc;
	}
	public void setCountryDesc(String countryDesc) {
		this.countryDesc = countryDesc;
	}
	public String getLeBookDesc() {
		return leBookDesc;
	}
	public void setLeBookDesc(String leBookDesc) {
		this.leBookDesc = leBookDesc;
	}
	public String getMatrixID() {
		return matrixID;
	}
	public void setMatrixID(String matrixID) {
		this.matrixID = matrixID;
	}
	public String getMatrixDescription() {
		return matrixDescription;
	}
	public void setMatrixDescription(String matrixDescription) {
		this.matrixDescription = matrixDescription;
	}
	public int getMatrixAlertStatusNt() {
		return matrixAlertStatusNt;
	}
	public void setMatrixAlertStatusNt(int matrixAlertStatusNt) {
		this.matrixAlertStatusNt = matrixAlertStatusNt;
	}
	public int getMatrixAlertStatus() {
		return matrixAlertStatus;
	}
	public void setMatrixAlertStatus(int matrixAlertStatus) {
		this.matrixAlertStatus = matrixAlertStatus;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
	public List<EtlFeedAlertMatrixVb> getalertMatrix() {
		return alertMatrix;
	}
	public void setalertMatrix(List<EtlFeedAlertMatrixVb> alertMatrix) {
		this.alertMatrix = alertMatrix;
	}
	
}