package com.vision.vb;

import java.util.ArrayList;
import java.util.List;

public class EtlFeedTransformationMasterVb extends CommonVb {

	private String transformationId = "";
	private int sortOrder = 0;
	private int transformationCategoryAT = 2128;
	private String transformationCategory = "";
	private String transformationCategoryDesc = "";
	private String transformationName = "";
	private String transformationDesc = "";
	private int transformationTypeAT = 2126;
	private String transformationType = "";
	private String transformationTypeDesc = "";
	//private int inputCount = 0;
	private int ioDataTypeAT = 2127;
	//private String inputType = "";
	//private String inputTypeDesc = "";
	private int outputCount = 0;
	private String outputType = "";
	private String syntaxExpresssion = "";
	private String example = "";
	private int transformationStatusNt = 2000;
	private int transformationStatus = 0;
	List<EtlFeedTransformationMasterVb> masterLst = null;
	private int inputSlabCount = 0;
	List<EtlTransMasterInputSlabVb> slabList = new ArrayList<>();

	public String getTransformationId() {
		return transformationId;
	}

	public void setTransformationId(String transformationId) {
		this.transformationId = transformationId;
	}

	public int getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(int sortOrder) {
		this.sortOrder = sortOrder;
	}

	public int getTransformationCategoryAT() {
		return transformationCategoryAT;
	}

	public void setTransformationCategoryAT(int transformationCategoryAT) {
		this.transformationCategoryAT = transformationCategoryAT;
	}

	public String getTransformationCategory() {
		return transformationCategory;
	}

	public void setTransformationCategory(String transformationCategory) {
		this.transformationCategory = transformationCategory;
	}

	public String getTransformationName() {
		return transformationName;
	}

	public void setTransformationName(String transformationName) {
		this.transformationName = transformationName;
	}

	public String getTransformationDesc() {
		return transformationDesc;
	}

	public void setTransformationDesc(String transformationDesc) {
		this.transformationDesc = transformationDesc;
	}

	public int getTransformationTypeAT() {
		return transformationTypeAT;
	}

	public void setTransformationTypeAT(int transformationTypeAT) {
		this.transformationTypeAT = transformationTypeAT;
	}

	public String getTransformationType() {
		return transformationType;
	}

	public void setTransformationType(String transformationType) {
		this.transformationType = transformationType;
	}

	public int getIoDataTypeAT() {
		return ioDataTypeAT;
	}

	public void setIoDataTypeAT(int ioDataTypeAT) {
		this.ioDataTypeAT = ioDataTypeAT;
	}

	public int getOutputCount() {
		return outputCount;
	}

	public void setOutputCount(int outputCount) {
		this.outputCount = outputCount;
	}

	public String getOutputType() {
		return outputType;
	}

	public void setOutputType(String outputType) {
		this.outputType = outputType;
	}

	public String getSyntaxExpresssion() {
		return syntaxExpresssion;
	}

	public void setSyntaxExpresssion(String syntaxExpresssion) {
		this.syntaxExpresssion = syntaxExpresssion;
	}

	public String getExample() {
		return example;
	}

	public void setExample(String example) {
		this.example = example;
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

	public String getTransformationCategoryDesc() {
		return transformationCategoryDesc;
	}

	public void setTransformationCategoryDesc(String transformationCategoryDesc) {
		this.transformationCategoryDesc = transformationCategoryDesc;
	}

	public String getTransformationTypeDesc() {
		return transformationTypeDesc;
	}

	public void setTransformationTypeDesc(String transformationTypeDesc) {
		this.transformationTypeDesc = transformationTypeDesc;
	}

	public List<EtlFeedTransformationMasterVb> getMasterLst() {
		return masterLst;
	}

	public void setMasterLst(List<EtlFeedTransformationMasterVb> masterLst) {
		this.masterLst = masterLst;
	}

	public int getInputSlabCount() {
		return inputSlabCount;
	}

	public void setInputSlabCount(int inputSlabCount) {
		this.inputSlabCount = inputSlabCount;
	}

	public List<EtlTransMasterInputSlabVb> getSlabList() {
		return slabList;
	}

	public void setSlabList(List<EtlTransMasterInputSlabVb> slabList) {
		this.slabList = slabList;
	}

}