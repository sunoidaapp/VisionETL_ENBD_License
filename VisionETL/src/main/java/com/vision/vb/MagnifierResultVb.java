package com.vision.vb;

import java.util.List;

public class MagnifierResultVb extends CommonVb {

	private static final long serialVersionUID = -1135639825777689058L;
	private String columnOne = "";
	private String columnTwo = "";
	private String columnThree = "";
	private String columnFour = "";
	private String columnFive = "";
	private String columnSix = "";
	private String columnSeven = "";
	private String columnEight = "";
	private String columnNine = "";
	private String columnTen = "";	
	private String columnEleven = "";	
	private String columnTwelve= "";	
	private String columnThirteen= "";	
	private String columnFourteen= "";	
	private String query = "";
	private String whereCond = "";
	private String orderByCond = "";
	private int numberOfCols = 2;
	
	private String header1	= "";
	private String header2	= "";
	private String header3	= "";
	private String header4	= "";
	private String targetField1	= "";
	private String targetField2	= "";
	private String targetField3	= "";
	private String targetField4	= "";
	private String targetField5	= "";
	private String targetField6	= "";
	private String targetField7	= "";
	private String targetField8	= "";
	private String targetField9	= "";
	private String targetField10	= "";
	private String targetField11	= "";
	private String targetField12	= "";
	private String targetField13	= "";
	private String targetField14	= "";
	private String targetField15	= "";
	private String targetFieldDesc	= "";
	private String description = "";
	
	private int coumnOneWidth = 100;
	private int coumnTowWidth = 100;
	private int coumnThreeWidth = 360;
	private int coumnFourWidth = 100;
	
	private String restrictionCountry = "";
	private String restrictionLeBook = "";
	private String restrictionAo = "";
	private String restrictionSbu = "";
	private String restrictionOuc = "";
	private String specialFilter ="";
	private String specialFilterValue ="";
	private String prompt1 = "";
	private String prompt2 = "";
	private String prompt3 = "";
	
	private String promptValue1 = "";
	private String promptValue2 = ""; 
	private String promptValue3 =  "";
	private String promptValue4 =  "";
	private String promptValue5 =  "";
	private String promptValue6 =  "";
	private String promptValue7 =  "";
	private String promptValue8 =  "";
	private String promptValue9 = "";
	private String promptValue10 = "";
	private String magnifierResult="";

	
	List<SmartSearchVb> smartSearchOpt = null;
	
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getTargetField13() {
		return targetField13;
	}
	public void setTargetField13(String targetField13) {
		this.targetField13 = targetField13;
	}
	public String getTargetField14() {
		return targetField14;
	}
	public void setTargetField14(String targetField14) {
		this.targetField14 = targetField14;
	}
	public String getTargetField15() {
		return targetField15;
	}
	public void setTargetField15(String targetField15) {
		this.targetField15 = targetField15;
	}
	public String getColumnThirteen() {
		return columnThirteen;
	}
	public void setColumnThirteen(String columnThirteen) {
		this.columnThirteen = columnThirteen;
	}
	public String getColumnFourteen() {
		return columnFourteen;
	}
	public void setColumnFourteen(String columnFourteen) {
		this.columnFourteen = columnFourteen;
	}
	public String getHeader1() {
		return header1;
	}
	public void setHeader1(String header1) {
		this.header1 = header1;
	}
	public String getHeader2() {
		return header2;
	}
	public void setHeader2(String header2) {
		this.header2 = header2;
	}
	public String getHeader3() {
		return header3;
	}
	public void setHeader3(String header3) {
		this.header3 = header3;
	}
	public String getTargetField1() {
		return targetField1;
	}
	public void setTargetField1(String targetField1) {
		this.targetField1 = targetField1;
	}
	public String getTargetField2() {
		return targetField2;
	}
	public void setTargetField2(String targetField2) {
		this.targetField2 = targetField2;
	}
	public String getTargetField3() {
		return targetField3;
	}
	public void setTargetField3(String targetField3) {
		this.targetField3 = targetField3;
	}
	public String getTargetField4() {
		return targetField4;
	}
	public void setTargetField4(String targetField4) {
		this.targetField4 = targetField4;
	}
	public String getTargetField5() {
		return targetField5;
	}
	public void setTargetField5(String targetField5) {
		this.targetField5 = targetField5;
	}
	public String getTargetField6() {
		return targetField6;
	}
	public void setTargetField6(String targetField6) {
		this.targetField6 = targetField6;
	}
	public String getTargetField7() {
		return targetField7;
	}
	public void setTargetField7(String targetField7) {
		this.targetField7 = targetField7;
	}
	public String getTargetField8() {
		return targetField8;
	}
	public void setTargetField8(String targetField8) {
		this.targetField8 = targetField8;
	}
	public String getTargetField9() {
		return targetField9;
	}
	public void setTargetField9(String targetField9) {
		this.targetField9 = targetField9;
	}
	public String getTargetField10() {
		return targetField10;
	}
	public void setTargetField10(String targetField10) {
		this.targetField10 = targetField10;
	}
	public String getTargetFieldDesc() {
		return targetFieldDesc;
	}
	public void setTargetFieldDesc(String targetFieldDesc) {
		this.targetFieldDesc = targetFieldDesc;
	}
	public String getColumnOne() {
		return columnOne;
	}
	public void setColumnOne(String columnOne) {
		this.columnOne = columnOne;
	}
	public String getColumnTwo() {
		return columnTwo;
	}
	public void setColumnTwo(String columnTwo) {
		this.columnTwo = columnTwo;
	}
	public String getColumnThree() {
		return columnThree;
	}
	public void setColumnThree(String columnThree) {
		this.columnThree = columnThree;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public String getWhereCond() {
		return whereCond;
	}
	public void setWhereCond(String whereCond) {
		this.whereCond = whereCond;
	}
	public String getOrderByCond() {
		return orderByCond;
	}
	public void setOrderByCond(String orderByCond) {
		this.orderByCond = orderByCond;
	}
	public int getNumberOfCols() {
		return numberOfCols;
	}
	public void setNumberOfCols(int numberOfCols) {
		this.numberOfCols = numberOfCols;
	}
	public String getColumnFour() {
		return columnFour;
	}
	public void setColumnFour(String columnFour) {
		this.columnFour = columnFour;
	}
	public String getColumnFive() {
		return columnFive;
	}
	public void setColumnFive(String columnFive) {
		this.columnFive = columnFive;
	}
	public String getColumnSix() {
		return columnSix;
	}
	public void setColumnSix(String columnSix) {
		this.columnSix = columnSix;
	}
	public String getColumnSeven() {
		return columnSeven;
	}
	public void setColumnSeven(String columnSeven) {
		this.columnSeven = columnSeven;
	}
	public String getColumnEight() {
		return columnEight;
	}
	public void setColumnEight(String columnEight) {
		this.columnEight = columnEight;
	}
	public String getColumnNine() {
		return columnNine;
	}
	public void setColumnNine(String columnNine) {
		this.columnNine = columnNine;
	}
	public String getColumnTen() {
		return columnTen;
	}
	public void setColumnTen(String columnTen) {
		this.columnTen = columnTen;
	}
	public int getCoumnOneWidth() {
		return coumnOneWidth;
	}
	public void setCoumnOneWidth(int coumnOneWidth) {
		this.coumnOneWidth = coumnOneWidth;
	}
	public int getCoumnTowWidth() {
		return coumnTowWidth;
	}
	public void setCoumnTowWidth(int coumnTowWidth) {
		this.coumnTowWidth = coumnTowWidth;
	}
	public int getCoumnThreeWidth() {
		return coumnThreeWidth;
	}
	public void setCoumnThreeWidth(int coumnThreeWidth) {
		this.coumnThreeWidth = coumnThreeWidth;
	}
	public int getCoumnFourWidth() {
		return coumnFourWidth;
	}
	public void setCoumnFourWidth(int coumnFourWidth) {
		this.coumnFourWidth = coumnFourWidth;
	}
	public String getHeader4() {
		return header4;
	}
	public void setHeader4(String header4) {
		this.header4 = header4;
	}
	
	public String getColumnEleven() {
		return columnEleven;
	}
	public void setColumnEleven(String columnEleven) {
		this.columnEleven = columnEleven;
	}
	public String getColumnTwelve() {
		return columnTwelve;
	}
	public void setColumnTwelve(String columnTwelve) {
		this.columnTwelve = columnTwelve;
	}
	public String getTargetField11() {
		return targetField11;
	}
	public void setTargetField11(String targetField11) {
		this.targetField11 = targetField11;
	}
	public String getTargetField12() {
		return targetField12;
	}
	public void setTargetField12(String targetField12) {
		this.targetField12 = targetField12;
	}
	public List<SmartSearchVb> getSmartSearchOpt() {
		return smartSearchOpt;
	}
	public void setSmartSearchOpt(List<SmartSearchVb> smartSearchOpt) {
		this.smartSearchOpt = smartSearchOpt;
	}
	public String getRestrictionCountry() {
		return restrictionCountry;
	}
	public void setRestrictionCountry(String restrictionCountry) {
		this.restrictionCountry = restrictionCountry;
	}
	public String getRestrictionLeBook() {
		return restrictionLeBook;
	}
	public void setRestrictionLeBook(String restrictionLeBook) {
		this.restrictionLeBook = restrictionLeBook;
	}
	public String getRestrictionAo() {
		return restrictionAo;
	}
	public void setRestrictionAo(String restrictionAo) {
		this.restrictionAo = restrictionAo;
	}
	public String getRestrictionSbu() {
		return restrictionSbu;
	}
	public void setRestrictionSbu(String restrictionSbu) {
		this.restrictionSbu = restrictionSbu;
	}
	public String getRestrictionOuc() {
		return restrictionOuc;
	}
	public void setRestrictionOuc(String restrictionOuc) {
		this.restrictionOuc = restrictionOuc;
	}
	public String getSpecialFilter() {
		return specialFilter;
	}
	public void setSpecialFilter(String specialFilter) {
		this.specialFilter = specialFilter;
	}
	public String getSpecialFilterValue() {
		return specialFilterValue;
	}
	public void setSpecialFilterValue(String specialFilterValue) {
		this.specialFilterValue = specialFilterValue;
	}
	public String getPrompt1() {
		return prompt1;
	}
	public void setPrompt1(String prompt1) {
		this.prompt1 = prompt1;
	}
	public String getPrompt2() {
		return prompt2;
	}
	public void setPrompt2(String prompt2) {
		this.prompt2 = prompt2;
	}
	public String getPrompt3() {
		return prompt3;
	}
	public void setPrompt3(String prompt3) {
		this.prompt3 = prompt3;
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
	public String getMagnifierResult() {
		return magnifierResult;
	}
	public void setMagnifierResult(String magnifierResult) {
		this.magnifierResult = magnifierResult;
	}
	
}
