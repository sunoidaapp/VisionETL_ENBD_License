package com.vision.vb;

import java.util.List;

public class EtlFeedRelationVb extends CommonVb{

	private String country = "";
	private String leBook = "";
	private String feedId = "";
	private String fromTableId = "";
	private String toTableId = "";
	private int joinTypeNt = 1100 ;
	private int joinType = 0 ;
	private String filterContext = "";
	private String relationContext = "";
	private int feedRelationStatusNt = 2000 ;
	private int feedRelationStatus = 0 ;
	private String fromTableName = "";
	private String toTableName = "";

	public EtlFeedRelationVb() {}

	public EtlFeedRelationVb(String country, String leBook, String feedId) {
		super();
		this.country = country;
		this.leBook = leBook;
		this.feedId = feedId;
	}
	
	private List<RelationMapVb> relationScriptParsed = null;
	
	/* Sample XML */
	/*<relation>
	    <columnmapping>
	        <column>
	            <fcolumn>Country</fcolumn>
	            <tcolumn>Country</tcolumn>
	        </column>
	        <column>
	            <fcolumn>Le_Book</fcolumn>
	            <tcolumn>Le_Book</tcolumn>
	        </column>
	        <column>
	            <fcolumn>Year_Month</fcolumn>
	            <tcolumn>Year_Month</tcolumn>
	        </column>
	        <column>
	            <fcolumn>Sequence_FD</fcolumn>
	            <tcolumn>Sequence_FD</tcolumn>
	        </column>
	    </columnmapping>
	    <customjoin></customjoin>
	    <ansii_joinstring>(Fin_Dly_Headers.Country = Fin_Dly_Balances.Country AND Fin_Dly_Headers.Le_Book = Fin_Dly_Balances.Le_Book AND Fin_Dly_Headers.Year_Month = Fin_Dly_Balances.Year_Month AND Fin_Dly_Headers.Sequence_FD = Fin_Dly_Balances.Sequence_FD)</ansii_joinstring>
	    <std_joinstring>Fin_Dly_Headers.Country = Fin_Dly_Balances.Country AND Fin_Dly_Headers.Le_Book = Fin_Dly_Balances.Le_Book AND Fin_Dly_Headers.Year_Month = Fin_Dly_Balances.Year_Month AND Fin_Dly_Headers.Sequence_FD = Fin_Dly_Balances.Sequence_FD </std_joinstring>
	</relation>*/
	
	public String getFromTableName() {
		return fromTableName;
	}

	public void setFromTableName(String fromTableName) {
		this.fromTableName = fromTableName;
	}

	public String getToTableName() {
		return toTableName;
	}

	public void setToTableName(String toTableName) {
		this.toTableName = toTableName;
	}

	public List<RelationMapVb> getRelationScriptParsed() {
		return relationScriptParsed;
	}

	public void setRelationScriptParsed(List<RelationMapVb> relationScriptParsed) {
		this.relationScriptParsed = relationScriptParsed;
	}

	public String getFilterCondition() {
		return filterCondition;
	}

	public void setFilterCondition(String filterCondition) {
		this.filterCondition = filterCondition;
	}

	public String getCustomJoinString() {
		return customJoinString;
	}

	public void setCustomJoinString(String customJoinString) {
		this.customJoinString = customJoinString;
	}

	public String getRelationScript() {
		return relationScript;
	}

	public void setRelationScript(String relationScript) {
		this.relationScript = relationScript;
	}

	private String filterCondition = "";
	private String customJoinString = "";
	private String relationScript = "";
	
	public void setCountry(String country) {
		this.country = country; 
	}

	public String getCountry() {
		return country; 
	}
	public void setLeBook(String leBook) {
		this.leBook = leBook; 
	}

	public String getLeBook() {
		return leBook; 
	}
	public void setFeedId(String feedId) {
		this.feedId = feedId; 
	}

	public String getFeedId() {
		return feedId; 
	}
	public void setFromTableId(String fromTableId) {
		this.fromTableId = fromTableId; 
	}

	public String getFromTableId() {
		return fromTableId; 
	}
	public void setToTableId(String toTableId) {
		this.toTableId = toTableId; 
	}

	public String getToTableId() {
		return toTableId; 
	}
	public void setJoinTypeNt(int joinTypeNt) {
		this.joinTypeNt = joinTypeNt; 
	}

	public int getJoinTypeNt() {
		return joinTypeNt; 
	}
	public void setJoinType(int joinType) {
		this.joinType = joinType; 
	}

	public int getJoinType() {
		return joinType; 
	}
	public void setFilterContext(String filterContext) {
		this.filterContext = filterContext; 
	}

	public String getFilterContext() {
		return filterContext; 
	}
	public void setRelationContext(String relationContext) {
		this.relationContext = relationContext; 
	}

	public String getRelationContext() {
		return relationContext; 
	}
	public void setFeedRelationStatusNt(int feedRelationStatusNt) {
		this.feedRelationStatusNt = feedRelationStatusNt; 
	}

	public int getFeedRelationStatusNt() {
		return feedRelationStatusNt; 
	}
	public void setFeedRelationStatus(int feedRelationStatus) {
		this.feedRelationStatus = feedRelationStatus; 
	}

	public int getFeedRelationStatus() {
		return feedRelationStatus; 
	}


}