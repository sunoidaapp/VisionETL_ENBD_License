package com.vision.dao;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlExtractionSummaryFieldsVb;
import com.vision.vb.EtlFeedMainVb;

@Component
public class EtlExtractionSummaryFieldsDao extends AbstractDao<EtlExtractionSummaryFieldsVb> {

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String EtlFieldsStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.ETL_FIELDS_STATUS", "ETL_FIELDS_STATUS_DESC");
	String EtlFieldsStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.ETL_FIELDS_STATUS", "ETL_FIELDS_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlExtractionSummaryFieldsVb vObject = new EtlExtractionSummaryFieldsVb();
				if(rs.getString("COUNTRY")!= null){ 
					vObject.setCountry(rs.getString("COUNTRY"));
				}else{
					vObject.setCountry("");
				}
				if(rs.getString("LE_BOOK")!= null){ 
					vObject.setLeBook(rs.getString("LE_BOOK"));
				}else{
					vObject.setLeBook("");
				}
				if(rs.getString("FEED_ID")!= null){ 
					vObject.setFeedId(rs.getString("FEED_ID"));
				}else{
					vObject.setFeedId("");
				}
				if(rs.getString("TABLE_ID")!= null){ 
					vObject.setTableId(rs.getString("TABLE_ID"));
				}else{
					vObject.setTableId("");
				}
				vObject.setColId(rs.getInt("COL_ID"));
				if(rs.getString("SOURCE_CONNECTOR_ID")!= null){ 
					vObject.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
				}else{
					vObject.setSourceConnectorId("");
				}
				
				vObject.setUserId(rs.getInt("USER_ID"));
				if(rs.getString("COL_NAME")!= null){ 
					vObject.setColName(rs.getString("COL_NAME"));
				}else{
					vObject.setColName("");
				}
				if(rs.getString("COLUMN_DATA_TYPE")!= null){ 
					vObject.setColDataType(rs.getString("COLUMN_DATA_TYPE"));
				}else{
					vObject.setColDataType("");
				}
				
				if(rs.getString("TABLE_NAME")!= null){ 
					vObject.setTableName(rs.getString("TABLE_NAME"));
				}else{
					vObject.setTableName("");
				}
				if(rs.getString("TABLE_ALIAS_NAME")!= null){ 
					vObject.setTableAliasName(rs.getString("TABLE_ALIAS_NAME"));
				}else{
					vObject.setTableAliasName("");
				}
				if(rs.getString("TABLE_TYPE")!= null){ 
					vObject.setTableType(rs.getString("TABLE_TYPE"));
				}else{
					vObject.setTableType("");
				}
				
				if(rs.getString("ALIAS")!= null){ 
					vObject.setAlias(rs.getString("ALIAS"));
				}else{
					vObject.setAlias("");
				}
				if(rs.getString("CONDITION_OPERATOR")!= null){ 
					vObject.setConditionOperator(rs.getString("CONDITION_OPERATOR"));
				}else{
					vObject.setConditionOperator("");
				}
				if(rs.getString("VALUE_1")!= null){ 
					vObject.setValue1(rs.getString("VALUE_1"));
				}else{
					vObject.setValue1("");
				}
				if(rs.getString("VALUE_2")!= null){ 
					vObject.setValue2(rs.getString("VALUE_2"));
				}else{
					vObject.setValue2("");
				}
				if(rs.getString("DISPLAY_FLAG")!= null){ 
					vObject.setDisplayFlag(rs.getString("DISPLAY_FLAG"));
				}else{
					vObject.setDisplayFlag("");
				}
				if(rs.getString("SORT_TYPE")!= null){ 
					vObject.setSortType(rs.getString("SORT_TYPE"));
				}else{
					vObject.setSortType("");
				}
				vObject.setSortOrder(rs.getInt("SORT_ORDER"));
				if(rs.getString("AGG_FUNCTION")!= null){ 
					vObject.setAggFunction(rs.getString("AGG_FUNCTION"));
				}else{
					vObject.setAggFunction("");
				}
				if(rs.getString("GROUP_BY")!= null){ 
					vObject.setGroupBy(rs.getString("GROUP_BY"));
				}else{
					vObject.setGroupBy("");
				}
				if(rs.getString("COL_DISPLAY_TYPE")!= null){ 
					vObject.setColDisplayType(rs.getString("COL_DISPLAY_TYPE"));
				}else{
					vObject.setColDisplayType("");
				}
				vObject.setEtlFieldsStatusNt(rs.getInt("ETL_FIELDS_STATUS_NT"));
				vObject.setEtlFieldsStatus(rs.getInt("ETL_FIELDS_STATUS"));
				if(rs.getString("FORMAT_TYPE")!= null){ 
					vObject.setDateFormat(rs.getString("FORMAT_TYPE"));
				}else{
					vObject.setDateFormat("");
				}
				vObject.setFormatTypeDesc(rs.getString("FORMAT_TYPE_DESC"));
				vObject.setDateFormattingSyntax(rs.getString("DATE_FORMATTING_SYNTAX"));
				vObject.setDateConversionSyntax(rs.getString("DATE_CONVERSION_SYNTAX"));
				vObject.setJavaFormatDesc(rs.getString("JAVA_DATE_FORMAT"));
				if(ValidationUtil.isValid(rs.getString("EXPERSSION_TEXT"))){
					vObject.setExperssionText(rs.getString("EXPERSSION_TEXT").replaceAll("#TABLE_ALIAS#", ValidationUtil.isValid(rs.getString("TABLE_ALIAS_NAME"))?rs.getString("TABLE_ALIAS_NAME"):rs.getString("TABLE_NAME")));
				}
				if(rs.getString("JOIN_CONDITION")!= null){ 
					vObject.setJoinCondition(rs.getString("JOIN_CONDITION"));
				}else{
					vObject.setJoinCondition("");
				}
				if(rs.getString("DYNAMIC_START_FLAG")!= null){ 
					vObject.setDynamicStartFlag(rs.getString("DYNAMIC_START_FLAG"));
				}else{
					vObject.setDynamicStartFlag("");
				}
				if(rs.getString("DYNAMIC_END_FLAG")!= null){ 
					vObject.setDynamicEndFlag(rs.getString("DYNAMIC_END_FLAG"));
				}else{
					vObject.setDynamicEndFlag("");
				}
				if(rs.getString("DYNAMIC_START_DATE")!= null){ 
					vObject.setDynamicStartDate(rs.getString("DYNAMIC_START_DATE"));
				}else{
					vObject.setDynamicStartDate("");
				}
				if(rs.getString("DYNAMIC_END_DATE")!= null){ 
					vObject.setDynamicEndDate(rs.getString("DYNAMIC_END_DATE"));
				}else{
					vObject.setDynamicEndDate("");
				}
				vObject.setDynamicStartOperator(rs.getString("DYNAMIC_START_OPERATOR"));
				vObject.setDynamicEndOperator(rs.getString("DYNAMIC_END_OPERATOR"));
				if(rs.getString("DYNAMIC_DATE_FORMAT")!= null){ 
					vObject.setDynamicDateFormat(rs.getString("DYNAMIC_DATE_FORMAT"));
				}else{
					vObject.setDynamicDateFormat("");
				}
				if(rs.getString("SUMMARY_CRITERIA")!= null){ 
					vObject.setSummaryCriteria(rs.getString("SUMMARY_CRITERIA"));
				}else{
					vObject.setSummaryCriteria("");
				}
				if(rs.getString("SUMMARY_VALUE_1")!= null){ 
					vObject.setSummaryValue1(rs.getString("SUMMARY_VALUE_1"));
				}else{
					vObject.setSummaryValue1("");
				}
				if(rs.getString("SUMMARY_VALUE_2")!= null){ 
					vObject.setSummaryValue2(rs.getString("SUMMARY_VALUE_2"));
				}else{
					vObject.setSummaryValue2("");
				}
				
				if(rs.getString("SUMMARY_DYNAMIC_START_FLAG")!= null){ 
					vObject.setSummaryStartFlag(rs.getString("SUMMARY_DYNAMIC_START_FLAG"));
				}else{
					vObject.setSummaryStartFlag("");
				}
				if(rs.getString("SUMMARY_DYNAMIC_END_FLAG")!= null){ 
					vObject.setSummaryEndFlag(rs.getString("SUMMARY_DYNAMIC_END_FLAG"));
				}else{
					vObject.setSummaryEndFlag("");
				}
				if(rs.getString("SUMMARY_DYNAMIC_START_DATE")!= null){ 
					vObject.setSummaryStartDate(rs.getString("SUMMARY_DYNAMIC_START_DATE"));
				}else{
					vObject.setSummaryStartDate("");
				}
				if(rs.getString("SUMMARY_DYNAMIC_END_DATE")!= null){ 
					vObject.setSummaryEndDate(rs.getString("SUMMARY_DYNAMIC_END_DATE"));
				}else{
					vObject.setSummaryEndDate("");
				}
				vObject.setSummaryStartOperator(rs.getString("SUMMARY_DYNAMIC_START_OPERATOR"));
				vObject.setSummaryEndOperator(rs.getString("SUMMARY_DYNAMIC_END_OPERATOR"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				if(rs.getString("DATE_LAST_MODIFIED")!= null){ 
					vObject.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				}else{
					vObject.setDateLastModified("");
				}
				if(rs.getString("DATE_CREATION")!= null){ 
					vObject.setDateCreation(rs.getString("DATE_CREATION"));
				}else{
					vObject.setDateCreation("");
				}
				
				vObject.setSampleDataRule(rs.getString("SAMPLE_DATA_RULE"));
				// vObject.setSampleDataCustomRule(rs.getString("SAMPLE_DATA_CUSTOM_RULE"));
				vObject.setSampleDataCustomRule(ValidationUtil.isValid(rs.getString("SAMPLE_DATA_CUSTOM_RULE"))?rs.getString("SAMPLE_DATA_CUSTOM_RULE"):"");
				vObject.setLinkedColumnId(rs.getInt("LINKED_COLUMN_ID"));
				return vObject;
			}
		};
		return mapper;
	}

/*******Mapper End**********/
	public List<EtlExtractionSummaryFieldsVb> getQueryPopupResults(EtlExtractionSummaryFieldsVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TABLE_ID"
			+ ",TAppr.COL_ID"
			+ ",TAppr.USER_ID"
			+ ",TAppr.COL_NAME"
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_TYPE " 
			+ ",TAppr.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TAppr.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TAppr.ALIAS"
			+ ",TAppr.CONDITION_OPERATOR"
			+ ",TAppr.VALUE_1"
			+ ",TAppr.VALUE_2"
			+ ",TAppr.DISPLAY_FLAG"
			+ ",TAppr.SORT_TYPE"
			+ ",TAppr.SORT_ORDER"
			+ ",TAppr.AGG_FUNCTION"
			+ ",TAppr.GROUP_BY"
			+ ",TAppr.COL_DISPLAY_TYPE"
			+ ",TAppr.ETL_FIELDS_STATUS_NT"
			+ ",TAppr.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TAppr.EXPERSSION_TEXT")+" EXPERSSION_TEXT"
			+ ",TAppr.JOIN_CONDITION"
			+ ",TAppr.DYNAMIC_START_FLAG"
			+ ",TAppr.DYNAMIC_END_FLAG"
			+ ",TAppr.DYNAMIC_START_DATE"
			+ ",TAppr.DYNAMIC_END_DATE"
			+ ",TAppr.DYNAMIC_START_OPERATOR"
			+ ",TAppr.DYNAMIC_END_OPERATOR"
			+ ",TAppr.DYNAMIC_DATE_FORMAT"
			+ ",TAppr.SUMMARY_CRITERIA"
			+ ",TAppr.SUMMARY_VALUE_1"
			+ ",TAppr.SUMMARY_VALUE_2"
			+ ",TAppr.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TAppr.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TAppr.SUMMARY_DYNAMIC_START_DATE"
			+ ",TAppr.SUMMARY_DYNAMIC_END_DATE"
			+ ",TAppr.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TAppr.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TAppr.SAMPLE_DATA_RULE, TAppr.SAMPLE_DATA_CUSTOM_RULE, TAppr.LINKED_COLUMN_ID "
			+ " from ETL_EXTRACTION_SUMMARY_FIELDS TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_EXTRACTION_SUMMARY_FIELDS_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.TABLE_ID = TPend.TABLE_ID AND TAppr.COL_ID = TPend.COL_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TABLE_ID"
			+ ",TPend.COL_ID"
			+ ",TPend.USER_ID"
			+ ",TPend.COL_NAME"
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_TYPE " 
			+ ",TPend.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TPend.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TPend.ALIAS"
			+ ",TPend.CONDITION_OPERATOR"
			+ ",TPend.VALUE_1"
			+ ",TPend.VALUE_2"
			+ ",TPend.DISPLAY_FLAG"
			+ ",TPend.SORT_TYPE"
			+ ",TPend.SORT_ORDER"
			+ ",TPend.AGG_FUNCTION"
			+ ",TPend.GROUP_BY"
			+ ",TPend.COL_DISPLAY_TYPE"
			+ ",TPend.ETL_FIELDS_STATUS_NT"
			+ ",TPend.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TPend.EXPERSSION_TEXT")+"  EXPERSSION_TEXT"
			+ ",TPend.JOIN_CONDITION"
			+ ",TPend.DYNAMIC_START_FLAG"
			+ ",TPend.DYNAMIC_END_FLAG"
			+ ",TPend.DYNAMIC_START_DATE"
			+ ",TPend.DYNAMIC_END_DATE"
			+ ",TPend.DYNAMIC_START_OPERATOR"
			+ ",TPend.DYNAMIC_END_OPERATOR"
			+ ",TPend.DYNAMIC_DATE_FORMAT"
			+ ",TPend.SUMMARY_CRITERIA"
			+ ",TPend.SUMMARY_VALUE_1"
			+ ",TPend.SUMMARY_VALUE_2"
			+ ",TPend.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TPend.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TPend.SUMMARY_DYNAMIC_START_DATE"
			+ ",TPend.SUMMARY_DYNAMIC_END_DATE"
			+ ",TPend.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TPend.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TPend.SAMPLE_DATA_RULE, TPend.SAMPLE_DATA_CUSTOM_RULE, TPend.LINKED_COLUMN_ID "
			+ " from ETL_EXTRACTION_SUMMARY_FIELDS_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TABLE_ID"
			+ ",TUpl.COL_ID"
			+ ",TUpl.USER_ID"
			+ ",TUpl.COL_NAME"
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_TYPE " 
			+ ",TUpl.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TUpl.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TUpl.ALIAS"
			+ ",TUpl.CONDITION_OPERATOR"
			+ ",TUpl.VALUE_1"
			+ ",TUpl.VALUE_2"
			+ ",TUpl.DISPLAY_FLAG"
			+ ",TUpl.SORT_TYPE"
			+ ",TUpl.SORT_ORDER"
			+ ",TUpl.AGG_FUNCTION"
			+ ",TUpl.GROUP_BY"
			+ ",TUpl.COL_DISPLAY_TYPE"
			+ ",TUpl.ETL_FIELDS_STATUS_NT"
			+ ",TUpl.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TUpl.EXPERSSION_TEXT")+"  EXPERSSION_TEXT"
			+ ",TUpl.JOIN_CONDITION"
			+ ",TUpl.DYNAMIC_START_FLAG"
			+ ",TUpl.DYNAMIC_END_FLAG"
			+ ",TUpl.DYNAMIC_START_DATE"
			+ ",TUpl.DYNAMIC_END_DATE"
			+ ",TUpl.DYNAMIC_START_OPERATOR"
			+ ",TUpl.DYNAMIC_END_OPERATOR"
			+ ",TUpl.DYNAMIC_DATE_FORMAT"
			+ ",TUpl.SUMMARY_CRITERIA"
			+ ",TUpl.SUMMARY_VALUE_1"
			+ ",TUpl.SUMMARY_VALUE_2"
			+ ",TUpl.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TUpl.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TUpl.SUMMARY_DYNAMIC_START_DATE"
			+ ",TUpl.SUMMARY_DYNAMIC_END_DATE"
			+ ",TUpl.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TUpl.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TUpl.SAMPLE_DATA_RULE, TUpl.SAMPLE_DATA_CUSTOM_RULE, TUpl.LINKED_COLUMN_ID "
			+ " from ETL_EXTRACTION_SUMMARY_FIELDS_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String(
				" Not Exists (Select 'X' From ETL_EXTRACTION_SUMMARY_FIELDS_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.TABLE_ID = TUpl.TABLE_ID AND TAppr.COL_ID = TUpl.COL_ID) ");
		String strWhereNotExistsPendInUpl = new String(
				" Not Exists (Select 'X' From ETL_EXTRACTION_SUMMARY_FIELDS_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID AND TUpl.TABLE_ID = TPend.TABLE_ID AND TUpl.COL_ID = TPend.COL_ID) ");
		try
		{
			if (ValidationUtil.isValid(dObj.getCountry())) {
				params.addElement(dObj.getCountry());
				CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getLeBook())) {
				params.addElement(dObj.getLeBook());
				CommonUtils.addToQuery("TAppr.LE_BOOK = ?", strBufApprove);
				CommonUtils.addToQuery("TPend.LE_BOOK = ?", strBufPending);
				CommonUtils.addToQuery("TUpl.LE_BOOK = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getFeedId())) {
				params.addElement(dObj.getFeedId());
				CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
			}
			String orderBy = " Order By SORT_ORDER ";
			CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);
			CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);
			StringBuffer lPageQuery = new StringBuffer();
			if (dObj.isVerificationRequired())
				lPageQuery.append(strBufPending.toString() + " Union " + strBufUpl.toString());
			else
				lPageQuery.append(strBufApprove.toString() + " Union " + strBufUpl.toString());
			return getQueryPopupResultsPgn(dObj, lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,
					getMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlExtractionSummaryFieldsVb> getQueryResults(EtlExtractionSummaryFieldsVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlExtractionSummaryFieldsVb> collTemp = null;

		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TABLE_ID"
			+ ",TAppr.COL_ID"
			+ ",TAppr.USER_ID"
			+ ",TAppr.COL_NAME"
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_TYPE " 
			+ ",TAppr.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TAppr.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TAppr.ALIAS"
			+ ",TAppr.CONDITION_OPERATOR"
			+ ",TAppr.VALUE_1"
			+ ",TAppr.VALUE_2"
			+ ",TAppr.DISPLAY_FLAG"
			+ ",TAppr.SORT_TYPE"
			+ ",TAppr.SORT_ORDER"
			+ ",TAppr.AGG_FUNCTION"
			+ ",TAppr.GROUP_BY"
			+ ",TAppr.COL_DISPLAY_TYPE"
			+ ",TAppr.ETL_FIELDS_STATUS_NT"
			+ ",TAppr.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TAppr.EXPERSSION_TEXT")+" EXPERSSION_TEXT"
			+ ",TAppr.JOIN_CONDITION"
			+ ",TAppr.DYNAMIC_START_FLAG"
			+ ",TAppr.DYNAMIC_END_FLAG"
			+ ",TAppr.DYNAMIC_START_DATE"
			+ ",TAppr.DYNAMIC_END_DATE"
			+ ",TAppr.DYNAMIC_START_OPERATOR"
			+ ",TAppr.DYNAMIC_END_OPERATOR"
			+ ",TAppr.DYNAMIC_DATE_FORMAT"
			+ ",TAppr.SUMMARY_CRITERIA"
			+ ",TAppr.SUMMARY_VALUE_1"
			+ ",TAppr.SUMMARY_VALUE_2"
			+ ",TAppr.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TAppr.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TAppr.SUMMARY_DYNAMIC_START_DATE"
			+ ",TAppr.SUMMARY_DYNAMIC_END_DATE"
			+ ",TAppr.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TAppr.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TAppr.SAMPLE_DATA_RULE, TAppr.SAMPLE_DATA_CUSTOM_RULE, TAppr.LINKED_COLUMN_ID "
			+ " From ETL_EXTRACTION_SUMMARY_FIELDS TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TABLE_ID"
			+ ",TPend.COL_ID"
			+ ",TPend.USER_ID"
			+ ",TPend.COL_NAME"
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_TYPE " 
			+ ",TPend.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TPend.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TPend.ALIAS"
			+ ",TPend.CONDITION_OPERATOR"
			+ ",TPend.VALUE_1"
			+ ",TPend.VALUE_2"
			+ ",TPend.DISPLAY_FLAG"
			+ ",TPend.SORT_TYPE"
			+ ",TPend.SORT_ORDER"
			+ ",TPend.AGG_FUNCTION"
			+ ",TPend.GROUP_BY"
			+ ",TPend.COL_DISPLAY_TYPE"
			+ ",TPend.ETL_FIELDS_STATUS_NT"
			+ ",TPend.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TPend.EXPERSSION_TEXT")+"  EXPERSSION_TEXT"
			+ ",TPend.DYNAMIC_START_FLAG"
			+ ",TPend.DYNAMIC_END_FLAG"
			+ ",TPend.DYNAMIC_START_DATE"
			+ ",TPend.DYNAMIC_END_DATE"
			+ ",TPend.DYNAMIC_START_OPERATOR"
			+ ",TPend.DYNAMIC_END_OPERATOR"
			+ ",TPend.DYNAMIC_DATE_FORMAT"
			+ ",TPend.SUMMARY_CRITERIA"
			+ ",TPend.SUMMARY_VALUE_1"
			+ ",TPend.SUMMARY_VALUE_2"
			+ ",TPend.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TPend.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TPend.SUMMARY_DYNAMIC_START_DATE"
			+ ",TPend.SUMMARY_DYNAMIC_END_DATE"
			+ ",TPend.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TPend.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TPend.SAMPLE_DATA_RULE, TPend.SAMPLE_DATA_CUSTOM_RULE, TPend.LINKED_COLUMN_ID "
			+ " From ETL_EXTRACTION_SUMMARY_FIELDS_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TABLE_ID"
			+ ",TUpl.COL_ID"
			+ ",TUpl.USER_ID"
			+ ",TUpl.COL_NAME"
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_TYPE " 
			+ ",TUpl.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TUpl.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TUpl.ALIAS"
			+ ",TUpl.CONDITION_OPERATOR"
			+ ",TUpl.VALUE_1"
			+ ",TUpl.VALUE_2"
			+ ",TUpl.DISPLAY_FLAG"
			+ ",TUpl.SORT_TYPE"
			+ ",TUpl.SORT_ORDER"
			+ ",TUpl.AGG_FUNCTION"
			+ ",TUpl.GROUP_BY"
			+ ",TUpl.COL_DISPLAY_TYPE"
			+ ",TUpl.ETL_FIELDS_STATUS_NT"
			+ ",TUpl.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TUpl.EXPERSSION_TEXT")+"  EXPERSSION_TEXT"
			+ ",TUpl.JOIN_CONDITION"
			+ ",TUpl.DYNAMIC_START_FLAG"
			+ ",TUpl.DYNAMIC_END_FLAG"
			+ ",TUpl.DYNAMIC_START_DATE"
			+ ",TUpl.DYNAMIC_END_DATE"
			+ ",TUpl.DYNAMIC_START_OPERATOR"
			+ ",TUpl.DYNAMIC_END_OPERATOR"
			+ ",TUpl.DYNAMIC_DATE_FORMAT"
			+ ",TUpl.SUMMARY_CRITERIA"
			+ ",TUpl.SUMMARY_VALUE_1"
			+ ",TUpl.SUMMARY_VALUE_2"
			+ ",TUpl.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TUpl.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TUpl.SUMMARY_DYNAMIC_START_DATE"
			+ ",TUpl.SUMMARY_DYNAMIC_END_DATE"
			+ ",TUpl.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TUpl.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TUpl.SAMPLE_DATA_RULE, TUpl.SAMPLE_DATA_CUSTOM_RULE, TUpl.LINKED_COLUMN_ID "
			+ " From ETL_EXTRACTION_SUMMARY_FIELDS_UPL TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getMapper());
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getMapper());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getMapper());
			}
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	public List<EtlExtractionSummaryFieldsVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlExtractionSummaryFieldsVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TABLE_ID"
			+ ",TAppr.COL_ID"
			+ ",TAppr.USER_ID"
			+ ",TAppr.COL_NAME"
            + ",(SELECT COLUMN_DATATYPE FROM ETL_FEED_COLUMNS WHERE COLUMN_NAME=TAppr.COL_NAME and COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) COLUMN_DATA_TYPE"  
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_TYPE " 
			+ ",TAppr.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TAppr.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TAppr.ALIAS"
			+ ",TAppr.CONDITION_OPERATOR"
			+ ",TAppr.VALUE_1"
			+ ",TAppr.VALUE_2"
			+ ",TAppr.DISPLAY_FLAG"
			+ ",TAppr.SORT_TYPE"
			+ ",TAppr.SORT_ORDER"
			+ ",TAppr.AGG_FUNCTION"
			+ ",TAppr.GROUP_BY"
			+ ",TAppr.COL_DISPLAY_TYPE"
			+ ",TAppr.ETL_FIELDS_STATUS_NT"
			+ ",TAppr.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TAppr.EXPERSSION_TEXT")+" EXPERSSION_TEXT"
			+ ",TAppr.JOIN_CONDITION"
			+ ",TAppr.DYNAMIC_START_FLAG"
			+ ",TAppr.DYNAMIC_END_FLAG"
			+ ",TAppr.DYNAMIC_START_DATE"
			+ ",TAppr.DYNAMIC_END_DATE"
			+ ",TAppr.DYNAMIC_START_OPERATOR"
			+ ",TAppr.DYNAMIC_END_OPERATOR"
			+ ",TAppr.DYNAMIC_DATE_FORMAT"
			+ ",TAppr.SUMMARY_CRITERIA"
			+ ",TAppr.SUMMARY_VALUE_1"
			+ ",TAppr.SUMMARY_VALUE_2"
			+ ",TAppr.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TAppr.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TAppr.SUMMARY_DYNAMIC_START_DATE"
			+ ",TAppr.SUMMARY_DYNAMIC_END_DATE"
			+ ",TAppr.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TAppr.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+" (TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TAppr.SAMPLE_DATA_RULE, TAppr.SAMPLE_DATA_CUSTOM_RULE, TAppr.LINKED_COLUMN_ID "
			+ " From ETL_EXTRACTION_SUMMARY_FIELDS TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ORDER BY TAppr.SORT_ORDER");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TABLE_ID"
			+ ",TPend.COL_ID"
			+ ",TPend.USER_ID"
			+ ",TPend.COL_NAME"
            + ",(SELECT COLUMN_DATATYPE FROM ETL_FEED_COLUMNS_PEND WHERE COLUMN_NAME=TPend.COL_NAME and COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) COLUMN_DATA_TYPE"  
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES_PEND WHERE COUNTRY=TPend.COUNTRY AND LE_BOOK=TPend.LE_BOOK AND FEED_ID=TPend.FEED_ID AND TABLE_ID= TPend.TABLE_ID) TABLE_TYPE " 
			+ ",TPend.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TPend.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TPend.ALIAS"
			+ ",TPend.CONDITION_OPERATOR"
			+ ",TPend.VALUE_1"
			+ ",TPend.VALUE_2"
			+ ",TPend.DISPLAY_FLAG"
			+ ",TPend.SORT_TYPE"
			+ ",TPend.SORT_ORDER"
			+ ",TPend.AGG_FUNCTION"
			+ ",TPend.GROUP_BY"
			+ ",TPend.COL_DISPLAY_TYPE"
			+ ",TPend.ETL_FIELDS_STATUS_NT"
			+ ",TPend.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TPend.EXPERSSION_TEXT")+"  EXPERSSION_TEXT"
			+ ",TPend.JOIN_CONDITION"
			+ ",TPend.DYNAMIC_START_FLAG"
			+ ",TPend.DYNAMIC_END_FLAG"
			+ ",TPend.DYNAMIC_START_DATE"
			+ ",TPend.DYNAMIC_END_DATE"
			+ ",TPend.DYNAMIC_START_OPERATOR"
			+ ",TPend.DYNAMIC_END_OPERATOR"
			+ ",TPend.DYNAMIC_DATE_FORMAT"
			+ ",TPend.SUMMARY_CRITERIA"
			+ ",TPend.SUMMARY_VALUE_1"
			+ ",TPend.SUMMARY_VALUE_2"
			+ ",TPend.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TPend.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TPend.SUMMARY_DYNAMIC_START_DATE"
			+ ",TPend.SUMMARY_DYNAMIC_END_DATE"
			+ ",TPend.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TPend.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TPend.SAMPLE_DATA_RULE, TPend.SAMPLE_DATA_CUSTOM_RULE, TPend.LINKED_COLUMN_ID "
			+ " From ETL_EXTRACTION_SUMMARY_FIELDS_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ORDER BY TPend.SORT_ORDER");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TABLE_ID"
			+ ",TUpl.COL_ID"
			+ ",TUpl.USER_ID"
			+ ",TUpl.COL_NAME"
            + ",(SELECT COLUMN_DATATYPE FROM ETL_FEED_COLUMNS_UPL WHERE COLUMN_NAME=TUpl.COL_NAME and COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) COLUMN_DATA_TYPE"  
			+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) SOURCE_CONNECTOR_ID " 
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_NAME " 
			+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_ALIAS_NAME " 
			+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES_UPL WHERE COUNTRY=TUpl.COUNTRY AND LE_BOOK=TUpl.LE_BOOK AND FEED_ID=TUpl.FEED_ID AND TABLE_ID= TUpl.TABLE_ID) TABLE_TYPE " 
			+ ",TUpl.DATE_FORMAT FORMAT_TYPE"
			+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) FORMAT_TYPE_DESC " 
			+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TUpl.DATE_FORMAT) JAVA_DATE_FORMAT " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
			+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
			+ ",TUpl.ALIAS"
			+ ",TUpl.CONDITION_OPERATOR"
			+ ",TUpl.VALUE_1"
			+ ",TUpl.VALUE_2"
			+ ",TUpl.DISPLAY_FLAG"
			+ ",TUpl.SORT_TYPE"
			+ ",TUpl.SORT_ORDER"
			+ ",TUpl.AGG_FUNCTION"
			+ ",TUpl.GROUP_BY"
			+ ",TUpl.COL_DISPLAY_TYPE"
			+ ",TUpl.ETL_FIELDS_STATUS_NT"
			+ ",TUpl.ETL_FIELDS_STATUS"
			+ ", "+getDbFunction(Constants.TO_CHAR, "TUpl.EXPERSSION_TEXT")+"  EXPERSSION_TEXT"
			+ ",TUpl.JOIN_CONDITION"
			+ ",TUpl.DYNAMIC_START_FLAG"
			+ ",TUpl.DYNAMIC_END_FLAG"
			+ ",TUpl.DYNAMIC_START_DATE"
			+ ",TUpl.DYNAMIC_END_DATE"
			+ ",TUpl.DYNAMIC_START_OPERATOR"
			+ ",TUpl.DYNAMIC_END_OPERATOR"
			+ ",TUpl.DYNAMIC_DATE_FORMAT"
			+ ",TUpl.SUMMARY_CRITERIA"
			+ ",TUpl.SUMMARY_VALUE_1"
			+ ",TUpl.SUMMARY_VALUE_2"
			+ ",TUpl.SUMMARY_DYNAMIC_START_FLAG"
			+ ",TUpl.SUMMARY_DYNAMIC_END_FLAG"
			+ ",TUpl.SUMMARY_DYNAMIC_START_DATE"
			+ ",TUpl.SUMMARY_DYNAMIC_END_DATE"
			+ ",TUpl.SUMMARY_DYNAMIC_START_OPERATOR"
			+ ",TUpl.SUMMARY_DYNAMIC_END_OPERATOR"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ ", TUpl.SAMPLE_DATA_RULE, TUpl.SAMPLE_DATA_CUSTOM_RULE, TUpl.LINKED_COLUMN_ID "
			+ " From ETL_EXTRACTION_SUMMARY_FIELDS_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ORDER BY TUpl.SORT_ORDER");

	//	String orderby = " ORDER BY TABLE_ID, COL_ID ";
		
		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getMapper());
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getMapper());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getMapper());
			}
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	@Override
	protected List<EtlExtractionSummaryFieldsVb> selectApprovedRecord(EtlExtractionSummaryFieldsVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<EtlExtractionSummaryFieldsVb> doSelectPendingRecord(EtlExtractionSummaryFieldsVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}


	@Override
	protected List<EtlExtractionSummaryFieldsVb> doSelectUplRecord(EtlExtractionSummaryFieldsVb vObject){
		return getQueryResults(vObject, 9999);
	}


	@Override
	protected int getStatus(EtlExtractionSummaryFieldsVb records){return records.getEtlFieldsStatus();}

	@Override
	protected void setStatus(EtlExtractionSummaryFieldsVb vObject,int status){vObject.setEtlFieldsStatus(status);}

	@Override
	protected int doInsertionAppr(EtlExtractionSummaryFieldsVb vObject){
		String query = "Insert Into ETL_EXTRACTION_SUMMARY_FIELDS (COUNTRY, LE_BOOK, FEED_ID, TABLE_ID, COL_ID, USER_ID, "
				+ " COL_NAME, ALIAS, CONDITION_OPERATOR, VALUE_1, VALUE_2, DISPLAY_FLAG, SORT_TYPE, SORT_ORDER, AGG_FUNCTION,"
				+ " GROUP_BY,  COL_DISPLAY_TYPE, date_format,  "
				+ "ETL_FIELDS_STATUS_NT, ETL_FIELDS_STATUS,  JOIN_CONDITION,  "
				+ " DYNAMIC_START_FLAG, DYNAMIC_END_FLAG, DYNAMIC_START_DATE, DYNAMIC_END_DATE, DYNAMIC_START_OPERATOR, "
				+ "DYNAMIC_END_OPERATOR, DYNAMIC_DATE_FORMAT,SUMMARY_CRITERIA, SUMMARY_VALUE_1,SUMMARY_VALUE_2,"
				+ " SUMMARY_DYNAMIC_START_FLAG, SUMMARY_DYNAMIC_END_FLAG, SUMMARY_DYNAMIC_START_DATE, SUMMARY_DYNAMIC_END_DATE, SUMMARY_DYNAMIC_START_OPERATOR, "
				+ "SUMMARY_DYNAMIC_END_OPERATOR,"
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, "
				+ "DATE_CREATION, SAMPLE_DATA_RULE, SAMPLE_DATA_CUSTOM_RULE, LINKED_COLUMN_ID, EXPERSSION_TEXT )"
				+ "Values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getColId(), vObject.getUserId(), vObject.getColName(), vObject.getAlias(),
				vObject.getConditionOperator(), vObject.getValue1(), vObject.getValue2(), vObject.getDisplayFlag(),
				vObject.getSortType(), vObject.getSortOrder(), vObject.getAggFunction(), vObject.getGroupBy(),
				vObject.getColDisplayType(), vObject.getDateFormat(), vObject.getEtlFieldsStatusNt(),
				vObject.getEtlFieldsStatus(), vObject.getJoinCondition(), vObject.getDynamicStartFlag(),
				vObject.getDynamicEndFlag(), vObject.getDynamicStartDate(), vObject.getDynamicEndDate(),
				vObject.getDynamicStartOperator(), vObject.getDynamicEndOperator(), vObject.getDynamicDateFormat(),
				vObject.getSummaryCriteria(), vObject.getSummaryValue1(), vObject.getSummaryValue2(),
				vObject.getSummaryStartFlag(), vObject.getSummaryEndFlag(), vObject.getSummaryStartDate(),
				vObject.getSummaryEndDate(), vObject.getSummaryStartOperator(), vObject.getSummaryEndOperator(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getSampleDataRule(), vObject.getSampleDataCustomRule(),
				vObject.getLinkedColumnId() };
		
		
		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getExperssionText()) ? vObject.getExperssionText()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	@Override
	protected int doInsertionPend(EtlExtractionSummaryFieldsVb vObject){
		String query = "Insert Into ETL_EXTRACTION_SUMMARY_FIELDS_PEND (COUNTRY, LE_BOOK, FEED_ID, TABLE_ID, COL_ID, USER_ID, "
				+ " COL_NAME, ALIAS, CONDITION_OPERATOR, VALUE_1, VALUE_2, DISPLAY_FLAG, SORT_TYPE, SORT_ORDER, AGG_FUNCTION,"
				+ " GROUP_BY,  COL_DISPLAY_TYPE, date_format,   "
				+ "ETL_FIELDS_STATUS_NT, ETL_FIELDS_STATUS,  JOIN_CONDITION,  "
				+ " DYNAMIC_START_FLAG, DYNAMIC_END_FLAG, DYNAMIC_START_DATE, DYNAMIC_END_DATE, DYNAMIC_START_OPERATOR, "
				+ "DYNAMIC_END_OPERATOR, DYNAMIC_DATE_FORMAT,SUMMARY_CRITERIA, SUMMARY_VALUE_1,SUMMARY_VALUE_2,"
				+ " SUMMARY_DYNAMIC_START_FLAG, SUMMARY_DYNAMIC_END_FLAG, SUMMARY_DYNAMIC_START_DATE, SUMMARY_DYNAMIC_END_DATE, SUMMARY_DYNAMIC_START_OPERATOR, "
				+ "SUMMARY_DYNAMIC_END_OPERATOR,"
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, "
				+ "DATE_CREATION, SAMPLE_DATA_RULE, SAMPLE_DATA_CUSTOM_RULE, LINKED_COLUMN_ID, EXPERSSION_TEXT )"
				+ "Values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getColId(), vObject.getUserId(), vObject.getColName(), vObject.getAlias(),
				vObject.getConditionOperator(), vObject.getValue1(), vObject.getValue2(), vObject.getDisplayFlag(),
				vObject.getSortType(), vObject.getSortOrder(), vObject.getAggFunction(), vObject.getGroupBy(),
				vObject.getColDisplayType(), vObject.getDateFormat(), vObject.getEtlFieldsStatusNt(),
				vObject.getEtlFieldsStatus(), vObject.getJoinCondition(), vObject.getDynamicStartFlag(),
				vObject.getDynamicEndFlag(), vObject.getDynamicStartDate(), vObject.getDynamicEndDate(),
				vObject.getDynamicStartOperator(), vObject.getDynamicEndOperator(), vObject.getDynamicDateFormat(),
				vObject.getSummaryCriteria(), vObject.getSummaryValue1(), vObject.getSummaryValue2(),
				vObject.getSummaryStartFlag(), vObject.getSummaryEndFlag(), vObject.getSummaryStartDate(),
				vObject.getSummaryEndDate(), vObject.getSummaryStartOperator(), vObject.getSummaryEndOperator(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getSampleDataRule(), vObject.getSampleDataCustomRule(),
				vObject.getLinkedColumnId() };

		int result=0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getExperssionText()) ? vObject.getExperssionText()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}
	
	@Override
	protected int doInsertionUpl(EtlExtractionSummaryFieldsVb vObject){
		String query = "Insert Into ETL_EXTRACTION_SUMMARY_FIELDS_UPL (COUNTRY, LE_BOOK, FEED_ID, TABLE_ID, COL_ID, USER_ID, "
				+ " COL_NAME, ALIAS, CONDITION_OPERATOR, VALUE_1, VALUE_2, DISPLAY_FLAG, SORT_TYPE, SORT_ORDER, AGG_FUNCTION,"
				+ "  GROUP_BY,  COL_DISPLAY_TYPE, date_format,   "
				+ "ETL_FIELDS_STATUS_NT, ETL_FIELDS_STATUS,  JOIN_CONDITION,  "
				+ " DYNAMIC_START_FLAG, DYNAMIC_END_FLAG, DYNAMIC_START_DATE, DYNAMIC_END_DATE, DYNAMIC_START_OPERATOR, "
				+ "DYNAMIC_END_OPERATOR, DYNAMIC_DATE_FORMAT,SUMMARY_CRITERIA, SUMMARY_VALUE_1,SUMMARY_VALUE_2,"
				+ " SUMMARY_DYNAMIC_START_FLAG, SUMMARY_DYNAMIC_END_FLAG, SUMMARY_DYNAMIC_START_DATE, SUMMARY_DYNAMIC_END_DATE, SUMMARY_DYNAMIC_START_OPERATOR, "
				+ "SUMMARY_DYNAMIC_END_OPERATOR,"
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, "
				+ "DATE_CREATION, SAMPLE_DATA_RULE, SAMPLE_DATA_CUSTOM_RULE, LINKED_COLUMN_ID, EXPERSSION_TEXT )"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getColId(), vObject.getUserId(), vObject.getColName(), vObject.getAlias(),
				vObject.getConditionOperator(), vObject.getValue1(), vObject.getValue2(), vObject.getDisplayFlag(),
				vObject.getSortType(), vObject.getSortOrder(), vObject.getAggFunction(), vObject.getGroupBy(),
				vObject.getColDisplayType(), vObject.getDateFormat(), vObject.getEtlFieldsStatusNt(),
				vObject.getEtlFieldsStatus(), vObject.getJoinCondition(), vObject.getDynamicStartFlag(),
				vObject.getDynamicEndFlag(), vObject.getDynamicStartDate(), vObject.getDynamicEndDate(),
				vObject.getDynamicStartOperator(), vObject.getDynamicEndOperator(), vObject.getDynamicDateFormat(),
				vObject.getSummaryCriteria(), vObject.getSummaryValue1(), vObject.getSummaryValue2(),
				vObject.getSummaryStartFlag(), vObject.getSummaryEndFlag(), vObject.getSummaryStartDate(),
				vObject.getSummaryEndDate(), vObject.getSummaryStartOperator(), vObject.getSummaryEndOperator(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getSampleDataRule(), vObject.getSampleDataCustomRule(),
				vObject.getLinkedColumnId() };

		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getExperssionText()) ? vObject.getExperssionText()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					return ps;
				}
			});

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}


	@Override
	protected int doInsertionPendWithDc(EtlExtractionSummaryFieldsVb vObject){
		String query = "Insert Into ETL_EXTRACTION_SUMMARY_FIELDS_UPL (COUNTRY, LE_BOOK, FEED_ID, TABLE_ID, COL_ID, USER_ID, "
				+ " COL_NAME, ALIAS, CONDITION_OPERATOR, VALUE_1, VALUE_2, DISPLAY_FLAG, SORT_TYPE, SORT_ORDER, AGG_FUNCTION,"
				+ " GROUP_BY,  COL_DISPLAY_TYPE, date_format,   "
				+ "ETL_FIELDS_STATUS_NT, ETL_FIELDS_STATUS,  JOIN_CONDITION,  "
				+ " DYNAMIC_START_FLAG, DYNAMIC_END_FLAG, DYNAMIC_START_DATE, DYNAMIC_END_DATE, DYNAMIC_START_OPERATOR, "
				+ "DYNAMIC_END_OPERATOR, DYNAMIC_DATE_FORMAT,SUMMARY_CRITERIA, SUMMARY_VALUE_1,SUMMARY_VALUE_2,"
				+ " SUMMARY_DYNAMIC_START_FLAG, SUMMARY_DYNAMIC_END_FLAG, SUMMARY_DYNAMIC_START_DATE, SUMMARY_DYNAMIC_END_DATE, SUMMARY_DYNAMIC_START_OPERATOR, "
				+ "SUMMARY_DYNAMIC_END_OPERATOR,"
				+ " RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, "
				+ "DATE_CREATION, SAMPLE_DATA_RULE, SAMPLE_DATA_CUSTOM_RULE, LINKED_COLUMN_ID, EXPERSSION_TEXT )"
				+ "Values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ "SysDate, " + getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + ", ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getColId(), vObject.getUserId(), vObject.getColName(), vObject.getAlias(),
				vObject.getConditionOperator(), vObject.getValue1(), vObject.getValue2(), vObject.getDisplayFlag(),
				vObject.getSortType(), vObject.getSortOrder(), vObject.getAggFunction(), vObject.getGroupBy(),
				vObject.getColDisplayType(), vObject.getDateFormat(), vObject.getEtlFieldsStatusNt(),
				vObject.getEtlFieldsStatus(), vObject.getJoinCondition(), vObject.getDynamicStartFlag(),
				vObject.getDynamicEndFlag(), vObject.getDynamicStartDate(), vObject.getDynamicEndDate(),
				vObject.getDynamicStartOperator(), vObject.getDynamicEndOperator(), vObject.getDynamicDateFormat(),
				vObject.getSummaryCriteria(), vObject.getSummaryValue1(), vObject.getSummaryValue2(),
				vObject.getSummaryStartFlag(), vObject.getSummaryEndFlag(), vObject.getSummaryStartDate(),
				vObject.getSummaryEndDate(), vObject.getSummaryStartOperator(), vObject.getSummaryEndOperator(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getSampleDataRule(), vObject.getSampleDataCustomRule(),
				vObject.getLinkedColumnId() };

		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getExperssionText()) ? vObject.getExperssionText()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});

		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	
	}


	@Override
	protected int doUpdateAppr(EtlExtractionSummaryFieldsVb vObject){
	String query = "Update ETL_EXTRACTION_SUMMARY_FIELDS Set EXPERSSION_TEXT = ?, USER_ID = ?,  COL_NAME = ?, ALIAS = ?, CONDITION_OPERATOR = ?, VALUE_1 = ?, "
			+ "VALUE_2 = ?, DISPLAY_FLAG = ?, SORT_TYPE = ?, SORT_ORDER = ?, AGG_FUNCTION = ?, GROUP_BY = ?, "
			+ " COL_DISPLAY_TYPE = ?,date_format =?,  ETL_FIELDS_STATUS_NT = ?, ETL_FIELDS_STATUS = ?,  JOIN_CONDITION = ?,"
			+ " DYNAMIC_START_FLAG = ?, DYNAMIC_END_FLAG = ?, DYNAMIC_START_DATE = ?, "
			+ " DYNAMIC_END_DATE = ?, DYNAMIC_START_OPERATOR = ?, DYNAMIC_END_OPERATOR = ?, DYNAMIC_DATE_FORMAT = ?,"
			+ " SUMMARY_CRITERIA =?, SUMMARY_VALUE_1=?,SUMMARY_VALUE_2=?,"
			+ " SUMMARY_DYNAMIC_START_FLAG = ?, SUMMARY_DYNAMIC_END_FLAG = ?, SUMMARY_DYNAMIC_START_DATE = ?, "
			+ "SUMMARY_DYNAMIC_END_DATE = ?, SUMMARY_DYNAMIC_START_OPERATOR = ?, SUMMARY_DYNAMIC_END_OPERATOR = ?,"
			+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, "
			+ "VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+", SAMPLE_DATA_RULE = ?, SAMPLE_DATA_CUSTOM_RULE = ?, LINKED_COLUMN_ID = ? "
			+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TABLE_ID = ?  AND COL_ID = ? ";
		Object[] args = { vObject.getUserId(), vObject.getColName(), vObject.getAlias(), vObject.getConditionOperator(),
				vObject.getValue1(), vObject.getValue2(), vObject.getDisplayFlag(), vObject.getSortType(),
				vObject.getSortOrder(), vObject.getAggFunction(), vObject.getGroupBy(), vObject.getColDisplayType(),
				vObject.getDateFormat(), vObject.getEtlFieldsStatusNt(), vObject.getEtlFieldsStatus(),
				vObject.getJoinCondition(), vObject.getDynamicStartFlag(), vObject.getDynamicEndFlag(),
				vObject.getDynamicStartDate(), vObject.getDynamicEndDate(), vObject.getDynamicStartOperator(),
				vObject.getDynamicEndOperator(), vObject.getDynamicDateFormat(), vObject.getSummaryCriteria(),
				vObject.getSummaryValue1(), vObject.getSummaryValue2(), vObject.getSummaryStartFlag(),
				vObject.getSummaryEndFlag(), vObject.getSummaryStartDate(), vObject.getSummaryEndDate(),
				vObject.getSummaryStartOperator(), vObject.getSummaryEndOperator(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getSampleDataRule(), vObject.getSampleDataCustomRule(), vObject.getLinkedColumnId(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getColId() };

		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getExperssionText()) ? vObject.getExperssionText()
							: "";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}

					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	@Override
	protected int doUpdatePend(EtlExtractionSummaryFieldsVb vObject) {
		String query = "Update ETL_EXTRACTION_SUMMARY_FIELDS_PEND Set EXPERSSION_TEXT = ?, USER_ID = ?,  COL_NAME = ?, ALIAS = ?, CONDITION_OPERATOR = ?, VALUE_1 = ?, "
				+ "VALUE_2 = ?, DISPLAY_FLAG = ?, SORT_TYPE = ?, SORT_ORDER = ?, AGG_FUNCTION = ?,  GROUP_BY = ?, "
				+ " COL_DISPLAY_TYPE = ?, date_format =?, ETL_FIELDS_STATUS_NT = ?, ETL_FIELDS_STATUS = ?,  JOIN_CONDITION = ?,"
				+ " DYNAMIC_START_FLAG = ?, DYNAMIC_END_FLAG = ?, DYNAMIC_START_DATE = ?, "
				+ "DYNAMIC_END_DATE = ?, DYNAMIC_START_OPERATOR = ?, DYNAMIC_END_OPERATOR = ?, DYNAMIC_DATE_FORMAT = ?,"
				+ " SUMMARY_CRITERIA =?, SUMMARY_VALUE_1=?,SUMMARY_VALUE_2=?,"
				+ " SUMMARY_DYNAMIC_START_FLAG = ?, SUMMARY_DYNAMIC_END_FLAG = ?, SUMMARY_DYNAMIC_START_DATE = ?, "
				+ "SUMMARY_DYNAMIC_END_DATE = ?, SUMMARY_DYNAMIC_START_OPERATOR = ?, SUMMARY_DYNAMIC_END_OPERATOR = ?,"
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, "
				+ "VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ ", SAMPLE_DATA_RULE = ?, SAMPLE_DATA_CUSTOM_RULE = ?, LINKED_COLUMN_ID=? "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TABLE_ID = ?  AND COL_ID = ? ";
		Object[] args = { vObject.getUserId(), vObject.getColName(), vObject.getAlias(), vObject.getConditionOperator(),
				vObject.getValue1(), vObject.getValue2(), vObject.getDisplayFlag(), vObject.getSortType(),
				vObject.getSortOrder(), vObject.getAggFunction(), vObject.getGroupBy(), vObject.getColDisplayType(),
				vObject.getDateFormat(), vObject.getEtlFieldsStatusNt(), vObject.getEtlFieldsStatus(),
				vObject.getJoinCondition(), vObject.getDynamicStartFlag(), vObject.getDynamicEndFlag(),
				vObject.getDynamicStartDate(), vObject.getDynamicEndDate(), vObject.getDynamicStartOperator(),
				vObject.getDynamicEndOperator(), vObject.getDynamicDateFormat(), vObject.getSummaryCriteria(),
				vObject.getSummaryValue1(), vObject.getSummaryValue2(), vObject.getSummaryStartFlag(),
				vObject.getSummaryEndFlag(), vObject.getSummaryStartDate(), vObject.getSummaryEndDate(),
				vObject.getSummaryStartOperator(), vObject.getSummaryEndOperator(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getSampleDataRule(), vObject.getSampleDataCustomRule(), vObject.getLinkedColumnId(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getColId() };

		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getExperssionText()) ? vObject.getExperssionText()
							: "";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}

					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}


	@Override
	protected int doDeleteAppr(EtlExtractionSummaryFieldsVb vObject){
		String query = "Delete From ETL_EXTRACTION_SUMMARY_FIELDS Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deletePendingRecord(EtlExtractionSummaryFieldsVb vObject){
		String query = "Delete From ETL_EXTRACTION_SUMMARY_FIELDS_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deleteUplRecord(EtlExtractionSummaryFieldsVb vObject){
		String query = "Delete From ETL_EXTRACTION_SUMMARY_FIELDS_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}
	
	protected int deleteUplRecordByParent(EtlFeedMainVb vObject){
		String query = "Delete From ETL_EXTRACTION_SUMMARY_FIELDS_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}
	
	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_EXTRACTION_SUMMARY_FIELDS";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_EXTRACTION_SUMMARY_FIELDS_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_EXTRACTION_SUMMARY_FIELDS_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_EXTRACTION_SUMMARY_FIELDS_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_EXTRACTION_SUMMARY_FIELDS";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_EXTRACTION_SUMMARY_FIELDS_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_EXTRACTION_SUMMARY_FIELDS_PEND";
		}
		String query = "update "+table+" set ETL_FIELDS_STATUS = ?,RECORD_INDICATOR =?  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(),vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String getAuditString(EtlExtractionSummaryFieldsVb vObject) {
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");
		if (ValidationUtil.isValid(vObject.getCountry()))
			strAudit.append("COUNTRY" + auditDelimiterColVal + vObject.getCountry().trim());
		else
			strAudit.append("COUNTRY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("LE_BOOK" + auditDelimiterColVal + vObject.getLeBook());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFeedId()))
			strAudit.append("FEED_ID" + auditDelimiterColVal + vObject.getFeedId().trim());
		else
			strAudit.append("FEED_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getTableId()))
			strAudit.append("TABLE_ID" + auditDelimiterColVal + vObject.getTableId().trim());
		else
			strAudit.append("TABLE_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COL_ID" + auditDelimiterColVal + vObject.getColId());
		strAudit.append(auditDelimiter);

		strAudit.append("USER_ID" + auditDelimiterColVal + vObject.getUserId());
		strAudit.append(auditDelimiter);

		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColName()))
			strAudit.append("COL_NAME" + auditDelimiterColVal + vObject.getColName().trim());
		else
			strAudit.append("COL_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getAlias()))
			strAudit.append("ALIAS" + auditDelimiterColVal + vObject.getAlias().trim());
		else
			strAudit.append("ALIAS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getConditionOperator()))
			strAudit.append("CONDITION_OPERATOR" + auditDelimiterColVal + vObject.getConditionOperator().trim());
		else
			strAudit.append("CONDITION_OPERATOR" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getValue1()))
			strAudit.append("VALUE_1" + auditDelimiterColVal + vObject.getValue1().trim());
		else
			strAudit.append("VALUE_1" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getValue2()))
			strAudit.append("VALUE_2" + auditDelimiterColVal + vObject.getValue2().trim());
		else
			strAudit.append("VALUE_2" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDisplayFlag()))
			strAudit.append("DISPLAY_FLAG" + auditDelimiterColVal + vObject.getDisplayFlag().trim());
		else
			strAudit.append("DISPLAY_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSortType()))
			strAudit.append("SORT_TYPE" + auditDelimiterColVal + vObject.getSortType().trim());
		else
			strAudit.append("SORT_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("SORT_ORDER" + auditDelimiterColVal + vObject.getSortOrder());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getAggFunction()))
			strAudit.append("AGG_FUNCTION" + auditDelimiterColVal + vObject.getAggFunction().trim());
		else
			strAudit.append("AGG_FUNCTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getGroupBy()))
			strAudit.append("GROUP_BY" + auditDelimiterColVal + vObject.getGroupBy().trim());
		else
			strAudit.append("GROUP_BY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColDisplayType()))
			strAudit.append("COL_DISPLAY_TYPE" + auditDelimiterColVal + vObject.getColDisplayType().trim());
		else
			strAudit.append("COL_DISPLAY_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_FIELDS_STATUS_NT" + auditDelimiterColVal + vObject.getEtlFieldsStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_FIELDS_STATUS" + auditDelimiterColVal + vObject.getEtlFieldsStatus());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getExperssionText()))
			strAudit.append("EXPERSSION_TEXT" + auditDelimiterColVal + vObject.getExperssionText().trim());
		else
			strAudit.append("EXPERSSION_TEXT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getJoinCondition()))
			strAudit.append("JOIN_CONDITION" + auditDelimiterColVal + vObject.getJoinCondition().trim());
		else
			strAudit.append("JOIN_CONDITION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDynamicStartFlag()))
			strAudit.append("DYNAMIC_START_FLAG" + auditDelimiterColVal + vObject.getDynamicStartFlag().trim());
		else
			strAudit.append("DYNAMIC_START_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDynamicEndFlag()))
			strAudit.append("DYNAMIC_END_FLAG" + auditDelimiterColVal + vObject.getDynamicEndFlag().trim());
		else
			strAudit.append("DYNAMIC_END_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDynamicStartDate()))
			strAudit.append("DYNAMIC_START_DATE" + auditDelimiterColVal + vObject.getDynamicStartDate().trim());
		else
			strAudit.append("DYNAMIC_START_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDynamicEndDate()))
			strAudit.append("DYNAMIC_END_DATE" + auditDelimiterColVal + vObject.getDynamicEndDate().trim());
		else
			strAudit.append("DYNAMIC_END_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("DYNAMIC_START_OPERATOR" + auditDelimiterColVal + vObject.getDynamicStartOperator());
		strAudit.append(auditDelimiter);

		strAudit.append("DYNAMIC_END_OPERATOR" + auditDelimiterColVal + vObject.getDynamicEndOperator());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDynamicDateFormat()))
			strAudit.append("DYNAMIC_DATE_FORMAT" + auditDelimiterColVal + vObject.getDynamicDateFormat().trim());
		else
			strAudit.append("DYNAMIC_DATE_FORMAT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSummaryCriteria()))
			strAudit.append("SUMMARY_CRITERIA" + auditDelimiterColVal + vObject.getSummaryCriteria().trim());
		else
			strAudit.append("SUMMARY_CRITERIA" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSummaryValue1()))
			strAudit.append("SUMMARY_VALUE_1" + auditDelimiterColVal + vObject.getSummaryValue1().trim());
		else
			strAudit.append("SUMMARY_VALUE_1" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSummaryValue2()))
			strAudit.append("SUMMARY_VALUE_2" + auditDelimiterColVal + vObject.getSummaryValue2().trim());
		else
			strAudit.append("SUMMARY_VALUE_2" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSummaryStartFlag()))
			strAudit.append("SUMMARY_DYNAMIC_START_FLAG" + auditDelimiterColVal + vObject.getSummaryStartFlag().trim());
		else
			strAudit.append("SUMMARY_DYNAMIC_START_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSummaryEndFlag()))
			strAudit.append("SUMMARY_DYNAMIC_END_FLAG" + auditDelimiterColVal + vObject.getSummaryEndFlag().trim());
		else
			strAudit.append("SUMMARY_DYNAMIC_END_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSummaryStartDate()))
			strAudit.append("SUMMARY_DYNAMIC_START_DATE" + auditDelimiterColVal + vObject.getSummaryStartDate().trim());
		else
			strAudit.append("SUMMARY_DYNAMIC_START_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSummaryEndDate()))
			strAudit.append("SUMMARY_DYNAMIC_END_DATE" + auditDelimiterColVal + vObject.getSummaryEndDate().trim());
		else
			strAudit.append("SUMMARY_DYNAMIC_END_DATE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("SUMMARY_DYNAMIC_START_OPERATOR" + auditDelimiterColVal + vObject.getSummaryStartOperator());
		strAudit.append(auditDelimiter);

		strAudit.append("SUMMARY_DYNAMIC_END_OPERATOR" + auditDelimiterColVal + vObject.getSummaryEndOperator());
		strAudit.append(auditDelimiter);

		strAudit.append("RECORD_INDICATOR_NT" + auditDelimiterColVal + vObject.getRecordIndicatorNt());
		strAudit.append(auditDelimiter);

		strAudit.append("RECORD_INDICATOR" + auditDelimiterColVal + vObject.getRecordIndicator());
		strAudit.append(auditDelimiter);

		strAudit.append("MAKER" + auditDelimiterColVal + vObject.getMaker());
		strAudit.append(auditDelimiter);

		strAudit.append("VERIFIER" + auditDelimiterColVal + vObject.getVerifier());
		strAudit.append(auditDelimiter);

		strAudit.append("INTERNAL_STATUS" + auditDelimiterColVal + vObject.getInternalStatus());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateLastModified()))
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + vObject.getDateLastModified().trim());
		else
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateCreation()))
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + vObject.getDateCreation().trim());
		else
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSampleDataRule()))
			strAudit.append("SAMPLE_DATA_RULE" + auditDelimiterColVal + vObject.getSampleDataRule().trim());
		else
			strAudit.append("SAMPLE_DATA_RULE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSampleDataCustomRule()))
			strAudit.append(
					"SAMPLE_DATA_CUSTOM_RULE" + auditDelimiterColVal + vObject.getSampleDataCustomRule().trim());
		else
			strAudit.append("SAMPLE_DATA_CUSTOM_RULE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateFormat()))
			strAudit.append("DATE_FORMAT" + auditDelimiterColVal + vObject.getDateFormat().trim());
		else
			strAudit.append("DATE_FORMAT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		return strAudit.toString();
	}

	@Override
	public void setServiceDefaults(){
		serviceName = "EtlExtractionSummaryFields";
		serviceDesc = "ETL Extraction Summary Fields";
		tableName = "ETL_EXTRACTION_SUMMARY_FIELDS";
		childTableName = "ETL_EXTRACTION_SUMMARY_FIELDS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		
	}

	public String getMaxOfReportNamePerUser(String tableName) {
		/* ORACLE QUERY
		 * 
		 * Long maxOfReportId = getJdbcTemplate().queryForObject("select MAX(To_NUMBER(substr(REPORT_ID,6,length(REPORT_ID)))) from "+tableName+" ", Long.class);*/
		
		Long maxOfReportId = getJdbcTemplate().queryForObject("select MAX("+getDbFunction(Constants.TO_NUMBER,getDbFunction(Constants.SUBSTR, null)+"(REPORT_ID,6,"+getDbFunction(Constants.LENGTH, null)+"(REPORT_ID))")+") from "+tableName+" ", Long.class);
		
		maxOfReportId = (maxOfReportId==null)?0000000:maxOfReportId;
		return ("ETLS_" + String.format("%07d", maxOfReportId+1));
	}

	public List<EtlExtractionSummaryFieldsVb> getSampleDataRuleDetails(EtlFeedMainVb etlFeedMainVb) {
		
		String tableName = "ETL_EXTRACTION_SUMMARY_FIELDS";
		/*if (etlFeedMainVb.getFeedStatus() == Constants.WORK_IN_PROGRESS
				|| etlFeedMainVb.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			tableName = "ETL_EXTRACTION_SUMMARY_FIELDS_UPL";
		} else {
			if (etlFeedMainVb.getRecordIndicator() == Constants.STATUS_ZERO) {
				tableName = "ETL_EXTRACTION_SUMMARY_FIELDS";
			} else {
				tableName = "ETL_EXTRACTION_SUMMARY_FIELDS_PEND";
			}
		}*/
		
		if (etlFeedMainVb.getFeedStatus() == Constants.WORK_IN_PROGRESS ) {
			tableName = "ETL_EXTRACTION_SUMMARY_FIELDS_UPL";
		}  else if( etlFeedMainVb.getFeedStatus() == Constants.PUBLISHED_AND_WORK_IN_PROGRESS) {
			tableName = "ETL_EXTRACTION_SUMMARY_FIELDS_PEND";
		}
	
		String sql = "Select TAppr.COUNTRY"
				+ ",TAppr.LE_BOOK"
				+ ",TAppr.FEED_ID"
				+ ",TAppr.TABLE_ID"
				+ ",TAppr.COL_ID"
				+ ",TAppr.USER_ID"
				+ ",TAppr.COL_NAME"
	            + ",(SELECT COLUMN_DATATYPE FROM ETL_FEED_COLUMNS WHERE COLUMN_NAME=TAppr.COL_NAME and COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) COLUMN_DATA_TYPE"  
				+ ",(SELECT SOURCE_CONNECTOR_ID FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) SOURCE_CONNECTOR_ID " 
				+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_NAME " 
				+ ",(SELECT TABLE_ALIAS_NAME FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_ALIAS_NAME " 
				+ ",(SELECT TABLE_TYPE FROM ETL_FEED_TABLES WHERE COUNTRY=TAppr.COUNTRY AND LE_BOOK=TAppr.LE_BOOK AND FEED_ID=TAppr.FEED_ID AND TABLE_ID= TAppr.TABLE_ID) TABLE_TYPE " 
				+ ",TAppr.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) FORMAT_TYPE_DESC " 
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TAppr.DATE_FORMAT) JAVA_DATE_FORMAT " 
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_FORMATTING_SYNTAX " 
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_CONVERSION_SYNTAX" 
				+ ",TAppr.ALIAS"
				+ ",TAppr.CONDITION_OPERATOR"
				+ ",TAppr.VALUE_1"
				+ ",TAppr.VALUE_2"
				+ ",TAppr.DISPLAY_FLAG"
				+ ",TAppr.SORT_TYPE"
				+ ",TAppr.SORT_ORDER"
				+ ",TAppr.AGG_FUNCTION"
				+ ",TAppr.GROUP_BY"
				+ ",TAppr.COL_DISPLAY_TYPE"
				+ ",TAppr.ETL_FIELDS_STATUS_NT"
				+ ",TAppr.ETL_FIELDS_STATUS"
				+ ", "+getDbFunction(Constants.TO_CHAR, "TAppr.EXPERSSION_TEXT")+" EXPERSSION_TEXT"
				+ ",TAppr.JOIN_CONDITION"
				+ ",TAppr.DYNAMIC_START_FLAG"
				+ ",TAppr.DYNAMIC_END_FLAG"
				+ ",TAppr.DYNAMIC_START_DATE"
				+ ",TAppr.DYNAMIC_END_DATE"
				+ ",TAppr.DYNAMIC_START_OPERATOR"
				+ ",TAppr.DYNAMIC_END_OPERATOR"
				+ ",TAppr.DYNAMIC_DATE_FORMAT"
				+ ",TAppr.SUMMARY_CRITERIA"
				+ ",TAppr.SUMMARY_VALUE_1"
				+ ",TAppr.SUMMARY_VALUE_2"
				+ ",TAppr.SUMMARY_DYNAMIC_START_FLAG"
				+ ",TAppr.SUMMARY_DYNAMIC_END_FLAG"
				+ ",TAppr.SUMMARY_DYNAMIC_START_DATE"
				+ ",TAppr.SUMMARY_DYNAMIC_END_DATE"
				+ ",TAppr.SUMMARY_DYNAMIC_START_OPERATOR"
				+ ",TAppr.SUMMARY_DYNAMIC_END_OPERATOR"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR"
				+ ",TAppr.MAKER"
				+ ",TAppr.VERIFIER"
				+ ",TAppr.INTERNAL_STATUS"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null)+" "+getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ ", TAppr.SAMPLE_DATA_RULE, TAppr.SAMPLE_DATA_CUSTOM_RULE, TAppr.LINKED_COLUMN_ID "
				+ " from "+tableName+" TAppr where TAppr.COUNTRY = ? AND TAppr.LE_BOOK = ? AND TAppr.FEED_ID = ? ";
		
		Object args[] = {etlFeedMainVb.getCountry(), etlFeedMainVb.getLeBook(), etlFeedMainVb.getFeedId()};
		return getJdbcTemplate().query(sql, args, getMapper());
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String query = "delete FROM ETL_EXTRACTION_SUMMARY_FIELDS_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_EXTRACTION_SUMMARY_FIELDS_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_EXTRACTION_SUMMARY_FIELDS_PRS.SESSION_ID ) ";
		Object[] args = {  vObject.getFeedCategory() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
}
