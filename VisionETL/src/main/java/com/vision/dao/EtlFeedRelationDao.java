package com.vision.dao;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedRelationVb;
import com.vision.vb.RelationMapVb;

@Component
public class EtlFeedRelationDao extends AbstractDao<EtlFeedRelationVb> {

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String JoinTypeNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1100, "TAppr.JOIN_TYPE", "JOIN_TYPE_DESC");
	String JoinTypeNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1100, "TPend.JOIN_TYPE", "JOIN_TYPE_DESC");

	String FeedRelationStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_RELATION_STATUS", "FEED_RELATION_STATUS_DESC");
	String FeedRelationStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_RELATION_STATUS", "FEED_RELATION_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedRelationVb vObject = new EtlFeedRelationVb();
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
				if(rs.getString("FROM_TABLE_ID")!= null){ 
					vObject.setFromTableId(rs.getString("FROM_TABLE_ID"));
				}else{
					vObject.setFromTableId("");
				}
				if(rs.getString("TO_TABLE_ID")!= null){ 
					vObject.setToTableId(rs.getString("TO_TABLE_ID"));
				}else{
					vObject.setToTableId("");
				}
				vObject.setFromTableName(rs.getString("FROM_TABLE_NAME"));
				vObject.setToTableName(rs.getString("TO_TABLE_NAME"));
				vObject.setJoinTypeNt(rs.getInt("JOIN_TYPE_NT"));
				vObject.setJoinType(rs.getInt("JOIN_TYPE"));
				if(rs.getString("FILTER_CONTEXT")!= null){ 
					vObject.setFilterContext(rs.getString("FILTER_CONTEXT"));
				}else{
					vObject.setFilterContext("");
				}
				if(rs.getString("FILTER_CONTEXT")!= null){ 
					vObject.setFilterCondition(rs.getString("FILTER_CONTEXT"));
				}else{
					vObject.setFilterCondition("");
				}
				if(rs.getString("RELATION_CONTEXT")!= null){ 
					vObject.setRelationContext(rs.getString("RELATION_CONTEXT"));
				}else{
					vObject.setRelationContext("");
				}
				vObject.setRelationScript(vObject.getRelationContext());
				if(ValidationUtil.isValid(vObject.getRelationScript())) {
					List<RelationMapVb> mapList = new ArrayList<RelationMapVb>();
					
					Matcher matchObj = Pattern.compile("<customjoin>(.*?)<\\/customjoin>", Pattern.DOTALL).matcher(vObject.getRelationScript());
					if(matchObj.find()) {
						vObject.setCustomJoinString(matchObj.group(1));
					}							
					
					if(!ValidationUtil.isValid(vObject.getCustomJoinString())) {
						matchObj = Pattern.compile("<column>(.*?)<\\/column>", Pattern.DOTALL).matcher(vObject.getRelationScript());
						while(matchObj.find()) {
							mapList.add(new RelationMapVb(CommonUtils.getValueForXmlTag(matchObj.group(1), "fcolumn"), CommonUtils.getValueForXmlTag(matchObj.group(1), "tcolumn")));
						}
					}
					vObject.setRelationScriptParsed(ValidationUtil.isValidList(mapList)?mapList:null);
				}
				vObject.setFeedRelationStatusNt(rs.getInt("FEED_RELATION_STATUS_NT"));
				vObject.setFeedRelationStatus(rs.getInt("FEED_RELATION_STATUS"));
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
				return vObject;
			}
		};
		return mapper;
	}

/*******Mapper End**********/
	public List<EtlFeedRelationVb> getQueryPopupResults(EtlFeedRelationVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY = TAppr.COUNTRY AND LE_BOOK = TAppr.LE_BOOK AND  FEED_ID = TAppr.FEED_ID AND TABLE_ID = TAppr.FROM_TABLE_ID  and COUNTRY='"+dObj.getCountry()+"' and LE_Book='"+dObj.getLeBook()+"' and FEED_ID='"+dObj.getFeedId()+"') FROM_TABLE_NAME " 
			+ ",TAppr.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY = TAppr.COUNTRY AND LE_BOOK = TAppr.LE_BOOK AND  FEED_ID = TAppr.FEED_ID AND TABLE_ID = TAppr.TO_TABLE_ID  and COUNTRY='"+dObj.getCountry()+"' and LE_Book='"+dObj.getLeBook()+"' and FEED_ID='"+dObj.getFeedId()+"') TO_TABLE_NAME " 
			+ ",TAppr.JOIN_TYPE_NT"
			+ ",TAppr.JOIN_TYPE"
			+ ",TAppr.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TAppr.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TAppr.FEED_RELATION_STATUS_NT"
			+ ",TAppr.FEED_RELATION_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" from ETL_FEED_RELATION TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FEED_RELATION_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.FROM_TABLE_ID = TPend.FROM_TABLE_ID AND TAppr.TO_TABLE_ID = TPend.TO_TABLE_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY = TPend.COUNTRY AND LE_BOOK = TPend.LE_BOOK AND  FEED_ID = TPend.FEED_ID AND " + 
			"TABLE_ID = TPend.FROM_TABLE_ID  and COUNTRY='"+dObj.getCountry()+"' and LE_Book='"+dObj.getLeBook()+"' and FEED_ID='"+dObj.getFeedId()+"') FROM_TABLE_NAME " 
			+ ",TPend.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY = TPend.COUNTRY AND LE_BOOK = TPend.LE_BOOK AND  FEED_ID = TPend.FEED_ID AND " + 
			"TABLE_ID = TPend.TO_TABLE_ID  and COUNTRY='"+dObj.getCountry()+"' and LE_Book='"+dObj.getLeBook()+"' and FEED_ID='"+dObj.getFeedId()+"') TO_TABLE_NAME " 
			+ ",TPend.JOIN_TYPE_NT"
			+ ",TPend.JOIN_TYPE"
			+ ",TPend.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TPend.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TPend.FEED_RELATION_STATUS_NT"
			+ ",TPend.FEED_RELATION_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" from ETL_FEED_RELATION_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY = TUpl.COUNTRY AND LE_BOOK = TUpl.LE_BOOK AND  FEED_ID = TUpl.FEED_ID AND TABLE_ID = TUpl.FROM_TABLE_ID and COUNTRY='"+dObj.getCountry()+"' and LE_Book='"+dObj.getLeBook()+"' and FEED_ID='"+dObj.getFeedId()+"') FROM_TABLE_NAME " 
			+ ",TUpl.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY = TUpl.COUNTRY AND LE_BOOK = TUpl.LE_BOOK AND  FEED_ID = TUpl.FEED_ID AND TABLE_ID = TUpl.TO_TABLE_ID  and COUNTRY='"+dObj.getCountry()+"' and LE_Book='"+dObj.getLeBook()+"' and FEED_ID='"+dObj.getFeedId()+"') TO_TABLE_NAME " 
			+ ",TUpl.JOIN_TYPE_NT"
			+ ",TUpl.JOIN_TYPE"
			+ ",TUpl.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TUpl.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TUpl.FEED_RELATION_STATUS_NT"
			+ ",TUpl.FEED_RELATION_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" from ETL_FEED_RELATION_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_RELATION_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY "
				+ "AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.FROM_TABLE_ID = TUpl.FROM_TABLE_ID AND TAppr.TO_TABLE_ID = TUpl.TO_TABLE_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_RELATION_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY "
				+ "AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID AND TUpl.FROM_TABLE_ID = TPend.FROM_TABLE_ID AND TUpl.TO_TABLE_ID = TPend.TO_TABLE_ID) ");
		try
			{
				if (ValidationUtil.isValid(dObj.getCountry())){
						params.addElement(dObj.getCountry());
						CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getLeBook())){
						params.addElement(dObj.getLeBook());
						CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedId())){
						params.addElement(dObj.getFeedId());
						CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
				}
/*				if (ValidationUtil.isValid(dObj.getFromTableId())){
						params.addElement(dObj.getFromTableId());
						CommonUtils.addToQuery("UPPER(TAppr.FROM_TABLE_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FROM_TABLE_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FROM_TABLE_ID) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getToTableId())){
						params.addElement(dObj.getToTableId());
						CommonUtils.addToQuery("UPPER(TAppr.TO_TABLE_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.TO_TABLE_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.TO_TABLE_ID) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getJoinType())){
						params.addElement(dObj.getJoinType());
						CommonUtils.addToQuery("TAppr.JOIN_TYPE = ?", strBufApprove);
						CommonUtils.addToQuery("TPend.JOIN_TYPE = ?", strBufPending);
						CommonUtils.addToQuery("TUpl.JOIN_TYPE = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFilterContext())){
						params.addElement(dObj.getFilterContext());
						CommonUtils.addToQuery("UPPER(TAppr.FILTER_CONTEXT) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FILTER_CONTEXT) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FILTER_CONTEXT) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getRelationContext())){
						params.addElement(dObj.getRelationContext());
						CommonUtils.addToQuery("UPPER(TAppr.RELATION_CONTEXT) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.RELATION_CONTEXT) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.RELATION_CONTEXT) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedRelationStatus())){
						params.addElement(dObj.getFeedRelationStatus());
						CommonUtils.addToQuery("TAppr.FEED_RELATION_STATUS = ?", strBufApprove);
						CommonUtils.addToQuery("TPend.FEED_RELATION_STATUS = ?", strBufPending);
						CommonUtils.addToQuery("TUpl.FEED_RELATION_STATUS = ?", strBufUpl);
				}
				if (dObj.getRecordIndicator() != -1){
					if (dObj.getRecordIndicator() > 3){
						params.addElement(new Integer(0));
						CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
						CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
						CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufUpl);
					}else{
						 params.addElement(new Integer(dObj.getRecordIndicator()));
						 CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
						 CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
						 CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufUpl);
					}
				}*/
			String orderBy=" Order By COUNTRY, LE_BOOK, FEED_ID, FROM_TABLE_ID, TO_TABLE_ID ";
			CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);
			CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);
			StringBuffer lPageQuery = new StringBuffer();
			if(dObj.isVerificationRequired())
				lPageQuery.append(strBufPending.toString() + " Union " + strBufUpl.toString());
			else
				lPageQuery.append(strBufApprove.toString() + " Union " + strBufUpl.toString());
			return getQueryPopupResultsPgn(dObj,lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,getMapper());

		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;

		}
	}

	public List<EtlFeedRelationVb> getQueryResults(EtlFeedRelationVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedRelationVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY = TAppr.COUNTRY AND LE_BOOK = TAppr.LE_BOOK AND  FEED_ID = TAppr.FEED_ID AND  TABLE_ID = TAppr.FROM_TABLE_ID) FROM_TABLE_NAME " 
			+ ",TAppr.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY = TAppr.COUNTRY AND LE_BOOK = TAppr.LE_BOOK AND  FEED_ID = TAppr.FEED_ID AND  TABLE_ID = TAppr.TO_TABLE_ID) TO_TABLE_NAME " 
			+ ",TAppr.TO_TABLE_ID"
			+ ",TAppr.JOIN_TYPE_NT"
			+ ",TAppr.JOIN_TYPE"
			+ ",TAppr.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TAppr.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TAppr.FEED_RELATION_STATUS_NT"
			+ ",TAppr.FEED_RELATION_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_RELATION TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY = TPend.COUNTRY AND LE_BOOK = TPend.LE_BOOK AND  FEED_ID = TPend.FEED_ID AND TABLE_ID = TPend.FROM_TABLE_ID) FROM_TABLE_NAME " 
			+ ",TPend.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY = TPend.COUNTRY AND LE_BOOK = TPend.LE_BOOK AND  FEED_ID = TPend.FEED_ID AND TABLE_ID = TPend.TO_TABLE_ID) TO_TABLE_NAME " 
			+ ",TPend.TO_TABLE_ID"
			+ ",TPend.JOIN_TYPE_NT"
			+ ",TPend.JOIN_TYPE"
			+ ",TPend.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TPend.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TPend.FEED_RELATION_STATUS_NT"
			+ ",TPend.FEED_RELATION_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_RELATION_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY = TUpl.COUNTRY AND LE_BOOK = TUpl.LE_BOOK AND  FEED_ID = TUpl.FEED_ID AND TABLE_ID = TUpl.FROM_TABLE_ID) FROM_TABLE_NAME " 
			+ ",TUpl.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY = TUpl.COUNTRY AND LE_BOOK = TUpl.LE_BOOK AND  FEED_ID = TUpl.FEED_ID AND TABLE_ID = TUpl.TO_TABLE_ID) TO_TABLE_NAME " 
			+ ",TUpl.TO_TABLE_ID"
			+ ",TUpl.JOIN_TYPE_NT"
			+ ",TUpl.JOIN_TYPE"
			+ ",TUpl.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TUpl.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TUpl.FEED_RELATION_STATUS_NT"
			+ ",TUpl.FEED_RELATION_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_RELATION_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

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
	public List<EtlFeedRelationVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedRelationVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY = TAppr.COUNTRY AND LE_BOOK = TAppr.LE_BOOK AND  FEED_ID = TAppr.FEED_ID AND TABLE_ID = TAppr.FROM_TABLE_ID) FROM_TABLE_NAME " 
			+ ",TAppr.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES WHERE COUNTRY = TAppr.COUNTRY AND LE_BOOK = TAppr.LE_BOOK AND  FEED_ID = TAppr.FEED_ID AND TABLE_ID = TAppr.TO_TABLE_ID) TO_TABLE_NAME " 
			+ ",TAppr.TO_TABLE_ID"
			+ ",TAppr.JOIN_TYPE_NT"
			+ ",TAppr.JOIN_TYPE"
			+ ",TAppr.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TAppr.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TAppr.FEED_RELATION_STATUS_NT"
			+ ",TAppr.FEED_RELATION_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_RELATION TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY = TPend.COUNTRY AND LE_BOOK = TPend.LE_BOOK AND  FEED_ID = TPend.FEED_ID AND TABLE_ID = TPend.FROM_TABLE_ID) FROM_TABLE_NAME " 
			+ ",TPend.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_PEND WHERE COUNTRY = TPend.COUNTRY AND LE_BOOK = TPend.LE_BOOK AND  FEED_ID = TPend.FEED_ID AND TABLE_ID = TPend.TO_TABLE_ID) TO_TABLE_NAME " 
			+ ",TPend.TO_TABLE_ID"
			+ ",TPend.JOIN_TYPE_NT"
			+ ",TPend.JOIN_TYPE"
			+ ",TPend.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TPend.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TPend.FEED_RELATION_STATUS_NT"
			+ ",TPend.FEED_RELATION_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_RELATION_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.FROM_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY = TUpl.COUNTRY AND LE_BOOK = TUpl.LE_BOOK AND  FEED_ID = TUpl.FEED_ID AND TABLE_ID = TUpl.FROM_TABLE_ID) FROM_TABLE_NAME " 
			+ ",TUpl.TO_TABLE_ID"
			+ ",(SELECT TABLE_NAME FROM ETL_FEED_TABLES_UPL WHERE COUNTRY = TUpl.COUNTRY AND LE_BOOK = TUpl.LE_BOOK AND  FEED_ID = TUpl.FEED_ID AND TABLE_ID = TUpl.TO_TABLE_ID) TO_TABLE_NAME " 
			+ ",TUpl.TO_TABLE_ID"
			+ ",TUpl.JOIN_TYPE_NT"
			+ ",TUpl.JOIN_TYPE"
			+ ",TUpl.FILTER_CONTEXT FILTER_CONTEXT"
			+ ",TUpl.RELATION_CONTEXT RELATION_CONTEXT"
			+ ",TUpl.FEED_RELATION_STATUS_NT"
			+ ",TUpl.FEED_RELATION_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_RELATION_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");

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
	protected List<EtlFeedRelationVb> selectApprovedRecord(EtlFeedRelationVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<EtlFeedRelationVb> doSelectPendingRecord(EtlFeedRelationVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}


	@Override
	protected List<EtlFeedRelationVb> doSelectUplRecord(EtlFeedRelationVb vObject){
		return getQueryResults(vObject, 9999);
	}


	@Override
	protected int getStatus(EtlFeedRelationVb records){return records.getFeedRelationStatus();}


	@Override
	protected void setStatus(EtlFeedRelationVb vObject,int status){vObject.setFeedRelationStatus(status);}


	@Override
	protected int doInsertionAppr(EtlFeedRelationVb vObject){
		String query = "Insert Into ETL_FEED_RELATION (COUNTRY, LE_BOOK, FEED_ID, FROM_TABLE_ID, "
				+ "TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE,  "
				+ "FEED_RELATION_STATUS_NT, FEED_RELATION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ "FILTER_CONTEXT, RELATION_CONTEXT)" + "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ? )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getFromTableId(),
				vObject.getToTableId(), vObject.getJoinTypeNt(), vObject.getJoinType(),
				vObject.getFeedRelationStatusNt(), vObject.getFeedRelationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
//		 return getJdbcTemplate().update(query,args);
	
		int result=0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for(int i=1;i<=argumentLength;i++){
						ps.setObject(i,args[i-1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getFilterContext())?vObject.getFilterContext():"";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					clobData = ValidationUtil.isValid(vObject.getRelationContext())?vObject.getRelationContext():"";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					
					return ps;
				}
			});
			
		}catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}


	@Override
	protected int doInsertionPend(EtlFeedRelationVb vObject){
		String query = "Insert Into ETL_FEED_RELATION_PEND (COUNTRY, LE_BOOK, FEED_ID, FROM_TABLE_ID, "
				+ "TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE,  "
				+ "FEED_RELATION_STATUS_NT, FEED_RELATION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ "FILTER_CONTEXT, RELATION_CONTEXT)" + "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ? )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getFromTableId(),
				vObject.getToTableId(), vObject.getJoinTypeNt(), vObject.getJoinType(),
				vObject.getFeedRelationStatusNt(), vObject.getFeedRelationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
//		 return getJdbcTemplate().update(query,args);
	
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
					String clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					clobData = ValidationUtil.isValid(vObject.getRelationContext()) ? vObject.getRelationContext() : "";
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
	protected int doInsertionUpl(EtlFeedRelationVb vObject){
		String query = "Insert Into ETL_FEED_RELATION_UPL (COUNTRY, LE_BOOK, FEED_ID, FROM_TABLE_ID, "
				+ "TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE,  "
				+ "FEED_RELATION_STATUS_NT, FEED_RELATION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ "FILTER_CONTEXT, RELATION_CONTEXT)" + "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ? )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getFromTableId(),
				vObject.getToTableId(), vObject.getJoinTypeNt(), vObject.getJoinType(),
				vObject.getFeedRelationStatusNt(), vObject.getFeedRelationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
//		 return getJdbcTemplate().update(query,args);
	
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
					String clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					clobData = ValidationUtil.isValid(vObject.getRelationContext()) ? vObject.getRelationContext() : "";
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
	protected int doInsertionPendWithDc(EtlFeedRelationVb vObject){
		String query = "Insert Into ETL_FEED_RELATION_PEND (COUNTRY, LE_BOOK, FEED_ID, FROM_TABLE_ID, "
				+ "TO_TABLE_ID, JOIN_TYPE_NT, JOIN_TYPE,  "
				+ "FEED_RELATION_STATUS_NT, FEED_RELATION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,"
				+ "FILTER_CONTEXT, RELATION_CONTEXT)" + "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " , ?, ? )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getFromTableId(),
				vObject.getToTableId(), vObject.getJoinTypeNt(), vObject.getJoinType(),
				vObject.getFeedRelationStatusNt(), vObject.getFeedRelationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
//			 return getJdbcTemplate().update(query,args);
		
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
					String clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext()
							: "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					clobData = ValidationUtil.isValid(vObject.getRelationContext()) ? vObject.getRelationContext() : "";
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
	protected int doUpdateAppr(EtlFeedRelationVb vObject){
		String query = "Update ETL_FEED_RELATION Set FILTER_CONTEXT = ?, RELATION_CONTEXT = ?, JOIN_TYPE_NT = ?, JOIN_TYPE = ?, "
				+ " FEED_RELATION_STATUS_NT = ?, FEED_RELATION_STATUS = ?, RECORD_INDICATOR_NT = ?, "
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND FROM_TABLE_ID = ?  AND TO_TABLE_ID = ? ";
		Object[] args = { vObject.getJoinTypeNt(), vObject.getJoinType(), vObject.getFeedRelationStatusNt(),
				vObject.getFeedRelationStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getFromTableId(), vObject.getToTableId() };
		int result = 0;
		try{
			
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getFilterContext())?vObject.getFilterContext():"";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					clobData = ValidationUtil.isValid(vObject.getRelationContext())?vObject.getRelationContext():"";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					for(int i=1;i<=args.length;i++){
						ps.setObject(++psIndex,args[i-1]);	
					}
					
					return ps;
				}
			});
		}catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}


	@Override
	protected int doUpdatePend(EtlFeedRelationVb vObject){
		String query = "Update ETL_FEED_RELATION_PEND Set FILTER_CONTEXT = ?, RELATION_CONTEXT = ?, JOIN_TYPE_NT = ?, JOIN_TYPE = ?, "
				+ " FEED_RELATION_STATUS_NT = ?, FEED_RELATION_STATUS = ?, RECORD_INDICATOR_NT = ?, "
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND FROM_TABLE_ID = ?  AND TO_TABLE_ID = ? ";
		Object[] args = { vObject.getJoinTypeNt(), vObject.getJoinType(), vObject.getFeedRelationStatusNt(),
				vObject.getFeedRelationStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getFromTableId(), vObject.getToTableId() };
		int result = 0;
			try{
				return getJdbcTemplate().update(new PreparedStatementCreator() {
					@Override
					public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
						PreparedStatement ps = connection.prepareStatement(query);
						int psIndex = 0;
						String clobData = ValidationUtil.isValid(vObject.getFilterContext())?vObject.getFilterContext():"";
						ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
						clobData = ValidationUtil.isValid(vObject.getRelationContext())?vObject.getRelationContext():"";
						ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
						
						for(int i=1;i<=args.length;i++){
							ps.setObject(++psIndex,args[i-1]);	
						}
						
						return ps;
					}
				});
			}catch (Exception e) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e.printStackTrace();
				}
				strErrorDesc = e.getMessage();
			}
			return result;
		}


	@Override
	protected int doDeleteAppr(EtlFeedRelationVb vObject){
		String query = "Delete From ETL_FEED_RELATION Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedRelationVb vObject){
		String query = "Delete From ETL_FEED_RELATION_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deleteUplRecord(EtlFeedRelationVb vObject){
		String query = "Delete From ETL_FEED_RELATION_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	
	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_RELATION";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_RELATION_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_RELATION_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_RELATION_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_FEED_RELATION";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_RELATION_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_RELATION_PEND";
		}
		String query = "update "+table+" set FEED_RELATION_STATUS = ? ,RECORD_INDICATOR =? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(),vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String getAuditString(EtlFeedRelationVb vObject){
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");
			if(ValidationUtil.isValid(vObject.getCountry()))
				strAudit.append("COUNTRY"+auditDelimiterColVal+vObject.getCountry().trim());
			else
				strAudit.append("COUNTRY"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getLeBook()))
				strAudit.append("LE_BOOK"+auditDelimiterColVal+vObject.getLeBook().trim());
			else
				strAudit.append("LE_BOOK"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getFeedId()))
				strAudit.append("FEED_ID"+auditDelimiterColVal+vObject.getFeedId().trim());
			else
				strAudit.append("FEED_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getFromTableId()))
				strAudit.append("FROM_TABLE_ID"+auditDelimiterColVal+vObject.getFromTableId().trim());
			else
				strAudit.append("FROM_TABLE_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getToTableId()))
				strAudit.append("TO_TABLE_ID"+auditDelimiterColVal+vObject.getToTableId().trim());
			else
				strAudit.append("TO_TABLE_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("JOIN_TYPE_NT"+auditDelimiterColVal+vObject.getJoinTypeNt());
			strAudit.append(auditDelimiter);

				strAudit.append("JOIN_TYPE"+auditDelimiterColVal+vObject.getJoinType());
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getFilterContext()))
				strAudit.append("FILTER_CONTEXT"+auditDelimiterColVal+vObject.getFilterContext().trim());
			else
				strAudit.append("FILTER_CONTEXT"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getRelationContext()))
				strAudit.append("RELATION_CONTEXT"+auditDelimiterColVal+vObject.getRelationContext().trim());
			else
				strAudit.append("RELATION_CONTEXT"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_RELATION_STATUS_NT"+auditDelimiterColVal+vObject.getFeedRelationStatusNt());
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_RELATION_STATUS"+auditDelimiterColVal+vObject.getFeedRelationStatus());
			strAudit.append(auditDelimiter);

				strAudit.append("RECORD_INDICATOR_NT"+auditDelimiterColVal+vObject.getRecordIndicatorNt());
			strAudit.append(auditDelimiter);

				strAudit.append("RECORD_INDICATOR"+auditDelimiterColVal+vObject.getRecordIndicator());
			strAudit.append(auditDelimiter);

				strAudit.append("MAKER"+auditDelimiterColVal+vObject.getMaker());
			strAudit.append(auditDelimiter);

				strAudit.append("VERIFIER"+auditDelimiterColVal+vObject.getVerifier());
			strAudit.append(auditDelimiter);

				strAudit.append("INTERNAL_STATUS"+auditDelimiterColVal+vObject.getInternalStatus());
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getDateLastModified()))
				strAudit.append("DATE_LAST_MODIFIED"+auditDelimiterColVal+vObject.getDateLastModified().trim());
			else
				strAudit.append("DATE_LAST_MODIFIED"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getDateCreation()))
				strAudit.append("DATE_CREATION"+auditDelimiterColVal+vObject.getDateCreation().trim());
			else
				strAudit.append("DATE_CREATION"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

		return strAudit.toString();
		}

	@Override
	public void setServiceDefaults(){
		serviceName = "EtlFeedRelation";
		serviceDesc = "ETL Feed Relation";
		tableName = "ETL_FEED_RELATION";
		childTableName = "ETL_FEED_RELATION";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_FEED_RELATION_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ " FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_RELATION_PRS.FEED_ID AND "
				+ " ETL_FEED_MAIN_PRS.SESSION_ID = ETL_FEED_RELATION_PRS.SESSION_ID"
				+ ") ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

}
