package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedSourceVb;

@Component
public class EtlFeedSourceDao extends AbstractDao<EtlFeedSourceVb> {

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";
	
	String connectorApprDesc = "(SELECT CONNECTION_NAME FROM ETL_CONNECTOR WHERE LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY AND  CONNECTOR_ID = TAppr.SOURCE_CONNECTOR_ID) CONNECTOR_ID_DESC";
	String connectorPendDesc = "(SELECT CONNECTION_NAME FROM ETL_CONNECTOR WHERE LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY AND  CONNECTOR_ID = TPend.SOURCE_CONNECTOR_ID) CONNECTOR_ID_DESC";

	String FeedSourceStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_SOURCE_STATUS", "FEED_SOURCE_STATUS_DESC");
	String FeedSourceStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_SOURCE_STATUS", "FEED_SOURCE_STATUS_DESC");
	
	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedSourceVb vObject = new EtlFeedSourceVb();
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
				if(rs.getString("SOURCE_CONNECTOR_ID")!= null){ 
					vObject.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
				}else{
					vObject.setSourceConnectorId("");
				}
				vObject.setFeedSourceStatusNt(rs.getInt("FEED_SOURCE_STATUS_NT"));
				vObject.setFeedSourceStatus(rs.getInt("FEED_SOURCE_STATUS"));
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
	public List<EtlFeedSourceVb> getQueryPopupResults(EtlFeedSourceVb dObj){
		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
				+ ",TAppr.LE_BOOK "
				+ ",TAppr.FEED_ID"
				+ ",TAppr.SOURCE_CONNECTOR_ID "
				+ ",TAppr.FEED_SOURCE_STATUS_NT"
				+ ",TAppr.FEED_SOURCE_STATUS"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR"
				+ ",TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FEED_SOURCE_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = "
				+ "TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
				+ ",TPend.LE_BOOK"
				+ ",TPend.FEED_ID"
				+ ",TPend.SOURCE_CONNECTOR_ID"
				+ ",TPend.FEED_SOURCE_STATUS_NT"
				+ ",TPend.FEED_SOURCE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR"
				+ ",TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE_PEND TPend ");

		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
				+ ",TUpl.LE_BOOK"
				+ ",TUpl.FEED_ID"
				+ ",TUpl.SOURCE_CONNECTOR_ID"
				+ ",TUpl.FEED_SOURCE_STATUS_NT"
				+ ",TUpl.FEED_SOURCE_STATUS"
				+ ",TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR"
				+ ",TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_SOURCE_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY "
				+ "AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_SOURCE_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY "
				+ "AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID) ");
		try
			{
			if(ValidationUtil.isValid(dObj.getCountry())){
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
			String orderBy=" Order By COUNTRY , LE_BOOK , FEED_ID  ";
//			return getQueryPopupResults(dObj,strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
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



	public List<EtlFeedSourceVb> getQueryResults(EtlFeedSourceVb dObj, int intStatus){

		List<EtlFeedSourceVb> collTemp = null;

		final int intKeyFieldsCount = 3;
		StringBuffer strQueryAppr = new StringBuffer("Select TAppr.COUNTRY"
				+ ",TAppr.LE_BOOK "
				+ ",TAppr.FEED_ID"
				+ ",TAppr.SOURCE_CONNECTOR_ID "
				+ ",TAppr.FEED_SOURCE_STATUS_NT"
				+ ",TAppr.FEED_SOURCE_STATUS"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR"
				+ ",TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE TAppr "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		StringBuffer strQueryPend = new StringBuffer("Select TPend.COUNTRY"
				+ ",TPend.LE_BOOK"
				+ ",TPend.FEED_ID"
				+ ",TPend.SOURCE_CONNECTOR_ID"
				+ ",TPend.FEED_SOURCE_STATUS_NT"
				+ ",TPend.FEED_SOURCE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR"
				+ ",TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE_PEND TPend "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
				+ ",TUpl.LE_BOOK"
				+ ",TUpl.FEED_ID"
				+ ",TUpl.SOURCE_CONNECTOR_ID"
				+ ",TUpl.FEED_SOURCE_STATUS_NT"
				+ ",TUpl.FEED_SOURCE_STATUS"
				+ ",TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR"
				+ ",TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE_UPL TUpl "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		
		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strBufUpl.toString(), objParams, getMapper());
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
	
	public List<EtlFeedSourceVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		List<EtlFeedSourceVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		StringBuffer strQueryAppr = new StringBuffer("Select TAppr.COUNTRY"
				+ ",TAppr.LE_BOOK "
				+ ",TAppr.FEED_ID"
				+ ",TAppr.SOURCE_CONNECTOR_ID "
				+ ",TAppr.FEED_SOURCE_STATUS_NT"
				+ ",TAppr.FEED_SOURCE_STATUS"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR"
				+ ",TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE TAppr "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		StringBuffer strQueryPend = new StringBuffer("Select TPend.COUNTRY"
				+ ",TPend.LE_BOOK"
				+ ",TPend.FEED_ID"
				+ ",TPend.SOURCE_CONNECTOR_ID"
				+ ",TPend.FEED_SOURCE_STATUS_NT"
				+ ",TPend.FEED_SOURCE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR"
				+ ",TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE_PEND TPend "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
				+ ",TUpl.LE_BOOK"
				+ ",TUpl.FEED_ID"
				+ ",TUpl.SOURCE_CONNECTOR_ID"
				+ ",TUpl.FEED_SOURCE_STATUS_NT"
				+ ",TUpl.FEED_SOURCE_STATUS"
				+ ",TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR"
				+ ",TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, "
				+ ""+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION from ETL_FEED_SOURCE_UPL TUpl "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		
		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strBufUpl.toString(), objParams, getMapper());
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
	protected List<EtlFeedSourceVb> selectApprovedRecord(EtlFeedSourceVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<EtlFeedSourceVb> doSelectPendingRecord(EtlFeedSourceVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlFeedSourceVb> doSelectUplRecord(EtlFeedSourceVb vObject){
		return getQueryResults(vObject, 9999);
	}



	@Override
	protected int getStatus(EtlFeedSourceVb records){return records.getFeedSourceStatus();}


	@Override
	protected void setStatus(EtlFeedSourceVb vObject,int status){vObject.setFeedSourceStatus(status);}


	@Override
	protected int doInsertionAppr(EtlFeedSourceVb vObject){
		String query =	"Insert Into ETL_FEED_SOURCE (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+ 
		 "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")"; 
		Object[] args = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSourceConnectorId(), vObject.getFeedSourceStatusNt(), 
				vObject.getFeedSourceStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		 return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int doInsertionPend(EtlFeedSourceVb vObject){
		String query =	"Insert Into ETL_FEED_SOURCE_PEND (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+ 
		 "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")"; 
		Object[] args = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSourceConnectorId(), vObject.getFeedSourceStatusNt(), 
				vObject.getFeedSourceStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query,args);
	}
	
	@Override
	protected int doInsertionUpl(EtlFeedSourceVb vObject){
		String query =	"Insert Into ETL_FEED_SOURCE_UPL (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+ 
		 "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")"; 
		Object[] args = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSourceConnectorId(), vObject.getFeedSourceStatusNt(), 
				vObject.getFeedSourceStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int doInsertionPendWithDc(EtlFeedSourceVb vObject){
		
		String query = "Insert Into ETL_FEED_SOURCE_PEND (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, FEED_SOURCE_STATUS_NT, FEED_SOURCE_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getSourceConnectorId(), vObject.getFeedSourceStatusNt(), vObject.getFeedSourceStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int doUpdateAppr(EtlFeedSourceVb vObject){
	String query = "Update ETL_FEED_SOURCE Set SOURCE_CONNECTOR_ID = ?, FEED_SOURCE_STATUS_NT = ?, FEED_SOURCE_STATUS = ?, RECORD_INDICATOR_NT = ?, "
			+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+" WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = {vObject.getSourceConnectorId() , vObject.getFeedSourceStatusNt() , vObject.getFeedSourceStatus() , vObject.getRecordIndicatorNt() 
				, vObject.getRecordIndicator() , vObject.getMaker() , vObject.getVerifier() , vObject.getInternalStatus() , vObject.getCountry() , vObject.getLeBook() , vObject.getFeedId() , };

		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int doUpdatePend(EtlFeedSourceVb vObject){
		String query = "Update ETL_FEED_SOURCE_PEND Set SOURCE_CONNECTOR_ID = ?, FEED_SOURCE_STATUS_NT = ?, FEED_SOURCE_STATUS = ?, RECORD_INDICATOR_NT = ?, "
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+" WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
			Object[] args = {vObject.getSourceConnectorId() , vObject.getFeedSourceStatusNt() , vObject.getFeedSourceStatus() , vObject.getRecordIndicatorNt() 
					, vObject.getRecordIndicator() , vObject.getMaker() , vObject.getVerifier() , vObject.getInternalStatus() , vObject.getCountry() , vObject.getLeBook() , vObject.getFeedId() , };

			return getJdbcTemplate().update(query,args);
		}


	@Override
	protected int doDeleteAppr(EtlFeedSourceVb vObject){
		String query = "Delete From ETL_FEED_SOURCE Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deletePendingRecord(EtlFeedSourceVb vObject){
		String query = "Delete From ETL_FEED_SOURCE_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	
	@Override
	protected int deleteUplRecord(EtlFeedSourceVb vObject){
		String query = "Delete From ETL_FEED_SOURCE_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	
	protected int deleteUplRecordByParent(EtlFeedMainVb vObject){
		String query = "Delete From ETL_FEED_SOURCE_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_SOURCE";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SOURCE_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SOURCE_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_SOURCE_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID= ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_FEED_SOURCE";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SOURCE_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_SOURCE_PEND";
		}
		String query = "update "+table+" set FEED_SOURCE_STATUS=?, RECORD_INDICATOR =? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String getAuditString(EtlFeedSourceVb vObject){
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

			if(ValidationUtil.isValid(vObject.getSourceConnectorId()))
				strAudit.append("SOURCE_CONNECTOR_ID"+auditDelimiterColVal+vObject.getSourceConnectorId().trim());
			else
				strAudit.append("SOURCE_CONNECTOR_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_SOURCE_STATUS_NT"+auditDelimiterColVal+vObject.getFeedSourceStatusNt());
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_SOURCE_STATUS"+auditDelimiterColVal+vObject.getFeedSourceStatus());
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
		serviceName = "EtlFeedSource";
		serviceDesc = "ETL Feed Source";
		tableName = "ETL_FEED_SOURCE";
		childTableName = "ETL_FEED_SOURCE";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_FEED_SOURCE_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_SOURCE_PRS.FEED_ID AND "
				+ " ETL_FEED_MAIN_PRS.SESSION_ID = ETL_FEED_SOURCE_PRS.SESSION_ID) ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
	
}