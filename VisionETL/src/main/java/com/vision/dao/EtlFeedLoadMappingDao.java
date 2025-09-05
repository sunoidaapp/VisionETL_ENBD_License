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
import com.vision.vb.EtlFeedLoadMappingVb;
import com.vision.vb.EtlFeedMainVb;

@Component
public class EtlFeedLoadMappingDao extends AbstractDao<EtlFeedLoadMappingVb> {

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String FeedMappingStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_MAPPING_STATUS", "FEED_MAPPING_STATUS_DESC");
	String FeedMappingStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_MAPPING_STATUS", "FEED_MAPPING_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedLoadMappingVb vObject = new EtlFeedLoadMappingVb();
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
				if(rs.getString("TRAN_COLUMN_ID")!= null){ 
					vObject.setTranColumnId(rs.getString("TRAN_COLUMN_ID"));
				}else{
					vObject.setTranColumnId("");
				}
				if(rs.getString("TARGET_COLUMN_ID")!= null){ 
					vObject.setTargetColumnId(rs.getString("TARGET_COLUMN_ID"));
				}else{
					vObject.setTargetColumnId("");
				}
				vObject.setFeedMappingStatusNt(rs.getInt("FEED_MAPPING_STATUS_NT"));
				vObject.setFeedMappingStatus(rs.getInt("FEED_MAPPING_STATUS"));
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
	public List<EtlFeedLoadMappingVb> getQueryPopupResults(EtlFeedLoadMappingVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TRAN_COLUMN_ID"
			+ ",TAppr.TARGET_COLUMN_ID"
			+ ",TAppr.FEED_MAPPING_STATUS_NT"
			+ ",TAppr.FEED_MAPPING_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " from ETL_FEED_LOAD_MAPPING TAppr ");
		
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FEED_LOAD_MAPPING_PEND TPend WHERE TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.TRAN_COLUMN_ID = TPend.TRAN_COLUMN_ID AND TAppr.COUNTRY = TPend.COUNTRY AND TAppr.TARGET_COLUMN_ID = TPend.TARGET_COLUMN_ID )");

		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TRAN_COLUMN_ID"
			+ ",TPend.TARGET_COLUMN_ID"
			+ ",TPend.FEED_MAPPING_STATUS_NT"
			+ ",TPend.FEED_MAPPING_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " from ETL_FEED_LOAD_MAPPING_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TRAN_COLUMN_ID"
			+ ",TUpl.TARGET_COLUMN_ID"
			+ ",TUpl.FEED_MAPPING_STATUS_NT"
			+ ",TUpl.FEED_MAPPING_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " from ETL_FEED_LOAD_MAPPING_UPL TUpl ");
		
		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_LOAD_MAPPING_UPL TUpl WHERE TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.TRAN_COLUMN_ID = TUpl.TRAN_COLUMN_ID AND TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.TARGET_COLUMN_ID = TUpl.TARGET_COLUMN_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_LOAD_MAPPING_UPL TUpl WHERE TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID AND TUpl.TRAN_COLUMN_ID = TPend.TRAN_COLUMN_ID AND TUpl.COUNTRY = TPend.COUNTRY AND TUpl.TARGET_COLUMN_ID = TPend.TARGET_COLUMN_ID) ");
		
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
/*				if (ValidationUtil.isValid(dObj.getTranColumnId())){
						params.addElement(dObj.getTranColumnId());
						CommonUtils.addToQuery("UPPER(TAppr.TRAN_COLUMN_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.TRAN_COLUMN_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.TRAN_COLUMN_ID) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getTargetColumnId())){
						params.addElement(dObj.getTargetColumnId());
						CommonUtils.addToQuery("UPPER(TAppr.TARGET_COLUMN_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.TARGET_COLUMN_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.TARGET_COLUMN_ID) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedMappingStatus())){
						params.addElement(dObj.getFeedMappingStatus());
						CommonUtils.addToQuery("TAppr.FEED_MAPPING_STATUS = ?", strBufApprove);
						CommonUtils.addToQuery("TPend.FEED_MAPPING_STATUS = ?", strBufPending);
						CommonUtils.addToQuery("TUpl.FEED_MAPPING_STATUS = ?", strBufUpl);
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
			String orderBy=" Order By FEED_ID, TRAN_COLUMN_ID, TARGET_COLUMN_ID ";
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
	
	public List<EtlFeedLoadMappingVb> getQueryResults(EtlFeedLoadMappingVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedLoadMappingVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TRAN_COLUMN_ID"
			+ ",TAppr.TARGET_COLUMN_ID"
			+ ",TAppr.FEED_MAPPING_STATUS_NT"
			+ ",TAppr.FEED_MAPPING_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_LOAD_MAPPING TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TRAN_COLUMN_ID"
			+ ",TPend.TARGET_COLUMN_ID"
			+ ",TPend.FEED_MAPPING_STATUS_NT"
			+ ",TPend.FEED_MAPPING_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_LOAD_MAPPING_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TRAN_COLUMN_ID"
			+ ",TUpl.TARGET_COLUMN_ID"
			+ ",TUpl.FEED_MAPPING_STATUS_NT"
			+ ",TUpl.FEED_MAPPING_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_LOAD_MAPPING_UPL TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");

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
	
	public List<EtlFeedLoadMappingVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedLoadMappingVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TRAN_COLUMN_ID"
			+ ",TAppr.TARGET_COLUMN_ID"
			+ ",TAppr.FEED_MAPPING_STATUS_NT"
			+ ",TAppr.FEED_MAPPING_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_LOAD_MAPPING TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TRAN_COLUMN_ID"
			+ ",TPend.TARGET_COLUMN_ID"
			+ ",TPend.FEED_MAPPING_STATUS_NT"
			+ ",TPend.FEED_MAPPING_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_LOAD_MAPPING_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TRAN_COLUMN_ID"
			+ ",TUpl.TARGET_COLUMN_ID"
			+ ",TUpl.FEED_MAPPING_STATUS_NT"
			+ ",TUpl.FEED_MAPPING_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+ " From ETL_FEED_LOAD_MAPPING_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();
		try {
			// if(!dObj.isVerificationRequired()){intStatus =0;}
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
	protected List<EtlFeedLoadMappingVb> selectApprovedRecord(EtlFeedLoadMappingVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedLoadMappingVb> doSelectPendingRecord(EtlFeedLoadMappingVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlFeedLoadMappingVb> doSelectUplRecord(EtlFeedLoadMappingVb vObject){
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlFeedLoadMappingVb records){return records.getFeedMappingStatus();}

	@Override
	protected void setStatus(EtlFeedLoadMappingVb vObject,int status){vObject.setFeedMappingStatus(status);}

	@Override
	protected int doInsertionAppr(EtlFeedLoadMappingVb vObject){
		String query =	"Insert Into ETL_FEED_LOAD_MAPPING (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TARGET_COLUMN_ID, "
				+ "FEED_MAPPING_STATUS_NT, FEED_MAPPING_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+ 
		 "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")"; 
		Object[] args = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(), vObject.getTargetColumnId(), 
				vObject.getFeedMappingStatusNt(), vObject.getFeedMappingStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), 
				vObject.getVerifier(), vObject.getInternalStatus() };

		 return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int doInsertionPend(EtlFeedLoadMappingVb vObject){
		String query =	"Insert Into ETL_FEED_LOAD_MAPPING_PEND (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TARGET_COLUMN_ID, "
				+ "FEED_MAPPING_STATUS_NT, FEED_MAPPING_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+ 
		 "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")"; 
		Object[] args = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(), vObject.getTargetColumnId(), 
				vObject.getFeedMappingStatusNt(), vObject.getFeedMappingStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), 
				vObject.getVerifier(), vObject.getInternalStatus() };
		 return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int doInsertionUpl(EtlFeedLoadMappingVb vObject){
		String query =	"Insert Into ETL_FEED_LOAD_MAPPING_UPL (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TARGET_COLUMN_ID, "
				+ "FEED_MAPPING_STATUS_NT, FEED_MAPPING_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"+ 
		 "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")"; 
		Object[] args = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(), vObject.getTargetColumnId(), 
				vObject.getFeedMappingStatusNt(), vObject.getFeedMappingStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), 
				vObject.getVerifier(), vObject.getInternalStatus() };
		 return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFeedLoadMappingVb vObject) {
		String query = "Insert Into ETL_FEED_LOAD_MAPPING_PEND (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TARGET_COLUMN_ID, "
				+ "FEED_MAPPING_STATUS_NT, FEED_MAPPING_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(),
				vObject.getTargetColumnId(), vObject.getFeedMappingStatusNt(), vObject.getFeedMappingStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFeedLoadMappingVb vObject){
		String query = "Update ETL_FEED_LOAD_MAPPING_PEND Set FEED_MAPPING_STATUS_NT = ?, FEED_MAPPING_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+" "
				+ " WHERE COUNTRY = ? AND LE_BOOK = ? AND FEED_ID = ?  AND TRAN_COLUMN_ID = ?  AND TARGET_COLUMN_ID = ? ";
		Object[] args = {vObject.getFeedMappingStatusNt() , vObject.getFeedMappingStatus() , 
				vObject.getRecordIndicatorNt() , vObject.getRecordIndicator() , vObject.getMaker() , vObject.getVerifier() , vObject.getInternalStatus() ,
				vObject.getCountry() , vObject.getLeBook() , vObject.getFeedId() , vObject.getTranColumnId() , vObject.getTargetColumnId() };

		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int doUpdatePend(EtlFeedLoadMappingVb vObject){
		String query = "Update ETL_FEED_LOAD_MAPPING_PEND Set FEED_MAPPING_STATUS_NT = ?, FEED_MAPPING_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+" "
				+ " WHERE COUNTRY = ? AND LE_BOOK = ? AND FEED_ID = ?  AND TRAN_COLUMN_ID = ?  AND TARGET_COLUMN_ID = ? ";
			Object[] args = {vObject.getFeedMappingStatusNt() , vObject.getFeedMappingStatus() , 
					vObject.getRecordIndicatorNt() , vObject.getRecordIndicator() , vObject.getMaker() , vObject.getVerifier() , vObject.getInternalStatus() ,
					vObject.getCountry() , vObject.getLeBook() , vObject.getFeedId() , vObject.getTranColumnId() , vObject.getTargetColumnId() };

			return getJdbcTemplate().update(query,args);
		}


	@Override
	protected int doDeleteAppr(EtlFeedLoadMappingVb vObject){
		String query = "Delete From ETL_FEED_LOAD_MAPPING Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedLoadMappingVb vObject){
		String query = "Delete From ETL_FEED_LOAD_MAPPING_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_LOAD_MAPPING";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_LOAD_MAPPING_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_LOAD_MAPPING_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_LOAD_MAPPING_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = {vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_FEED_LOAD_MAPPING";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_LOAD_MAPPING_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_LOAD_MAPPING_PEND";
		}
		String query = "update "+table+" set FEED_MAPPING_STATUS = ?,RECORD_INDICATOR =?   Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(),vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected int deleteUplRecord(EtlFeedLoadMappingVb vObject){
		String query = "Delete From ETL_FEED_LOAD_MAPPING_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected String getAuditString(EtlFeedLoadMappingVb vObject){
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

			if(ValidationUtil.isValid(vObject.getTranColumnId()))
				strAudit.append("TRAN_COLUMN_ID"+auditDelimiterColVal+vObject.getTranColumnId().trim());
			else
				strAudit.append("TRAN_COLUMN_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getTargetColumnId()))
				strAudit.append("TARGET_COLUMN_ID"+auditDelimiterColVal+vObject.getTargetColumnId().trim());
			else
				strAudit.append("TARGET_COLUMN_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_MAPPING_STATUS_NT"+auditDelimiterColVal+vObject.getFeedMappingStatusNt());
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_MAPPING_STATUS"+auditDelimiterColVal+vObject.getFeedMappingStatus());
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
	protected void setServiceDefaults(){
		serviceName = "EtlFeedLoadMapping";
		serviceDesc = "ETL Feed Load Mapping";
		tableName = "ETL_FEED_LOAD_MAPPING";
		childTableName = "ETL_FEED_LOAD_MAPPING";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String query = "delete FROM ETL_FEED_LOAD_MAPPING_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_LOAD_MAPPING_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_FEED_LOAD_MAPPING_PRS.SESSION_ID ) ";
		Object[] args = {vObject.getFeedCategory() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

}
