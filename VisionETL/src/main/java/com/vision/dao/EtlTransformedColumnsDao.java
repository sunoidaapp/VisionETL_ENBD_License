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
import com.vision.vb.EtlTransformedColumnsVb;

@Component
public class EtlTransformedColumnsDao extends AbstractDao<EtlTransformedColumnsVb> {

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String ColumnDatatypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TAppr.COLUMN_DATATYPE", "COLUMN_DATATYPE_DESC");
	String ColumnDatatypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TPend.COLUMN_DATATYPE", "COLUMN_DATATYPE_DESC");

	String FeedColumnStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");
	String FeedColumnStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlTransformedColumnsVb vObject = new EtlTransformedColumnsVb();
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
				if(rs.getString("TRAN_COLUMN_NAME")!= null){ 
					vObject.setTranColumnName(rs.getString("TRAN_COLUMN_NAME"));
				}else{
					vObject.setTranColumnName("");
				}
				if(rs.getString("TRAN_COLUMN_DESCRIPTION")!= null){ 
					vObject.setTranColumnDescription(rs.getString("TRAN_COLUMN_DESCRIPTION"));
				}else{
					vObject.setTranColumnDescription("");
				}
				vObject.setTranColumnSortOrder(rs.getInt("TRAN_COLUMN_SORT_ORDER"));
				vObject.setColumnDatatypeAt(rs.getInt("COLUMN_DATATYPE_AT"));
				if(rs.getString("COLUMN_DATATYPE")!= null){ 
					vObject.setColumnDatatype(rs.getString("COLUMN_DATATYPE"));
				}else{
					vObject.setColumnDatatype("");
				}
				vObject.setColumnLength(rs.getInt("COLUMN_LENGTH"));
				if(rs.getString("DATE_KEY_FLAG")!= null){ 
					vObject.setDateKeyFlag(rs.getString("DATE_KEY_FLAG"));
				}else{
					vObject.setDateKeyFlag("");
				}
				
				if(rs.getString("PRIMARY_KEY_FLAG")!= null){ 
					vObject.setPrimaryKeyFlag(rs.getString("PRIMARY_KEY_FLAG"));
				}else{
					vObject.setPrimaryKeyFlag("");
				}
				if(rs.getString("PARTITION_COLUMN_FLAG")!= null){ 
					vObject.setPartitionColumnFlag(rs.getString("PARTITION_COLUMN_FLAG"));
				}else{
					vObject.setPartitionColumnFlag("");
				}
				vObject.setFeedColumnStatusNt(rs.getInt("FEED_COLUMN_STATUS_NT"));
				vObject.setFeedColumnStatus(rs.getInt("FEED_COLUMN_STATUS"));
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
	public List<EtlTransformedColumnsVb> getQueryPopupResults(EtlTransformedColumnsVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TRAN_COLUMN_ID"
			+ ",TAppr.TRAN_COLUMN_NAME"
			+ ",TAppr.TRAN_COLUMN_DESCRIPTION"
			+ ",TAppr.TRAN_COLUMN_SORT_ORDER"
			+ ",TAppr.COLUMN_DATATYPE_AT"
			+ ",TAppr.COLUMN_DATATYPE"
			+ ",TAppr.COLUMN_LENGTH"
			+ ",TAppr.PRIMARY_KEY_FLAG"
			+ ",TAppr.DATE_KEY_FLAG"
			+ ",TAppr.PARTITION_COLUMN_FLAG"
			+ ",TAppr.FEED_COLUMN_STATUS_NT"
			+ ",TAppr.FEED_COLUMN_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " from ETL_TRANSFORMED_COLUMNS TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_TRANSFORMED_COLUMNS_PEND TPend WHERE TAppr.COUNTRY = "
				+ " TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.TRAN_COLUMN_ID = "
				+ " TPend.TRAN_COLUMN_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TRAN_COLUMN_ID"
			+ ",TPend.TRAN_COLUMN_NAME"
			+ ",TPend.TRAN_COLUMN_DESCRIPTION"
			+ ",TPend.TRAN_COLUMN_SORT_ORDER"
			+ ",TPend.COLUMN_DATATYPE_AT"
			+ ",TPend.COLUMN_DATATYPE"
			+ ",TPend.COLUMN_LENGTH"
			+ ",TPend.PRIMARY_KEY_FLAG"
			+ ",TPend.DATE_KEY_FLAG"
			+ ",TPend.PARTITION_COLUMN_FLAG"
			+ ",TPend.FEED_COLUMN_STATUS_NT"
			+ ",TPend.FEED_COLUMN_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " from ETL_TRANSFORMED_COLUMNS_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TRAN_COLUMN_ID"
			+ ",TUpl.TRAN_COLUMN_NAME"
			+ ",TUpl.TRAN_COLUMN_DESCRIPTION"
			+ ",TUpl.TRAN_COLUMN_SORT_ORDER"
			+ ",TUpl.COLUMN_DATATYPE_AT"
			+ ",TUpl.COLUMN_DATATYPE"
			+ ",TUpl.COLUMN_LENGTH"
			+ ",TUpl.PRIMARY_KEY_FLAG"
			+ ",TUpl.DATE_KEY_FLAG"
			+ ",TUpl.PARTITION_COLUMN_FLAG"
			+ ",TUpl.FEED_COLUMN_STATUS_NT"
			+ ",TUpl.FEED_COLUMN_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " from ETL_TRANSFORMED_COLUMNS_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_TRANSFORMED_COLUMNS_UPL TUpl WHERE "
				+ " TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND "
				+ " TAppr.TRAN_COLUMN_ID = TUpl.TRAN_COLUMN_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_TRANSFORMED_COLUMNS_UPL TUpl WHERE "
				+ " TUpl.COUNTRY = TPend.COUNTRY AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID AND "
				+ " TUpl.TRAN_COLUMN_ID = TPend.TRAN_COLUMN_ID) ");
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
				CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getFeedId())) {
				params.addElement(dObj.getFeedId());
				CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
			}
			String orderBy = " Order By COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID ";
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

	public List<EtlTransformedColumnsVb> getQueryResults(EtlTransformedColumnsVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlTransformedColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TRAN_COLUMN_ID"
			+ ",TAppr.TRAN_COLUMN_NAME"
			+ ",TAppr.TRAN_COLUMN_DESCRIPTION"
			+ ",TAppr.TRAN_COLUMN_SORT_ORDER"
			+ ",TAppr.COLUMN_DATATYPE_AT"
			+ ",TAppr.COLUMN_DATATYPE"
			+ ",TAppr.COLUMN_LENGTH"
			+ ",TAppr.PRIMARY_KEY_FLAG"
			+ ",TAppr.DATE_KEY_FLAG"
			+ ",TAppr.PARTITION_COLUMN_FLAG"
			+ ",TAppr.FEED_COLUMN_STATUS_NT"
			+ ",TAppr.FEED_COLUMN_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_TRANSFORMED_COLUMNS TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TRAN_COLUMN_ID"
			+ ",TPend.TRAN_COLUMN_NAME"
			+ ",TPend.TRAN_COLUMN_DESCRIPTION"
			+ ",TPend.TRAN_COLUMN_SORT_ORDER"
			+ ",TPend.COLUMN_DATATYPE_AT"
			+ ",TPend.COLUMN_DATATYPE"
			+ ",TPend.COLUMN_LENGTH"
			+ ",TPend.PRIMARY_KEY_FLAG"
			+ ",TPend.DATE_KEY_FLAG"
			+ ",TPend.PARTITION_COLUMN_FLAG"
			+ ",TPend.FEED_COLUMN_STATUS_NT"
			+ ",TPend.FEED_COLUMN_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_TRANSFORMED_COLUMNS_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?    ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TRAN_COLUMN_ID"
			+ ",TUpl.TRAN_COLUMN_NAME"
			+ ",TUpl.TRAN_COLUMN_DESCRIPTION"
			+ ",TUpl.TRAN_COLUMN_SORT_ORDER"
			+ ",TUpl.COLUMN_DATATYPE_AT"
			+ ",TUpl.COLUMN_DATATYPE"
			+ ",TUpl.COLUMN_LENGTH"
			+ ",TUpl.PRIMARY_KEY_FLAG"
			+ ",TUpl.DATE_KEY_FLAG"
			+ ",TUpl.PARTITION_COLUMN_FLAG"
			+ ",TUpl.FEED_COLUMN_STATUS_NT"
			+ ",TUpl.FEED_COLUMN_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_TRANSFORMED_COLUMNS_UPL TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?     ");

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
	
	public List<EtlTransformedColumnsVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlTransformedColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TRAN_COLUMN_ID"
			+ ",TAppr.TRAN_COLUMN_NAME"
			+ ",TAppr.TRAN_COLUMN_DESCRIPTION"
			+ ",TAppr.TRAN_COLUMN_SORT_ORDER"
			+ ",TAppr.COLUMN_DATATYPE_AT"
			+ ",TAppr.COLUMN_DATATYPE"
			+ ",TAppr.COLUMN_LENGTH"
			+ ",TAppr.PRIMARY_KEY_FLAG"
			+ ",TAppr.DATE_KEY_FLAG"
			+ ",TAppr.PARTITION_COLUMN_FLAG"
			+ ",TAppr.FEED_COLUMN_STATUS_NT"
			+ ",TAppr.FEED_COLUMN_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_TRANSFORMED_COLUMNS TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TRAN_COLUMN_ID"
			+ ",TPend.TRAN_COLUMN_NAME"
			+ ",TPend.TRAN_COLUMN_DESCRIPTION"
			+ ",TPend.TRAN_COLUMN_SORT_ORDER"
			+ ",TPend.COLUMN_DATATYPE_AT"
			+ ",TPend.COLUMN_DATATYPE"
			+ ",TPend.COLUMN_LENGTH"
			+ ",TPend.PRIMARY_KEY_FLAG"
			+ ",TPend.DATE_KEY_FLAG"
			+ ",TPend.PARTITION_COLUMN_FLAG"
			+ ",TPend.FEED_COLUMN_STATUS_NT"
			+ ",TPend.FEED_COLUMN_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_TRANSFORMED_COLUMNS_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TRAN_COLUMN_ID"
			+ ",TUpl.TRAN_COLUMN_NAME"
			+ ",TUpl.TRAN_COLUMN_DESCRIPTION"
			+ ",TUpl.TRAN_COLUMN_SORT_ORDER"
			+ ",TUpl.COLUMN_DATATYPE_AT"
			+ ",TUpl.COLUMN_DATATYPE"
			+ ",TUpl.COLUMN_LENGTH"
			+ ",TUpl.PRIMARY_KEY_FLAG"
			+ ",TUpl.DATE_KEY_FLAG"
			+ ",TUpl.PARTITION_COLUMN_FLAG"
			+ ",TUpl.FEED_COLUMN_STATUS_NT"
			+ ",TUpl.FEED_COLUMN_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_TRANSFORMED_COLUMNS_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();
		
		String orderby = " ORDER BY COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID ";
		strQueryUpl = strQueryUpl + orderby;
		strQueryAppr = strQueryAppr + orderby;
		strQueryPend = strQueryPend + orderby;
		
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
	protected List<EtlTransformedColumnsVb> selectApprovedRecord(EtlTransformedColumnsVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlTransformedColumnsVb> doSelectPendingRecord(EtlTransformedColumnsVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlTransformedColumnsVb> doSelectUplRecord(EtlTransformedColumnsVb vObject){
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlTransformedColumnsVb records){return records.getFeedColumnStatus();}

	@Override
	protected void setStatus(EtlTransformedColumnsVb vObject,int status){vObject.setFeedColumnStatus(status);}

	@Override
	protected int doInsertionAppr(EtlTransformedColumnsVb vObject) {
		String query = "Insert Into ETL_TRANSFORMED_COLUMNS (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TRAN_COLUMN_NAME, "
				+ "TRAN_COLUMN_DESCRIPTION, TRAN_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, "
				+ "COLUMN_DATATYPE, COLUMN_LENGTH, "
				+ "PRIMARY_KEY_FLAG, DATE_KEY_FLAG,PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ "MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(),
				vObject.getTranColumnName(), vObject.getTranColumnDescription(), vObject.getTranColumnSortOrder(),
				vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(), vObject.getColumnLength(),
				vObject.getPrimaryKeyFlag(), vObject.getDateKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
		return getJdbcTemplate().update(query, args);
	}


	@Override
	protected int doInsertionPend(EtlTransformedColumnsVb vObject) {
		String query = "Insert Into ETL_TRANSFORMED_COLUMNS_PEND (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TRAN_COLUMN_NAME, "
				+ "TRAN_COLUMN_DESCRIPTION, TRAN_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, "
				+ "COLUMN_DATATYPE, COLUMN_LENGTH, "
				+ "PRIMARY_KEY_FLAG, DATE_KEY_FLAG,PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ "MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(),
				vObject.getTranColumnName(), vObject.getTranColumnDescription(), vObject.getTranColumnSortOrder(),
				vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(), vObject.getColumnLength(),
				vObject.getPrimaryKeyFlag(), vObject.getDateKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
		return getJdbcTemplate().update(query, args);
	}

	protected int doInsertionUpl(EtlTransformedColumnsVb vObject) {
		String query = "Insert Into ETL_TRANSFORMED_COLUMNS_UPL (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TRAN_COLUMN_NAME, "
				+ "TRAN_COLUMN_DESCRIPTION, TRAN_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, "
				+ "COLUMN_DATATYPE, COLUMN_LENGTH, "
				+ "PRIMARY_KEY_FLAG,DATE_KEY_FLAG, PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ "MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION )"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(),
				vObject.getTranColumnName(), vObject.getTranColumnDescription(), vObject.getTranColumnSortOrder(),
				vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(), vObject.getColumnLength(),
				vObject.getPrimaryKeyFlag(), vObject.getDateKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlTransformedColumnsVb vObject) {
		String query = "Insert Into ETL_TRANSFORMED_COLUMNS_UPL (COUNTRY, LE_BOOK, FEED_ID, TRAN_COLUMN_ID, TRAN_COLUMN_NAME, "
				+ "TRAN_COLUMN_DESCRIPTION, TRAN_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, "
				+ "COLUMN_DATATYPE, COLUMN_LENGTH, "
				+ "PRIMARY_KEY_FLAG, DATE_KEY_FLAG,PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, "
				+ "MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTranColumnId(),
				vObject.getTranColumnName(), vObject.getTranColumnDescription(), vObject.getTranColumnSortOrder(),
				vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(), vObject.getColumnLength(),
				vObject.getPrimaryKeyFlag(), vObject.getDateKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
		return getJdbcTemplate().update(query, args);
	}
	@Override
	protected int doUpdateAppr(EtlTransformedColumnsVb vObject) {
		String query = "Update ETL_TRANSFORMED_COLUMNS Set TRAN_COLUMN_NAME = ?, TRAN_COLUMN_DESCRIPTION = ?, TRAN_COLUMN_SORT_ORDER = ?, "
				+ "COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, COLUMN_LENGTH = ?, PRIMARY_KEY_FLAG = ?, DATE_KEY_FLAG = ?, "
				+ "PARTITION_COLUMN_FLAG = ?, FEED_COLUMN_STATUS_NT = ?, FEED_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, "
				+ "INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ ",  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TRAN_COLUMN_ID = ? ";
		Object[] args = { vObject.getTranColumnName(), vObject.getTranColumnDescription(),
				vObject.getTranColumnSortOrder(), vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(),
				vObject.getColumnLength(), vObject.getPrimaryKeyFlag(), vObject.getDateKeyFlag(),
				vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getTranColumnId() };
		return getJdbcTemplate().update(query, args);
	}


	@Override
	protected int doUpdatePend(EtlTransformedColumnsVb vObject) {
		String query = "Update ETL_TRANSFORMED_COLUMNS_PEND Set  TRAN_COLUMN_NAME = ?, TRAN_COLUMN_DESCRIPTION = ?, TRAN_COLUMN_SORT_ORDER = ?, "
				+ "COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, COLUMN_LENGTH = ?, PRIMARY_KEY_FLAG = ?, DATE_KEY_FLAG = ?,"
				+ "PARTITION_COLUMN_FLAG = ?, FEED_COLUMN_STATUS_NT = ?, FEED_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, "
				+ "INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ ",  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TRAN_COLUMN_ID = ? ";
		Object[] args = { vObject.getTranColumnName(), vObject.getTranColumnDescription(),
				vObject.getTranColumnSortOrder(), vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(),
				vObject.getColumnLength(), vObject.getPrimaryKeyFlag(), vObject.getDateKeyFlag(),
				vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getTranColumnId() };
		return getJdbcTemplate().update(query, args);
	}


	@Override
	protected int doDeleteAppr(EtlTransformedColumnsVb vObject){
		String query = "Delete From ETL_TRANSFORMED_COLUMNS Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deletePendingRecord(EtlTransformedColumnsVb vObject){
		String query = "Delete From ETL_TRANSFORMED_COLUMNS_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deleteUplRecord(EtlTransformedColumnsVb vObject){
		String query = "Delete From ETL_TRANSFORMED_COLUMNS_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	
	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_TRANSFORMED_COLUMNS";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_TRANSFORMED_COLUMNS_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_TRANSFORMED_COLUMNS_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_TRANSFORMED_COLUMNS_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

	
	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_TRANSFORMED_COLUMNS";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_TRANSFORMED_COLUMNS_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_TRANSFORMED_COLUMNS_PEND";
		}
		String query = "update "+table+" set FEED_COLUMN_STATUS = ?,RECORD_INDICATOR =?  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(),vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String getAuditString(EtlTransformedColumnsVb vObject){
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

			if(ValidationUtil.isValid(vObject.getTranColumnName()))
				strAudit.append("TRAN_COLUMN_NAME"+auditDelimiterColVal+vObject.getTranColumnName().trim());
			else
				strAudit.append("TRAN_COLUMN_NAME"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getTranColumnDescription()))
				strAudit.append("TRAN_COLUMN_DESCRIPTION"+auditDelimiterColVal+vObject.getTranColumnDescription().trim());
			else
				strAudit.append("TRAN_COLUMN_DESCRIPTION"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("TRAN_COLUMN_SORT_ORDER"+auditDelimiterColVal+vObject.getTranColumnSortOrder());
			strAudit.append(auditDelimiter);

				strAudit.append("COLUMN_DATATYPE_AT"+auditDelimiterColVal+vObject.getColumnDatatypeAt());
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getColumnDatatype()))
				strAudit.append("COLUMN_DATATYPE"+auditDelimiterColVal+vObject.getColumnDatatype().trim());
			else
				strAudit.append("COLUMN_DATATYPE"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("COLUMN_LENGTH"+auditDelimiterColVal+vObject.getColumnLength());
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getDateKeyFlag()))
				strAudit.append("DATE_KEY_FLAG"+auditDelimiterColVal+vObject.getDateKeyFlag().trim());
			else
				strAudit.append("DATE_KEY_FLAG"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getPrimaryKeyFlag()))
				strAudit.append("PRIMARY_KEY_FLAG"+auditDelimiterColVal+vObject.getPrimaryKeyFlag().trim());
			else
				strAudit.append("PRIMARY_KEY_FLAG"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getPartitionColumnFlag()))
				strAudit.append("PARTITION_COLUMN_FLAG"+auditDelimiterColVal+vObject.getPartitionColumnFlag().trim());
			else
				strAudit.append("PARTITION_COLUMN_FLAG"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_COLUMN_STATUS_NT"+auditDelimiterColVal+vObject.getFeedColumnStatusNt());
			strAudit.append(auditDelimiter);

				strAudit.append("FEED_COLUMN_STATUS"+auditDelimiterColVal+vObject.getFeedColumnStatus());
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
		serviceName = "EtlTransformedColumns";
		serviceDesc = "ETL Transformed Columns";
		tableName = "ETL_TRANSFORMED_COLUMNS";
		childTableName = "ETL_TRANSFORMED_COLUMNS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_TRANSFORMED_COLUMNS_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "  FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_TRANSFORMED_COLUMNS_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_TRANSFORMED_COLUMNS_PRS.SESSION_ID ) ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

}
