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
import com.vision.vb.EtlTargetColumnsVb;

@Component
public class EtlTargetColumnsDao extends AbstractDao<EtlTargetColumnsVb> {

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String ColumnDatatypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TAppr.COLUMN_DATATYPE", "COLUMN_DATATYPE_DESC");
	String ColumnDatatypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TPend.COLUMN_DATATYPE", "COLUMN_DATATYPE_DESC");

	String DateFormatAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2105, "TAppr.DATE_FORMAT", "DATE_FORMAT_DESC");
	String DateFormatAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2105, "TPend.DATE_FORMAT", "DATE_FORMAT_DESC");

	String FeedColumnStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");
	String FeedColumnStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlTargetColumnsVb vObject = new EtlTargetColumnsVb();
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
				if(rs.getString("TARGET_COLUMN_ID")!= null){ 
					vObject.setTargetColumnId(rs.getString("TARGET_COLUMN_ID"));
				}else{
					vObject.setTargetColumnId("");
				}
				if(rs.getString("TARGET_COLUMN_NAME")!= null){ 
					vObject.setTargetColumnName(rs.getString("TARGET_COLUMN_NAME"));
				}else{
					vObject.setTargetColumnName("");
				}
				vObject.setTargetColumnSortOrder(rs.getInt("TARGET_COLUMN_SORT_ORDER"));
				vObject.setColumnDatatypeAt(rs.getInt("COLUMN_DATATYPE_AT"));
				if(rs.getString("COLUMN_DATATYPE")!= null){ 
					vObject.setColumnDatatype(rs.getString("COLUMN_DATATYPE"));
				}else{
					vObject.setColumnDatatype("");
				}
				vObject.setColumnLength(rs.getInt("COLUMN_LENGTH"));
				vObject.setDateFormatAt(rs.getInt("DATE_FORMAT_NT"));
				if(rs.getString("DATE_FORMAT")!= null){ 
					vObject.setDateFormat(rs.getString("DATE_FORMAT"));
				}else{
					vObject.setDateFormat("");
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
	public List<EtlTargetColumnsVb> getQueryPopupResults(EtlTargetColumnsVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TARGET_COLUMN_ID"
			+ ",TAppr.TARGET_COLUMN_NAME"
			+ ",TAppr.TARGET_COLUMN_SORT_ORDER"
			+ ",TAppr.COLUMN_DATATYPE_AT"
			+ ",TAppr.COLUMN_DATATYPE"
			+ ",TAppr.COLUMN_LENGTH"
			+ ",TAppr.DATE_FORMAT_NT"
			+ ",TAppr.DATE_FORMAT"
			+ ",TAppr.PRIMARY_KEY_FLAG"
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
			+ " from ETL_TARGET_COLUMNS TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_TARGET_COLUMNS_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.TARGET_COLUMN_ID = TPend.TARGET_COLUMN_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TARGET_COLUMN_ID"
			+ ",TPend.TARGET_COLUMN_NAME"
			+ ",TPend.TARGET_COLUMN_SORT_ORDER"
			+ ",TPend.COLUMN_DATATYPE_AT"
			+ ",TPend.COLUMN_DATATYPE"
			+ ",TPend.COLUMN_LENGTH"
			+ ",TPend.DATE_FORMAT_NT"
			+ ",TPend.DATE_FORMAT"
			+ ",TPend.PRIMARY_KEY_FLAG"
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
			+ " from ETL_TARGET_COLUMNS_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TARGET_COLUMN_ID"
			+ ",TUpl.TARGET_COLUMN_NAME"
			+ ",TUpl.TARGET_COLUMN_SORT_ORDER"
			+ ",TUpl.COLUMN_DATATYPE_AT"
			+ ",TUpl.COLUMN_DATATYPE"
			+ ",TUpl.COLUMN_LENGTH"
			+ ",TUpl.DATE_FORMAT_NT"
			+ ",TUpl.DATE_FORMAT"
			+ ",TUpl.PRIMARY_KEY_FLAG"
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
			+ " from ETL_TARGET_COLUMNS_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_TARGET_COLUMNS_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.TARGET_COLUMN_ID = TUpl.TARGET_COLUMN_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_TARGET_COLUMNS_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID AND TUpl.TARGET_COLUMN_ID = TPend.TARGET_COLUMN_ID) ");
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
			/*
			 * if (ValidationUtil.isValid(dObj.getTargetColumnId())){
			 * params.addElement(dObj.getTargetColumnId());
			 * CommonUtils.addToQuery("UPPER(TAppr.TARGET_COLUMN_ID) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.TARGET_COLUMN_ID) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.TARGET_COLUMN_ID) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getTargetColumnName())){
			 * params.addElement(dObj.getTargetColumnName());
			 * CommonUtils.addToQuery("UPPER(TAppr.TARGET_COLUMN_NAME) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.TARGET_COLUMN_NAME) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.TARGET_COLUMN_NAME) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getTargetColumnSortOrder())){
			 * params.addElement(dObj.getTargetColumnSortOrder());
			 * CommonUtils.addToQuery("TAppr.TARGET_COLUMN_SORT_ORDER = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.TARGET_COLUMN_SORT_ORDER = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.TARGET_COLUMN_SORT_ORDER = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getColumnDatatype())){
			 * params.addElement(dObj.getColumnDatatype());
			 * CommonUtils.addToQuery("UPPER(TAppr.COLUMN_DATATYPE) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.COLUMN_DATATYPE) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.COLUMN_DATATYPE) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getColumnLength())){
			 * params.addElement(dObj.getColumnLength());
			 * CommonUtils.addToQuery("TAppr.COLUMN_LENGTH = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.COLUMN_LENGTH = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.COLUMN_LENGTH = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getDateFormat())){
			 * params.addElement(dObj.getDateFormat());
			 * CommonUtils.addToQuery("UPPER(TAppr.DATE_FORMAT) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.DATE_FORMAT) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.DATE_FORMAT) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getNumberFormat())){
			 * params.addElement(dObj.getNumberFormat());
			 * CommonUtils.addToQuery("UPPER(TAppr.NUMBER_FORMAT) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.NUMBER_FORMAT) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.NUMBER_FORMAT) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getPrimaryKeyFlag())){
			 * params.addElement(dObj.getPrimaryKeyFlag());
			 * CommonUtils.addToQuery("UPPER(TAppr.PRIMARY_KEY_FLAG) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.PRIMARY_KEY_FLAG) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.PRIMARY_KEY_FLAG) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getPartitionColumnFlag())){
			 * params.addElement(dObj.getPartitionColumnFlag());
			 * CommonUtils.addToQuery("UPPER(TAppr.PARTITION_COLUMN_FLAG) = ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.PARTITION_COLUMN_FLAG) = ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.PARTITION_COLUMN_FLAG) = ?", strBufUpl); }
			 * if (ValidationUtil.isValid(dObj.getFeedColumnStatus())){
			 * params.addElement(dObj.getFeedColumnStatus());
			 * CommonUtils.addToQuery("TAppr.FEED_COLUMN_STATUS = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.FEED_COLUMN_STATUS = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.FEED_COLUMN_STATUS = ?", strBufUpl); } if
			 * (dObj.getRecordIndicator() != -1){ if (dObj.getRecordIndicator() > 3){
			 * params.addElement(new Integer(0));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufUpl); }else{
			 * params.addElement(new Integer(dObj.getRecordIndicator()));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufUpl); } }
			 */
			String orderBy = " Order By COUNTRY, LE_BOOK, FEED_ID, TARGET_COLUMN_ID ";
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
	public List<EtlTargetColumnsVb> getQueryResults(EtlTargetColumnsVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlTargetColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 4;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TARGET_COLUMN_ID"
			+ ",TAppr.TARGET_COLUMN_NAME"
			+ ",TAppr.TARGET_COLUMN_SORT_ORDER"
			+ ",TAppr.COLUMN_DATATYPE_AT"
			+ ",TAppr.COLUMN_DATATYPE"
			+ ",TAppr.COLUMN_LENGTH"
			+ ",TAppr.DATE_FORMAT_NT"
			+ ",TAppr.DATE_FORMAT"
			+ ",TAppr.PRIMARY_KEY_FLAG"
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
			+ " From ETL_TARGET_COLUMNS TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TARGET_COLUMN_ID = ? "
			+ " order by COUNTRY, LE_BOOK, FEED_ID, target_column_sort_order");
	String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TARGET_COLUMN_ID"
			+ ",TPend.TARGET_COLUMN_NAME"
			+ ",TPend.TARGET_COLUMN_SORT_ORDER"
			+ ",TPend.COLUMN_DATATYPE_AT"
			+ ",TPend.COLUMN_DATATYPE"
			+ ",TPend.COLUMN_LENGTH"
			+ ",TPend.DATE_FORMAT_NT"
			+ ",TPend.DATE_FORMAT"
			+ ",TPend.PRIMARY_KEY_FLAG"
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
			+ " From ETL_TARGET_COLUMNS_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TARGET_COLUMN_ID = ? "
			+ "  order by COUNTRY, LE_BOOK, FEED_ID, target_column_sort_order");
	String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TARGET_COLUMN_ID"
			+ ",TUpl.TARGET_COLUMN_NAME"
			+ ",TUpl.TARGET_COLUMN_SORT_ORDER"
			+ ",TUpl.COLUMN_DATATYPE_AT"
			+ ",TUpl.COLUMN_DATATYPE"
			+ ",TUpl.COLUMN_LENGTH"
			+ ",TUpl.DATE_FORMAT_NT"
			+ ",TUpl.DATE_FORMAT"
			+ ",TUpl.PRIMARY_KEY_FLAG"
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
			+ " From ETL_TARGET_COLUMNS_UPL TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TARGET_COLUMN_ID = ? "
			+ " order by COUNTRY, LE_BOOK, FEED_ID, target_column_sort_order ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();
		objParams[3] = dObj.getTargetColumnId();

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
	public List<EtlTargetColumnsVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlTargetColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TARGET_COLUMN_ID"
			+ ",TAppr.TARGET_COLUMN_NAME"
			+ ",TAppr.TARGET_COLUMN_SORT_ORDER"
			+ ",TAppr.COLUMN_DATATYPE_AT"
			+ ",TAppr.COLUMN_DATATYPE"
			+ ",TAppr.COLUMN_LENGTH"
			+ ",TAppr.DATE_FORMAT_NT"
			+ ",TAppr.DATE_FORMAT"
			+ ",TAppr.PRIMARY_KEY_FLAG"
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
			+ " From ETL_TARGET_COLUMNS TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? "
			+ " order by COUNTRY, LE_BOOK, FEED_ID, target_column_sort_order");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TARGET_COLUMN_ID"
			+ ",TPend.TARGET_COLUMN_NAME"
			+ ",TPend.TARGET_COLUMN_SORT_ORDER"
			+ ",TPend.COLUMN_DATATYPE_AT"
			+ ",TPend.COLUMN_DATATYPE"
			+ ",TPend.COLUMN_LENGTH"
			+ ",TPend.DATE_FORMAT_NT"
			+ ",TPend.DATE_FORMAT"
			+ ",TPend.PRIMARY_KEY_FLAG"
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
			+ " From ETL_TARGET_COLUMNS_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? "
			+ " order by COUNTRY, LE_BOOK, FEED_ID, target_column_sort_order ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TARGET_COLUMN_ID"
			+ ",TUpl.TARGET_COLUMN_NAME"
			+ ",TUpl.TARGET_COLUMN_SORT_ORDER"
			+ ",TUpl.COLUMN_DATATYPE_AT"
			+ ",TUpl.COLUMN_DATATYPE"
			+ ",TUpl.COLUMN_LENGTH"
			+ ",TUpl.DATE_FORMAT_NT"
			+ ",TUpl.DATE_FORMAT"
			+ ",TUpl.PRIMARY_KEY_FLAG"
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
			+ " From ETL_TARGET_COLUMNS_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? "
			+ " order by COUNTRY, LE_BOOK, FEED_ID, target_column_sort_order");

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
	protected List<EtlTargetColumnsVb> selectApprovedRecord(EtlTargetColumnsVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<EtlTargetColumnsVb> doSelectPendingRecord(EtlTargetColumnsVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}


	@Override
	protected List<EtlTargetColumnsVb> doSelectUplRecord(EtlTargetColumnsVb vObject){
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlTargetColumnsVb records){return records.getFeedColumnStatus();}


	@Override
	protected void setStatus(EtlTargetColumnsVb vObject,int status){vObject.setFeedColumnStatus(status);}

	@Override
	protected int doInsertionAppr(EtlTargetColumnsVb vObject) {
		String query = "Insert Into ETL_TARGET_COLUMNS (COUNTRY, LE_BOOK, FEED_ID, TARGET_COLUMN_ID, "
				+ "TARGET_COLUMN_NAME, TARGET_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, "
				+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, "
				+ "RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTargetColumnId(),
				vObject.getTargetColumnName(), vObject.getTargetColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatAt(),
				vObject.getDateFormat(), vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlTargetColumnsVb vObject) {
		String query = "Insert Into ETL_TARGET_COLUMNS_PEND (COUNTRY, LE_BOOK, FEED_ID, TARGET_COLUMN_ID, "
				+ "TARGET_COLUMN_NAME, TARGET_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, "
				+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, "
				+ "RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTargetColumnId(),
				vObject.getTargetColumnName(), vObject.getTargetColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatAt(),
				vObject.getDateFormat(), vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionUpl(EtlTargetColumnsVb vObject) {
		String query = "Insert Into ETL_TARGET_COLUMNS_UPL (COUNTRY, LE_BOOK, FEED_ID, TARGET_COLUMN_ID, "
				+ "TARGET_COLUMN_NAME, TARGET_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, "
				+ " PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, "
				+ "RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTargetColumnId(),
				vObject.getTargetColumnName(), vObject.getTargetColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatAt(),
				vObject.getDateFormat(), vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlTargetColumnsVb vObject) {
		String query = "Insert Into ETL_TARGET_COLUMNS_PEND (COUNTRY, LE_BOOK, FEED_ID, TARGET_COLUMN_ID, "
				+ "TARGET_COLUMN_NAME, TARGET_COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, "
				+ "DATE_FORMAT,  PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTargetColumnId(),
				vObject.getTargetColumnName(), vObject.getTargetColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatAt(),
				vObject.getDateFormat(), vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(),
				vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlTargetColumnsVb vObject) {
		String query = "Update ETL_TARGET_COLUMNS Set TARGET_COLUMN_NAME = ?, TARGET_COLUMN_SORT_ORDER = ?, COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, "
				+ "COLUMN_LENGTH = ?, DATE_FORMAT_NT = ?, DATE_FORMAT = ?, PRIMARY_KEY_FLAG = ?, "
				+ "PARTITION_COLUMN_FLAG = ?, FEED_COLUMN_STATUS_NT = ?, FEED_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, "
				+ "MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TARGET_COLUMN_ID = ? ";
		Object[] args = { vObject.getTargetColumnName(), vObject.getTargetColumnSortOrder(),
				vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(), vObject.getColumnLength(),
				vObject.getDateFormatAt(), vObject.getDateFormat(), vObject.getPrimaryKeyFlag(),
				vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getTargetColumnId(), };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlTargetColumnsVb vObject) {
		String query = "Update ETL_TARGET_COLUMNS_PEND Set TARGET_COLUMN_NAME = ?, TARGET_COLUMN_SORT_ORDER = ?, COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, "
				+ "COLUMN_LENGTH = ?, DATE_FORMAT_NT = ?, DATE_FORMAT = ?, PRIMARY_KEY_FLAG = ?, "
				+ "PARTITION_COLUMN_FLAG = ?, FEED_COLUMN_STATUS_NT = ?, FEED_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, "
				+ "MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TARGET_COLUMN_ID = ? ";
		Object[] args = { vObject.getTargetColumnName(), vObject.getTargetColumnSortOrder(),
				vObject.getColumnDatatypeAt(), vObject.getColumnDatatype(), vObject.getColumnLength(),
				vObject.getDateFormatAt(), vObject.getDateFormat(), vObject.getPrimaryKeyFlag(),
				vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNt(), vObject.getFeedColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getTargetColumnId(), };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlTargetColumnsVb vObject){
		String query = "Delete From ETL_TARGET_COLUMNS Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TARGET_COLUMN_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTargetColumnId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deletePendingRecord(EtlTargetColumnsVb vObject){
		String query = "Delete From ETL_TARGET_COLUMNS_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deleteUplRecord(EtlTargetColumnsVb vObject){
		String query = "Delete From ETL_TARGET_COLUMNS_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_TARGET_COLUMNS";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_TARGET_COLUMNS_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_TARGET_COLUMNS_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_TARGET_COLUMNS_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_TARGET_COLUMNS";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_TARGET_COLUMNS_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_TARGET_COLUMNS_PEND";
		}
		String query = "update "+table+" set FEED_COLUMN_STATUS = ? ,RECORD_INDICATOR =?  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(),vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String getAuditString(EtlTargetColumnsVb vObject){
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

			if(ValidationUtil.isValid(vObject.getTargetColumnId()))
				strAudit.append("TARGET_COLUMN_ID"+auditDelimiterColVal+vObject.getTargetColumnId().trim());
			else
				strAudit.append("TARGET_COLUMN_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getTargetColumnName()))
				strAudit.append("TARGET_COLUMN_NAME"+auditDelimiterColVal+vObject.getTargetColumnName().trim());
			else
				strAudit.append("TARGET_COLUMN_NAME"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("TARGET_COLUMN_SORT_ORDER"+auditDelimiterColVal+vObject.getTargetColumnSortOrder());
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

				strAudit.append("DATE_FORMAT_NT"+auditDelimiterColVal+vObject.getDateFormatAt());
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getDateFormat()))
				strAudit.append("DATE_FORMAT"+auditDelimiterColVal+vObject.getDateFormat().trim());
			else
				strAudit.append("DATE_FORMAT"+auditDelimiterColVal+"NULL");
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
		serviceName = "EtlTargetColumns";
		serviceDesc = "ETL Target Columns";
		tableName = "ETL_TARGET_COLUMNS";
		childTableName = "ETL_TARGET_COLUMNS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String query = "delete FROM ETL_TARGET_COLUMNS_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_TARGET_COLUMNS_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_TARGET_COLUMNS_PRS.SESSION_ID ) ";
		Object[] args = {  vObject.getFeedCategory() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

}
