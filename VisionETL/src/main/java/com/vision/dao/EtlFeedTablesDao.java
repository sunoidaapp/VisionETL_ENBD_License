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
import com.vision.vb.ETLFeedTablesVb;
import com.vision.vb.EtlFeedMainVb;

@Component
public class EtlFeedTablesDao extends AbstractDao<ETLFeedTablesVb> {

/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String TableTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2103, "TAppr.TABLE_TYPE", "TABLE_TYPE_DESC");
	String TableTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2103, "TPend.TABLE_TYPE", "TABLE_TYPE_DESC");

	String FeedTableStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_TABLE_STATUS", "FEED_TABLE_STATUS_DESC");
	String FeedTableStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_TABLE_STATUS", "FEED_TABLE_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ETLFeedTablesVb vObject = new ETLFeedTablesVb();
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
				vObject.setTableId(rs.getInt("TABLE_ID"));
				if(rs.getString("SOURCE_CONNECTOR_ID")!= null){ 
					vObject.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
				}else{
					vObject.setSourceConnectorId("");
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
				vObject.setTableSortOrder(rs.getInt("TABLE_SORT_ORDER"));
				vObject.setTableTypeAt(rs.getInt("TABLE_TYPE_AT"));
				if(rs.getString("TABLE_TYPE")!= null){ 
					vObject.setTableType(rs.getString("TABLE_TYPE"));
				}else{
					vObject.setTableType("");
				}
				if(rs.getString("TABLE_TYPE_DESC")!= null){ 
					vObject.setTableTypeDesc(rs.getString("TABLE_TYPE_DESC"));
				}else{
					vObject.setTableTypeDesc("");
				}
				if(rs.getString("QUERY_ID")!= null){ 
					vObject.setQueryId(rs.getString("QUERY_ID"));
				}else{
					vObject.setQueryId("");
				}
				if(rs.getString("CUSTOM_PARTITION_COLUMN_FLAG")!= null){ 
					vObject.setCustomPartitionColumnFlag(rs.getString("CUSTOM_PARTITION_COLUMN_FLAG"));
				}else{
					vObject.setCustomPartitionColumnFlag("");
				}
				if(rs.getString("PARTITION_COLUMN_NAME")!= null){ 
					vObject.setPartitionColumnName(rs.getString("PARTITION_COLUMN_NAME"));
				}else{
					vObject.setPartitionColumnName("");
				}
				if(rs.getString("BASE_TABLE_FLAG")!= null){ 
					vObject.setBaseTableFlag(rs.getString("BASE_TABLE_FLAG"));
				}else{
					vObject.setBaseTableFlag("");
				}
				vObject.setFeedTableStatusNt(rs.getInt("FEED_TABLE_STATUS_NT"));
				vObject.setFeedTableStatus(rs.getInt("FEED_TABLE_STATUS"));
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
	public List<ETLFeedTablesVb> getQueryPopupResults(ETLFeedTablesVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TABLE_ID"
			+ ",TAppr.SOURCE_CONNECTOR_ID"
			+ ",TAppr.TABLE_NAME"
			+ ",TAppr.TABLE_ALIAS_NAME"
			+ ",TAppr.TABLE_SORT_ORDER"
			+ ",TAppr.TABLE_TYPE_AT"
			+ ",TAppr.TABLE_TYPE,"+TableTypeAtApprDesc
			+ ",TAppr.QUERY_ID"
			+ ",TAppr.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TAppr.PARTITION_COLUMN_NAME"
			+ ",TAppr.BASE_TABLE_FLAG"
			+ ",TAppr.FEED_TABLE_STATUS_NT"
			+ ",TAppr.FEED_TABLE_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" from ETL_FEED_TABLES TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FEED_TABLES_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.TABLE_ID = TPend.TABLE_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TABLE_ID"
			+ ",TPend.SOURCE_CONNECTOR_ID"
			+ ",TPend.TABLE_NAME"
			+ ",TPend.TABLE_ALIAS_NAME"
			+ ",TPend.TABLE_SORT_ORDER"
			+ ",TPend.TABLE_TYPE_AT"
			+ ",TPend.TABLE_TYPE, "+TableTypeAtPendDesc
			+ ",TPend.QUERY_ID"
			+ ",TPend.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TPend.PARTITION_COLUMN_NAME"
			+ ",TPend.BASE_TABLE_FLAG"
			+ ",TPend.FEED_TABLE_STATUS_NT"
			+ ",TPend.FEED_TABLE_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" from ETL_FEED_TABLES_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TABLE_ID"
			+ ",TUpl.SOURCE_CONNECTOR_ID"
			+ ",TUpl.TABLE_NAME"
			+ ",TUpl.TABLE_ALIAS_NAME"
			+ ",TUpl.TABLE_SORT_ORDER"
			+ ",TUpl.TABLE_TYPE_AT"
			+ ",TUpl.TABLE_TYPE, "+TableTypeAtApprDesc.replaceAll("TAppr.", "TUpl.")
			+ ",TUpl.QUERY_ID"
			+ ",TUpl.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TUpl.PARTITION_COLUMN_NAME"
			+ ",TUpl.BASE_TABLE_FLAG"
			+ ",TUpl.FEED_TABLE_STATUS_NT"
			+ ",TUpl.FEED_TABLE_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" from ETL_FEED_TABLES_UPL TUpl ");

		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_TABLES_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY "
				+ "AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.TABLE_ID = TUpl.TABLE_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_TABLES_UPL TUpl WHERE TPend.COUNTRY = TUpl.COUNTRY "
				+ "AND TPend.LE_BOOK = TUpl.LE_BOOK AND TPend.FEED_ID = TUpl.FEED_ID AND TPend.TABLE_ID = TUpl.TABLE_ID) ");
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
				if (ValidationUtil.isValid(dObj.getSourceConnectorId())){
					params.addElement(dObj.getSourceConnectorId());
					CommonUtils.addToQuery("UPPER(TAppr.SOURCE_CONNECTOR_ID) = ?", strBufApprove);
					CommonUtils.addToQuery("UPPER(TPend.SOURCE_CONNECTOR_ID) = ?", strBufPending);
					CommonUtils.addToQuery("UPPER(TUpl.SOURCE_CONNECTOR_ID) = ?", strBufUpl);
			}
/*				if (ValidationUtil.isValid(dObj.getTableId())){
						params.addElement(dObj.getTableId());
						CommonUtils.addToQuery("UPPER(TAppr.TABLE_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.TABLE_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.TABLE_ID) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getSourceConnectorId())){
						params.addElement(dObj.getSourceConnectorId());
						CommonUtils.addToQuery("UPPER(TAppr.SOURCE_CONNECTOR_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.SOURCE_CONNECTOR_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.SOURCE_CONNECTOR_ID) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getTableName())){
						params.addElement(dObj.getTableName());
						CommonUtils.addToQuery("UPPER(TAppr.TABLE_NAME) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.TABLE_NAME) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.TABLE_NAME) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getTableAliasName())){
						params.addElement(dObj.getTableAliasName());
						CommonUtils.addToQuery("UPPER(TAppr.TABLE_ALIAS_NAME) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.TABLE_ALIAS_NAME) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.TABLE_ALIAS_NAME) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getTableSortOrder())){
						params.addElement(dObj.getTableSortOrder());
						CommonUtils.addToQuery("TAppr.TABLE_SORT_ORDER = ?", strBufApprove);
						CommonUtils.addToQuery("TPend.TABLE_SORT_ORDER = ?", strBufPending);
						CommonUtils.addToQuery("TUpl.TABLE_SORT_ORDER = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getTableType())){
						params.addElement(dObj.getTableType());
						CommonUtils.addToQuery("UPPER(TAppr.TABLE_TYPE) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.TABLE_TYPE) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.TABLE_TYPE) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getQueryId())){
						params.addElement(dObj.getQueryId());
						CommonUtils.addToQuery("UPPER(TAppr.QUERY_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.QUERY_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.QUERY_ID) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getCustomPartitionColumnFlag())){
						params.addElement(dObj.getCustomPartitionColumnFlag());
						CommonUtils.addToQuery("UPPER(TAppr.CUSTOM_PARTITION_COLUMN_FLAG) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.CUSTOM_PARTITION_COLUMN_FLAG) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.CUSTOM_PARTITION_COLUMN_FLAG) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getPartitionColumnName())){
						params.addElement(dObj.getPartitionColumnName());
						CommonUtils.addToQuery("UPPER(TAppr.PARTITION_COLUMN_NAME) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.PARTITION_COLUMN_NAME) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.PARTITION_COLUMN_NAME) = ?", strBufPending);
				}
				if (ValidationUtil.isValid(dObj.getBaseTableFlag())){
						params.addElement(dObj.getBaseTableFlag());
						CommonUtils.addToQuery("UPPER(TAppr.BASE_TABLE_FLAG) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.BASE_TABLE_FLAG) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.BASE_TABLE_FLAG) = ?", strBufPending);
				}
			
				if (ValidationUtil.isValid(dObj.getFeedTableStatus())){
						params.addElement(dObj.getFeedTableStatus());
						CommonUtils.addToQuery("TAppr.FEED_TABLE_STATUS = ?", strBufApprove);
						CommonUtils.addToQuery("TPend.FEED_TABLE_STATUS = ?", strBufPending);
						CommonUtils.addToQuery("TUpl.FEED_TABLE_STATUS = ?", strBufPending);
				}
				if (dObj.getRecordIndicator() != -1){
					if (dObj.getRecordIndicator() > 3){
						params.addElement(new Integer(0));
						CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
						CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
						CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufPending);
					}else{
						 params.addElement(new Integer(dObj.getRecordIndicator()));
						 CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
						 CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
						 CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufPending);
					}
				}*/
			String orderBy=" Order By COUNTRY, LE_BOOK ,FEED_ID ,TABLE_ID ";
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
	public List<ETLFeedTablesVb> getQueryResults(ETLFeedTablesVb dObj, int intStatus){
		List<ETLFeedTablesVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TABLE_ID"
			+ ",TAppr.SOURCE_CONNECTOR_ID"
			+ ",TAppr.TABLE_NAME"
			+ ",TAppr.TABLE_ALIAS_NAME"
			+ ",TAppr.TABLE_SORT_ORDER"
			+ ",TAppr.TABLE_TYPE_AT"
			+ ",TAppr.TABLE_TYPE, "+TableTypeAtApprDesc
			+ ",TAppr.QUERY_ID"
			+ ",TAppr.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TAppr.PARTITION_COLUMN_NAME"
			+ ",TAppr.BASE_TABLE_FLAG"
			+ ",TAppr.FEED_TABLE_STATUS_NT"
			+ ",TAppr.FEED_TABLE_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_TABLES TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TABLE_ID"
			+ ",TPend.SOURCE_CONNECTOR_ID"
			+ ",TPend.TABLE_NAME"
			+ ",TPend.TABLE_ALIAS_NAME"
			+ ",TPend.TABLE_SORT_ORDER"
			+ ",TPend.TABLE_TYPE_AT"
			+ ",TPend.TABLE_TYPE, "+TableTypeAtPendDesc
			+ ",TPend.QUERY_ID"
			+ ",TPend.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TPend.PARTITION_COLUMN_NAME"
			+ ",TPend.BASE_TABLE_FLAG"
			+ ",TPend.FEED_TABLE_STATUS_NT"
			+ ",TPend.FEED_TABLE_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_TABLES_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?     ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TABLE_ID"
			+ ",TUpl.SOURCE_CONNECTOR_ID"
			+ ",TUpl.TABLE_NAME"
			+ ",TUpl.TABLE_ALIAS_NAME"
			+ ",TUpl.TABLE_SORT_ORDER"
			+ ",TUpl.TABLE_TYPE_AT"
			+ ",TUpl.TABLE_TYPE, "+TableTypeAtApprDesc.replaceAll("TAppr.", "TUpl.")
			+ ",TUpl.QUERY_ID"
			+ ",TUpl.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TUpl.PARTITION_COLUMN_NAME"
			+ ",TUpl.BASE_TABLE_FLAG"
			+ ",TUpl.FEED_TABLE_STATUS_NT"
			+ ",TUpl.FEED_TABLE_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_TABLES_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");

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
	public List<ETLFeedTablesVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		List<ETLFeedTablesVb> collTemp = null;
	 int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.TABLE_ID"
			+ ",TAppr.SOURCE_CONNECTOR_ID"
			+ ",TAppr.TABLE_NAME"
			+ ",TAppr.TABLE_ALIAS_NAME"
			+ ",TAppr.TABLE_SORT_ORDER"
			+ ",TAppr.TABLE_TYPE_AT"
			+ ",TAppr.TABLE_TYPE, "+TableTypeAtApprDesc
			+ ",TAppr.QUERY_ID"
			+ ",TAppr.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TAppr.PARTITION_COLUMN_NAME"
			+ ",TAppr.BASE_TABLE_FLAG"
			+ ",TAppr.FEED_TABLE_STATUS_NT"
			+ ",TAppr.FEED_TABLE_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_TABLES TAppr WHERE TAppr.COUNTRY = ?  AND TAppr.LE_BOOK = ?  AND TAppr.FEED_ID = ? ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.TABLE_ID"
			+ ",TPend.SOURCE_CONNECTOR_ID"
			+ ",TPend.TABLE_NAME"
			+ ",TPend.TABLE_ALIAS_NAME"
			+ ",TPend.TABLE_SORT_ORDER"
			+ ",TPend.TABLE_TYPE_AT"
			+ ",TPend.TABLE_TYPE, "+TableTypeAtPendDesc
			+ ",TPend.QUERY_ID"
			+ ",TPend.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TPend.PARTITION_COLUMN_NAME"
			+ ",TPend.BASE_TABLE_FLAG"
			+ ",TPend.FEED_TABLE_STATUS_NT"
			+ ",TPend.FEED_TABLE_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_TABLES_PEND TPend WHERE TPend.COUNTRY = ?  AND TPend.LE_BOOK = ?  AND TPend.FEED_ID = ? ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.TABLE_ID"
			+ ",TUpl.SOURCE_CONNECTOR_ID"
			+ ",TUpl.TABLE_NAME"
			+ ",TUpl.TABLE_ALIAS_NAME"
			+ ",TUpl.TABLE_SORT_ORDER"
			+ ",TUpl.TABLE_TYPE_AT"
			+ ",TUpl.TABLE_TYPE, "+TableTypeAtApprDesc.replaceAll("TAppr.", "TUpl.")
			+ ",TUpl.QUERY_ID"
			+ ",TUpl.CUSTOM_PARTITION_COLUMN_FLAG"
			+ ",TUpl.PARTITION_COLUMN_NAME"
			+ ",TUpl.BASE_TABLE_FLAG"
			+ ",TUpl.FEED_TABLE_STATUS_NT"
			+ ",TUpl.FEED_TABLE_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
			+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
			+" From ETL_FEED_TABLES_UPL TUpl WHERE TUpl.COUNTRY = ?  AND TUpl.LE_BOOK = ?  AND TUpl.FEED_ID = ? ");

		Object objParams[] = new Object[intKeyFieldsCount];

		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		if(ValidationUtil.isValid(dObj.getConnectorId())) {
			intKeyFieldsCount = 4;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();

			objParams[3] = dObj.getConnectorId();
			
			strQueryUpl = strQueryUpl+"AND  SOURCE_CONNECTOR_ID = ?";
			strQueryAppr = strQueryAppr+"AND  SOURCE_CONNECTOR_ID = ?";
			strQueryPend = strQueryPend+"AND  SOURCE_CONNECTOR_ID = ?";

		}
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
	protected List<ETLFeedTablesVb> selectApprovedRecord(ETLFeedTablesVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<ETLFeedTablesVb> doSelectPendingRecord(ETLFeedTablesVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}


	@Override
	protected List<ETLFeedTablesVb> doSelectUplRecord(ETLFeedTablesVb vObject){
		return getQueryResults(vObject, 9999);
	}


	@Override
	protected int getStatus(ETLFeedTablesVb records){return records.getFeedTableStatus();}


	@Override
	protected void setStatus(ETLFeedTablesVb vObject,int status){vObject.setFeedTableStatus(status);}

	@Override
	protected int doInsertionAppr(ETLFeedTablesVb vObject) {
		String query = "Insert Into ETL_FEED_TABLES (COUNTRY, "
				+ "LE_BOOK, FEED_ID, TABLE_ID, SOURCE_CONNECTOR_ID, TABLE_NAME, TABLE_ALIAS_NAME, TABLE_SORT_ORDER, TABLE_TYPE_AT, TABLE_TYPE, QUERY_ID, "
				+ "CUSTOM_PARTITION_COLUMN_FLAG, PARTITION_COLUMN_NAME, BASE_TABLE_FLAG,  FEED_TABLE_STATUS_NT, FEED_TABLE_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getSourceConnectorId(), vObject.getTableName(), vObject.getTableAliasName(),
				vObject.getTableSortOrder(), vObject.getTableTypeAt(), vObject.getTableType(), vObject.getQueryId(),
				vObject.getCustomPartitionColumnFlag(), vObject.getPartitionColumnName(), vObject.getBaseTableFlag(),
				vObject.getFeedTableStatusNt(), vObject.getFeedTableStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}


	@Override
	protected int doInsertionPend(ETLFeedTablesVb vObject) {
		String query = "Insert Into ETL_FEED_TABLES_PEND (COUNTRY, LE_BOOK, FEED_ID, "
				+ "TABLE_ID, SOURCE_CONNECTOR_ID, TABLE_NAME, TABLE_ALIAS_NAME, TABLE_SORT_ORDER, TABLE_TYPE_AT, TABLE_TYPE, QUERY_ID, "
				+ "CUSTOM_PARTITION_COLUMN_FLAG, PARTITION_COLUMN_NAME, BASE_TABLE_FLAG, "
				+ "FEED_TABLE_STATUS_NT, FEED_TABLE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getSourceConnectorId(), vObject.getTableName(), vObject.getTableAliasName(),
				vObject.getTableSortOrder(), vObject.getTableTypeAt(), vObject.getTableType(), vObject.getQueryId(),
				vObject.getCustomPartitionColumnFlag(), vObject.getPartitionColumnName(), vObject.getBaseTableFlag(),
				vObject.getFeedTableStatusNt(), vObject.getFeedTableStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionUpl(ETLFeedTablesVb vObject) {
		String query = "Insert Into ETL_FEED_TABLES_UPL (COUNTRY, LE_BOOK, FEED_ID, "
				+ "TABLE_ID, SOURCE_CONNECTOR_ID, TABLE_NAME, TABLE_ALIAS_NAME, TABLE_SORT_ORDER, TABLE_TYPE_AT, TABLE_TYPE, QUERY_ID, "
				+ "CUSTOM_PARTITION_COLUMN_FLAG, PARTITION_COLUMN_NAME, BASE_TABLE_FLAG,  "
				+ "FEED_TABLE_STATUS_NT, FEED_TABLE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getSourceConnectorId(), vObject.getTableName(), vObject.getTableAliasName(),
				vObject.getTableSortOrder(), vObject.getTableTypeAt(), vObject.getTableType(), vObject.getQueryId(),
				vObject.getCustomPartitionColumnFlag(), vObject.getPartitionColumnName(), vObject.getBaseTableFlag(),
				vObject.getFeedTableStatusNt(), vObject.getFeedTableStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(ETLFeedTablesVb vObject) {
		String query = "Insert Into ETL_FEED_TABLES_PEND (COUNTRY, LE_BOOK, FEED_ID, TABLE_ID, SOURCE_CONNECTOR_ID, "
				+ "TABLE_NAME, TABLE_ALIAS_NAME, TABLE_SORT_ORDER, TABLE_TYPE_AT, TABLE_TYPE, QUERY_ID, CUSTOM_PARTITION_COLUMN_FLAG, "
				+ "PARTITION_COLUMN_NAME, BASE_TABLE_FLAG,  FEED_TABLE_STATUS_NT, FEED_TABLE_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + "  )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(),
				vObject.getSourceConnectorId(), vObject.getTableName(), vObject.getTableAliasName(),
				vObject.getTableSortOrder(), vObject.getTableTypeAt(), vObject.getTableType(), vObject.getQueryId(),
				vObject.getCustomPartitionColumnFlag(), vObject.getPartitionColumnName(), vObject.getBaseTableFlag(),
				vObject.getFeedTableStatusNt(), vObject.getFeedTableStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}


	@Override
	protected int doUpdateAppr(ETLFeedTablesVb vObject) {
		String query = "Update ETL_FEED_TABLES Set SOURCE_CONNECTOR_ID = ?, TABLE_NAME = ?, TABLE_ALIAS_NAME = ?, "
				+ "TABLE_SORT_ORDER = ?, TABLE_TYPE_AT = ?, TABLE_TYPE = ?, QUERY_ID = ?, CUSTOM_PARTITION_COLUMN_FLAG = ?, PARTITION_COLUMN_NAME = ?, "
				+ "BASE_TABLE_FLAG = ?,  FEED_TABLE_STATUS_NT = ?, FEED_TABLE_STATUS = ?, RECORD_INDICATOR_NT = ?, "
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? "
				+ " AND TABLE_ID = ? ";
		Object[] args = { vObject.getSourceConnectorId(), vObject.getTableName(), vObject.getTableAliasName(),
				vObject.getTableSortOrder(), vObject.getTableTypeAt(), vObject.getTableType(), vObject.getQueryId(),
				vObject.getCustomPartitionColumnFlag(), vObject.getPartitionColumnName(), vObject.getBaseTableFlag(),
				vObject.getFeedTableStatusNt(), vObject.getFeedTableStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(), };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(ETLFeedTablesVb vObject) {
		String query = "Update ETL_FEED_TABLES_PEND Set SOURCE_CONNECTOR_ID = ?, TABLE_NAME = ?, TABLE_ALIAS_NAME = ?, "
				+ "TABLE_SORT_ORDER = ?, TABLE_TYPE_AT = ?, TABLE_TYPE = ?, QUERY_ID = ?, CUSTOM_PARTITION_COLUMN_FLAG = ?, PARTITION_COLUMN_NAME = ?, "
				+ "BASE_TABLE_FLAG = ?, FEED_TABLE_STATUS_NT = ?, FEED_TABLE_STATUS = ?, RECORD_INDICATOR_NT = ?, "
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? "
				+ " AND TABLE_ID = ? ";
		Object[] args = { vObject.getSourceConnectorId(), vObject.getTableName(), vObject.getTableAliasName(),
				vObject.getTableSortOrder(), vObject.getTableTypeAt(), vObject.getTableType(), vObject.getQueryId(),
				vObject.getCustomPartitionColumnFlag(), vObject.getPartitionColumnName(), vObject.getBaseTableFlag(),
				vObject.getFeedTableStatusNt(), vObject.getFeedTableStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getTableId(), };

		return getJdbcTemplate().update(query, args);
	}


	@Override
	protected int doDeleteAppr(ETLFeedTablesVb vObject){
		String query = "Delete From ETL_FEED_TABLES Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deletePendingRecord(ETLFeedTablesVb vObject){
		String query = "Delete From ETL_FEED_TABLES_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deleteUplRecord(ETLFeedTablesVb vObject){
		String query = "Delete From ETL_FEED_TABLES_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_TABLES";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TABLES_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TABLES_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FEED_TABLES_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
	
	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus){
		String table = "ETL_FEED_TABLES";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TABLES_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TABLES_PEND";
		}
		String query = "update "+table+" set FEED_TABLE_STATUS = ? ,RECORD_INDICATOR =? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus,vObject.getRecordIndicator(),vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected String getAuditString(ETLFeedTablesVb vObject){
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

			strAudit.append("TABLE_ID"+auditDelimiterColVal+vObject.getTableId());
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getSourceConnectorId()))
				strAudit.append("SOURCE_CONNECTOR_ID"+auditDelimiterColVal+vObject.getSourceConnectorId().trim());
			else
				strAudit.append("SOURCE_CONNECTOR_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getTableName()))
				strAudit.append("TABLE_NAME"+auditDelimiterColVal+vObject.getTableName().trim());
			else
				strAudit.append("TABLE_NAME"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getTableAliasName()))
				strAudit.append("TABLE_ALIAS_NAME"+auditDelimiterColVal+vObject.getTableAliasName().trim());
			else
				strAudit.append("TABLE_ALIAS_NAME"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("TABLE_SORT_ORDER"+auditDelimiterColVal+vObject.getTableSortOrder());
			strAudit.append(auditDelimiter);

				strAudit.append("TABLE_TYPE_AT"+auditDelimiterColVal+vObject.getTableTypeAt());
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getTableType()))
				strAudit.append("TABLE_TYPE"+auditDelimiterColVal+vObject.getTableType().trim());
			else
				strAudit.append("TABLE_TYPE"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getQueryId()))
				strAudit.append("QUERY_ID"+auditDelimiterColVal+vObject.getQueryId().trim());
			else
				strAudit.append("QUERY_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getCustomPartitionColumnFlag()))
				strAudit.append("CUSTOM_PARTITION_COLUMN_FLAG"+auditDelimiterColVal+vObject.getCustomPartitionColumnFlag().trim());
			else
				strAudit.append("CUSTOM_PARTITION_COLUMN_FLAG"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getPartitionColumnName()))
				strAudit.append("PARTITION_COLUMN_NAME"+auditDelimiterColVal+vObject.getPartitionColumnName().trim());
			else
				strAudit.append("PARTITION_COLUMN_NAME"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getBaseTableFlag()))
				strAudit.append("BASE_TABLE_FLAG"+auditDelimiterColVal+vObject.getBaseTableFlag().trim());
			else
				strAudit.append("BASE_TABLE_FLAG"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			strAudit.append("FEED_TABLE_STATUS_NT"+auditDelimiterColVal+vObject.getFeedTableStatusNt());
			strAudit.append(auditDelimiter);

			strAudit.append("FEED_TABLE_STATUS"+auditDelimiterColVal+vObject.getFeedTableStatus());
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
		serviceName = "EtlFeedTables";
		serviceDesc = "ETL Feed Tables";
		tableName = "ETL_FEED_TABLES";
		childTableName = "ETL_FEED_TABLES";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		
	}
	public List<ETLFeedTablesVb> getQueryDetailResults(ETLFeedTablesVb dObj){
		Vector<Object> params = new Vector<Object>();
		setServiceDefaults();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY, "+CountryApprDesc
				+ ",TAppr.LE_BOOK, "+LeBookApprDesc
				+ ",TAppr.FEED_ID"
				+ ",TAppr.TABLE_ID"
				+ ",TAppr.SOURCE_CONNECTOR_ID"
				+ ",TAppr.TABLE_NAME"
				+ ",TAppr.TABLE_ALIAS_NAME"
				+ ",TAppr.TABLE_SORT_ORDER"
				+ ",TAppr.TABLE_TYPE_AT"
				+ ",TAppr.TABLE_TYPE, "+TableTypeAtApprDesc
				+ ",TAppr.QUERY_ID"
				+ ",TAppr.CUSTOM_PARTITION_COLUMN_FLAG"
				+ ",TAppr.PARTITION_COLUMN_NAME"
				+ ",TAppr.BASE_TABLE_FLAG"
				+ ",TAppr.FEED_TABLE_STATUS_NT"
				+ ",TAppr.FEED_TABLE_STATUS, "+FeedTableStatusNtApprDesc
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR, "+RecordIndicatorNtApprDesc
				+ ",TAppr.MAKER, "+makerApprDesc
				+ ",TAppr.VERIFIER, "+verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION, 'APPROVE' DATA_FROM "
				+" From ETL_FEED_TABLES TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FEED_TABLES_PEND TPend "
				+ "WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.TABLE_ID = TPend.TABLE_ID )");
		
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY, "+CountryPendDesc
				+ ",TPend.LE_BOOK, "+LeBookTPendDesc
				+ ",TPend.FEED_ID"
				+ ",TPend.TABLE_ID"
				+ ",TPend.SOURCE_CONNECTOR_ID"
				+ ",TPend.TABLE_NAME"
				+ ",TPend.TABLE_ALIAS_NAME"
				+ ",TPend.TABLE_SORT_ORDER"
				+ ",TPend.TABLE_TYPE_AT"
				+ ",TPend.TABLE_TYPE, "+TableTypeAtPendDesc
				+ ",TPend.QUERY_ID"
				+ ",TPend.CUSTOM_PARTITION_COLUMN_FLAG"
				+ ",TPend.PARTITION_COLUMN_NAME"
				+ ",TPend.BASE_TABLE_FLAG"
				+ ",TPend.FEED_TABLE_STATUS_NT"
				+ ",TPend.FEED_TABLE_STATUS, "+FeedTableStatusNtPendDesc
				//+ ",TPend.FEED_TABLE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR, "+RecordIndicatorNtPendDesc
				+ ",TPend.MAKER, "+makerPendDesc
				+ ",TPend.VERIFIER, "+verifierPendDesc
				+ ",TPend.INTERNAL_STATUS"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION, 'PEND' DATA_FROM  "
				+" From ETL_FEED_TABLES_PEND TPend ");

		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_TABLES_UPL TUpl "
				+ "WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.TABLE_ID = TUpl.TABLE_ID) ");
		
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_TABLES_UPL TUpl "
				+ "WHERE TPend.COUNTRY = TUpl.COUNTRY AND TPend.LE_BOOK = TUpl.LE_BOOK AND TPend.FEED_ID = TUpl.FEED_ID AND TPend.TABLE_ID = TUpl.TABLE_ID) ");
		
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY, "+CountryPendDesc.replaceAll("TPend.", "TUpl.")
		+ ",TUpl.LE_BOOK, "+LeBookTPendDesc.replaceAll("TPend.", "TUpl.")
				+ ",TUpl.FEED_ID"
				+ ",TUpl.TABLE_ID"
				+ ",TUpl.SOURCE_CONNECTOR_ID"
				+ ",TUpl.TABLE_NAME"
				+ ",TUpl.TABLE_ALIAS_NAME"
				+ ",TUpl.TABLE_SORT_ORDER"
				+ ",TUpl.TABLE_TYPE_AT"
				+ ",TUpl.TABLE_TYPE, "+TableTypeAtPendDesc.replaceAll("TPend.", "TUpl.")
				+ ",TUpl.QUERY_ID"
				+ ",TUpl.CUSTOM_PARTITION_COLUMN_FLAG"
				+ ",TUpl.PARTITION_COLUMN_NAME"
				+ ",TUpl.BASE_TABLE_FLAG"
				+ ",TUpl.FEED_TABLE_STATUS_NT"
				+ ",TUpl.FEED_TABLE_STATUS, "+FeedTableStatusNtPendDesc.replaceAll("TPend.", "TUpl.")
				//+ ",TUpl.FEED_TABLE_STATUS"
				+ ",TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR, "+RecordIndicatorNtPendDesc.replaceAll("TPend.", "TUpl.")
				+ ",TUpl.MAKER, "+makerUplDesc
				+ ",TUpl.VERIFIER, "+verifierUplDesc
				+ ",TUpl.INTERNAL_STATUS"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TUpl.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION, 'UPL' DATA_FROM  "
				+" From ETL_FEED_TABLES_UPL TUpl ");
		try
			{
				if (ValidationUtil.isValid(dObj.getCountry())){
					params.addElement(dObj.getCountry().toUpperCase());
					CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
					CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
					CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getLeBook())){
						params.addElement(dObj.getLeBook().toUpperCase());
						CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedId())){
						params.addElement(dObj.getFeedId().toUpperCase());
						CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
				}
/*				if (ValidationUtil.isValid(dObj.getFeedName())){
						params.addElement("%" + dObj.getFeedName().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.FEED_NAME) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FEED_NAME) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FEED_NAME) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedDescription())){
						params.addElement("%" + dObj.getFeedDescription().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.FEED_DESCRIPTION) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FEED_DESCRIPTION) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FEED_DESCRIPTION) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getEffectiveDate())){
						params.addElement("%" + dObj.getEffectiveDate().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.EFFECTIVE_DATE) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.EFFECTIVE_DATE) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.EFFECTIVE_DATE) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getLastExtractionDate())){
						params.addElement("%" + dObj.getLastExtractionDate().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.LAST_EXTRACTION_DATE) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.LAST_EXTRACTION_DATE) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.LAST_EXTRACTION_DATE) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedType())){
						params.addElement("%" + dObj.getFeedType().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.FEED_TYPE) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FEED_TYPE) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FEED_TYPE) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedCategory())){
						params.addElement("%" + dObj.getFeedCategory().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.FEED_CATEGORY) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.FEED_CATEGORY) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.FEED_CATEGORY) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getAlertMechanism())){
						params.addElement("%" + dObj.getAlertMechanism().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.ALERT_MECHANISM) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.ALERT_MECHANISM) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.ALERT_MECHANISM) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getPrivilageMechanism())){
						params.addElement("%" + dObj.getPrivilageMechanism().toUpperCase() + "%");
						CommonUtils.addToQuery("UPPER(TAppr.PRIVILAGE_MECHANISM) LIKE ?", strBufApprove);
						CommonUtils.addToQuery("UPPER(TPend.PRIVILAGE_MECHANISM) LIKE ?", strBufPending);
						CommonUtils.addToQuery("UPPER(TUpl.PRIVILAGE_MECHANISM) LIKE ?", strBufUpl);
				}
				if (ValidationUtil.isValid(dObj.getFeedStatus())){
						params.addElement("" + dObj.getFeedStatus() + "");
						CommonUtils.addToQuery("TAppr.FEED_STATUS = ?", strBufApprove);
						CommonUtils.addToQuery("TPend.FEED_STATUS = ?", strBufPending);
						CommonUtils.addToQuery("TUpl.FEED_STATUS = ?", strBufUpl);
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
			String orderBy = " Order By COUNTRY , LE_BOOK , FEED_ID ";
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

	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_FEED_TABLES_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_TABLES_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID = ETL_FEED_TABLES_PRS.SESSION_ID"
				+ ") ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
}
