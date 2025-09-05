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
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedTransformationVb;

@Component
public class EtlFeedTransformationDao extends AbstractDao<EtlFeedTransformationVb> {

	/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String FeedTransformStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_TRANSFORM_STATUS", "FEED_TRANSFORM_STATUS_DESC");
	String FeedTransformStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_TRANSFORM_STATUS", "FEED_TRANSFORM_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	
	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedTransformationVb vObject = new EtlFeedTransformationVb();
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
				if(rs.getString("NODE_ID")!= null){ 
					vObject.setNodeId(rs.getString("NODE_ID"));
				}else{
					vObject.setNodeId("");
				}
				if(rs.getString("NODE_DESCRIPTION")!= null){ 
					vObject.setNodeDesc(rs.getString("NODE_DESCRIPTION"));
				}else{
					vObject.setNodeDesc("");
				}
				if(rs.getString("NODE_NAME")!= null){ 
					vObject.setNodeName(rs.getString("NODE_NAME"));
				}else{
					vObject.setNodeName("");
				}
				if(rs.getString("TRANSFORMATION_ID")!= null){ 
					vObject.setTransformationId(rs.getString("TRANSFORMATION_ID"));
				}else{
					vObject.setTransformationId("");
				}
				if(rs.getString("X_AXIS")!= null){ 
					vObject.setxAxis(rs.getString("X_AXIS"));
				}else{
					vObject.setxAxis("");
				}
				if(rs.getString("Y_AXIS")!= null){ 
					vObject.setyAxis(rs.getString("Y_AXIS"));
				}else{
					vObject.setyAxis("");
				}
				vObject.setTransformationStatusNt(rs.getInt("TRANSFORMATION_STATUS_NT"));
				vObject.setTransformationStatus(rs.getInt("TRANSFORMATION_STATUS"));
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
				vObject.setMinimizeFlag(rs.getString("MINIMIZE_FLAG"));
				vObject.setJoinType(ValidationUtil.isValid(rs.getString("JOIN_TYPE"))?rs.getString("JOIN_TYPE"):"");
				vObject.setGroupId(ValidationUtil.isValid(rs.getString("GROUP_ID"))?rs.getString("GROUP_ID"):"");
				vObject.setCustomFlag(ValidationUtil.isValid(rs.getString("CUSTOM_FLAG"))?rs.getString("CUSTOM_FLAG"):"");
				vObject.setCustomExpression(ValidationUtil.isValid(rs.getString("CUSTOM_EXPRESSION"))?rs.getString("CUSTOM_EXPRESSION"):"");
				return vObject;
			}
		};
		return mapper;
	}

	protected RowMapper getParentNodeMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedTransformationVb vObject = new EtlFeedTransformationVb();
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
				if(rs.getString("NODE_ID")!= null){ 
					vObject.setNodeId(rs.getString("NODE_ID"));
				}else{
					vObject.setNodeId("");
				}
				if(rs.getString("PARENT_NODE_ID")!= null){ 
					vObject.setParentNodeId(rs.getString("PARENT_NODE_ID"));
				}else{
					vObject.setParentNodeId("");
				}
				vObject.setTransformationStatusNt(rs.getInt("PARENT_STATUS_NT"));
				vObject.setTransformationStatus(rs.getInt("PARENT_STATUS"));
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

	protected RowMapper getChildNodeMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedTransformationVb vObject = new EtlFeedTransformationVb();
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
				if(rs.getString("NODE_ID")!= null){ 
					vObject.setNodeId(rs.getString("NODE_ID"));
				}else{
					vObject.setNodeId("");
				}
				if(rs.getString("CHILD_NODE_ID")!= null){ 
					vObject.setChildNodeId(rs.getString("CHILD_NODE_ID"));
				}else{
					vObject.setChildNodeId("");
				}
				vObject.setTransformationStatusNt(rs.getInt("CHILD_STATUS_NT"));
				vObject.setTransformationStatus(rs.getInt("CHILD_STATUS"));
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
	public List<EtlFeedTransformationVb> getQueryPopupResults(EtlFeedTransformationVb dObj){

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.NODE_ID"
			+ ",TAppr.NODE_NAME"
			+ ",TAppr.NODE_DESCRIPTION"
			+ ",TAppr.X_AXIS"
			+ ",TAppr.Y_AXIS"
			+ ",TAppr.TRANSFORMATION_ID"
			+ ",TAppr.TRANSFORMATION_STATUS_NT"
			+ ",TAppr.TRANSFORMATION_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TAppr.MINIMIZE_FLAG,  TAppr.JOIN_TYPE, TAppr.GROUP_ID, TAppr.CUSTOM_FLAG "
			+ ",TAPPR.CUSTOM_EXPRESSION  from ETL_FEED_TRANSFORMATION TAppr ");
		String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FEED_TRANSFORMATION_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.NODE_ID = TPend.NODE_ID)");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.NODE_ID"
			+ ",TPend.NODE_NAME"
			+ ",TPend.NODE_DESCRIPTION"
			+ ",TPend.X_AXIS"
			+ ",TPend.Y_AXIS"
			+ ",TPend.TRANSFORMATION_ID"
			+ ",TPend.TRANSFORMATION_STATUS_NT"
			+ ",TPend.TRANSFORMATION_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TPend.MINIMIZE_FLAG, TPend.JOIN_TYPE, TPend.GROUP_ID, TPend.CUSTOM_FLAG "
			+ ",TPend.CUSTOM_EXPRESSION  from ETL_FEED_TRANSFORMATION_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.NODE_ID"
			+ ",TUpl.NODE_NAME"
			+ ",TUpl.NODE_DESCRIPTION"
			+ ",TUpl.X_AXIS"
			+ ",TUpl.Y_AXIS"
			+ ",TUpl.TRANSFORMATION_ID"
			+ ",TUpl.TRANSFORMATION_STATUS_NT"
			+ ",TUpl.TRANSFORMATION_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TUpl.MINIMIZE_FLAG, TUpl.JOIN_TYPE, TUpl.GROUP_ID, TUpl.CUSTOM_FLAG  "
			+ ",TUpl.CUSTOM_EXPRESSION  from ETL_FEED_TRANSFORMATION_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_TRANSFORMATION_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.NODE_ID = TUpl.NODE_ID) ");
		String strWhereNotExistsPendInUpl = new String( " Not Exists (Select 'X' From ETL_FEED_TRANSFORMATION_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID AND TUpl.NODE_ID = TPend.NODE_ID) ");
		try
		{
			if (ValidationUtil.isValid(dObj.getCountry())) {
				params.addElement(dObj.getCountry().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getLeBook())) {
				params.addElement(dObj.getLeBook().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getFeedId())) {
				params.addElement(dObj.getFeedId().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getNodeId())) {
				params.addElement(dObj.getNodeId().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.NODE_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.NODE_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.NODE_ID) = ?", strBufUpl);
			}
			String orderBy = " Order By COUNTRY, LE_BOOK, FEED_ID, NODE_ID ";
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

	public List<EtlFeedTransformationVb> getQueryResults(EtlFeedTransformationVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedTransformationVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.NODE_ID"
			+ ",TAppr.NODE_NAME"
			+ ",TAppr.NODE_DESCRIPTION"
			+ ",TAppr.X_AXIS"
			+ ",TAppr.Y_AXIS"
			+ ",TAppr.TRANSFORMATION_ID"
			+ ",TAppr.TRANSFORMATION_STATUS_NT"
			+ ",TAppr.TRANSFORMATION_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TAppr.MINIMIZE_FLAG, TAppr.JOIN_TYPE, TAppr.GROUP_ID, TAppr.CUSTOM_FLAG "
			+ ",TAPPR.CUSTOM_EXPRESSION "
			+ " From ETL_FEED_TRANSFORMATION TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.NODE_ID"
			+ ",TPend.NODE_NAME"
			+ ",TPend.NODE_DESCRIPTION"
			+ ",TPend.X_AXIS"
			+ ",TPend.Y_AXIS"
			+ ",TPend.TRANSFORMATION_ID"
			+ ",TPend.TRANSFORMATION_STATUS_NT"
			+ ",TPend.TRANSFORMATION_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TPend.MINIMIZE_FLAG, TPend.JOIN_TYPE, TPend.GROUP_ID, TPend.CUSTOM_FLAG "
			+ ",TPend.CUSTOM_EXPRESSION "
			+ " From ETL_FEED_TRANSFORMATION_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.NODE_ID"
			+ ",TUpl.NODE_NAME"
			+ ",TUpl.NODE_DESCRIPTION"
			+ ",TUpl.X_AXIS"
			+ ",TUpl.Y_AXIS"
			+ ",TUpl.TRANSFORMATION_ID"
			+ ",TUpl.TRANSFORMATION_STATUS_NT"
			+ ",TUpl.TRANSFORMATION_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TUpl.MINIMIZE_FLAG, TUpl.JOIN_TYPE, TUpl.GROUP_ID, TUpl.CUSTOM_FLAG  "
			+ ",TUpl.CUSTOM_EXPRESSION "
			+ " From ETL_FEED_TRANSFORMATION_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");

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
	
	public List<EtlFeedTransformationVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedTransformationVb> collTemp = null;
		 int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.NODE_ID"
			+ ",TAppr.NODE_NAME"
			+ ",TAppr.NODE_DESCRIPTION"
			+ ",TAppr.X_AXIS"
			+ ",TAppr.Y_AXIS"
			+ ",TAppr.TRANSFORMATION_ID"
			+ ",TAppr.TRANSFORMATION_STATUS_NT"
			+ ",TAppr.TRANSFORMATION_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TAppr.MINIMIZE_FLAG, TAppr.JOIN_TYPE, TAppr.GROUP_ID, TAppr.CUSTOM_FLAG  "
			+ ",TAPPR.CUSTOM_EXPRESSION "
			+ " From ETL_FEED_TRANSFORMATION TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?    ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.NODE_ID"
			+ ",TPend.NODE_NAME"
			+ ",TPend.NODE_DESCRIPTION"
			+ ",TPend.X_AXIS"
			+ ",TPend.Y_AXIS"
			+ ",TPend.TRANSFORMATION_ID"
			+ ",TPend.TRANSFORMATION_STATUS_NT"
			+ ",TPend.TRANSFORMATION_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TPend.MINIMIZE_FLAG, TPend.JOIN_TYPE, TPend.GROUP_ID, TPend.CUSTOM_FLAG  "
			+ ",TPend.CUSTOM_EXPRESSION "
			+ " From ETL_FEED_TRANSFORMATION_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.NODE_ID"
			+ ",TUpl.NODE_NAME"
			+ ",TUpl.NODE_DESCRIPTION"
			+ ",TUpl.X_AXIS"
			+ ",TUpl.Y_AXIS"
			+ ",TUpl.TRANSFORMATION_ID"
			+ ",TUpl.TRANSFORMATION_STATUS_NT"
			+ ",TUpl.TRANSFORMATION_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null)
			+ "') DATE_CREATION, TUpl.MINIMIZE_FLAG, TUpl.JOIN_TYPE, TUpl.GROUP_ID, TUpl.CUSTOM_FLAG  "
			+ ",TUpl.CUSTOM_EXPRESSION "
			+ " From ETL_FEED_TRANSFORMATION_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		if (ValidationUtil.isValid(dObj.getNodeId())) {
			intKeyFieldsCount = 4;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getNodeId();

			strQueryUpl = strQueryUpl + "  AND  NODE_ID =?  ";
			strQueryAppr = strQueryAppr + " AND  NODE_ID =?  ";
			strQueryPend = strQueryPend + " AND NODE_ID =?";
		}

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
	public List<EtlFeedTransformationVb> getQueryResultsByParentNode(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedTransformationVb> collTemp = null;
		 int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.NODE_ID"
			+ ",TAppr.PARENT_NODE_ID"
			+ ",TAppr.PARENT_STATUS_NT"
			+ ",TAppr.PARENT_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_Feed_Tran_Parent TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.NODE_ID"
			+ ",TPend.PARENT_NODE_ID"
			+ ",TPend.PARENT_STATUS_NT"
			+ ",TPend.PARENT_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_Feed_Tran_Parent_pend TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.NODE_ID"
			+ ",TUpl.PARENT_NODE_ID"
			+ ",TUpl.PARENT_STATUS_NT"
			+ ",TUpl.PARENT_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_Feed_Tran_Parent_upl TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		if (ValidationUtil.isValid(dObj.getNodeId())) {
			intKeyFieldsCount = 4;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getNodeId();

			strQueryUpl = strQueryUpl + "  AND  NODE_ID =?  ";
			strQueryAppr = strQueryAppr + " AND  NODE_ID =?  ";
			strQueryPend = strQueryPend + " AND NODE_ID =?";
		}

		try {
			// if(!dObj.isVerificationRequired()){intStatus =0;}
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getParentNodeMapper());
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getParentNodeMapper());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getParentNodeMapper());
			}
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	public List<EtlFeedTransformationVb> getQueryResultsByChildNode(EtlFeedMainVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedTransformationVb> collTemp = null;
		 int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.NODE_ID"
			+ ",TAppr.CHILD_NODE_ID"
			+ ",TAppr.CHILD_STATUS_NT"
			+ ",TAppr.CHILD_STATUS"
			+ ",TAppr.RECORD_INDICATOR_NT"
			+ ",TAppr.RECORD_INDICATOR"
			+ ",TAppr.MAKER"
			+ ",TAppr.VERIFIER"
			+ ",TAppr.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_Feed_Tran_Child TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ");
		
		String strQueryPend = new String("Select TPend.COUNTRY"
			+ ",TPend.LE_BOOK"
			+ ",TPend.FEED_ID"
			+ ",TPend.NODE_ID"
			+ ",TPend.CHILD_NODE_ID"
			+ ",TPend.CHILD_STATUS_NT"
			+ ",TPend.CHILD_STATUS"
			+ ",TPend.RECORD_INDICATOR_NT"
			+ ",TPend.RECORD_INDICATOR"
			+ ",TPend.MAKER"
			+ ",TPend.VERIFIER"
			+ ",TPend.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_Feed_Tran_Child_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ");
		
		String strQueryUpl = new String("Select TUpl.COUNTRY"
			+ ",TUpl.LE_BOOK"
			+ ",TUpl.FEED_ID"
			+ ",TUpl.NODE_ID"
			+ ",TUpl.CHILD_NODE_ID"
			+ ",TUpl.CHILD_STATUS_NT"
			+ ",TUpl.CHILD_STATUS"
			+ ",TUpl.RECORD_INDICATOR_NT"
			+ ",TUpl.RECORD_INDICATOR"
			+ ",TUpl.MAKER"
			+ ",TUpl.VERIFIER"
			+ ",TUpl.INTERNAL_STATUS"
			+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
			+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
			+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
			+ " From ETL_Feed_Tran_Child_UPL TUpl WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		if (ValidationUtil.isValid(dObj.getNodeId())) {
			intKeyFieldsCount = 4;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getNodeId();

			strQueryUpl = strQueryUpl + "  AND  NODE_ID =?  ";
			strQueryAppr = strQueryAppr + " AND  NODE_ID =?  ";
			strQueryPend = strQueryPend + " AND NODE_ID =?";
		}

		try {
			// if(!dObj.isVerificationRequired()){intStatus =0;}
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getChildNodeMapper());
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getChildNodeMapper());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getChildNodeMapper());
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
	protected List<EtlFeedTransformationVb> selectApprovedRecord(EtlFeedTransformationVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedTransformationVb> doSelectPendingRecord(EtlFeedTransformationVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlFeedTransformationVb> doSelectUplRecord(EtlFeedTransformationVb vObject){
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlFeedTransformationVb records){return records.getTransformationStatus();}

	@Override
	protected void setStatus(EtlFeedTransformationVb vObject,int status){vObject.setTransformationStatus(status);}


	@Override
	protected int doInsertionAppr(EtlFeedTransformationVb vObject){
		String sql = "Insert Into ETL_FEED_TRANSFORMATION (COUNTRY, LE_BOOK, "
				+ "FEED_ID, NODE_ID, NODE_NAME, TRANSFORMATION_ID, X_AXIS, Y_AXIS, "
				+ "TRANSFORMATION_STATUS_NT, TRANSFORMATION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MINIMIZE_FLAG, JOIN_TYPE, GROUP_ID, CUSTOM_FLAG, "
				+ " NODE_DESCRIPTION, CUSTOM_EXPRESSION ) Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?,?,?,"
				+ getDbFunction(Constants.SYSDATE, null) + ","
				+ getDbFunction(Constants.SYSDATE, null) + ", ?,?,?,?,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getNodeName(), vObject.getTransformationId(), vObject.getxAxis(), vObject.getyAxis(),
				vObject.getTransformationStatusNt(), vObject.getTransformationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getMinimizeFlag(), vObject.getJoinType(), vObject.getGroupId(), vObject.getCustomFlag() };
		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = con.prepareStatement(sql);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getNodeDesc()) ? vObject.getNodeDesc() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					clobData = ValidationUtil.isValid(vObject.getCustomExpression()) ? vObject.getCustomExpression() : "";
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
	protected int doInsertionPend(EtlFeedTransformationVb vObject){
		String sql = "Insert Into ETL_FEED_TRANSFORMATION_PEND (COUNTRY, LE_BOOK, "
				+ "FEED_ID, NODE_ID, NODE_NAME, TRANSFORMATION_ID, X_AXIS, Y_AXIS, "
				+ "TRANSFORMATION_STATUS_NT, TRANSFORMATION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MINIMIZE_FLAG, JOIN_TYPE, GROUP_ID, "
				+ " CUSTOM_FLAG, NODE_DESCRIPTION, CUSTOM_EXPRESSION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?,?,?," + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ", ?,?,?,?,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getNodeName(), vObject.getTransformationId(), vObject.getxAxis(), vObject.getyAxis(),
				vObject.getTransformationStatusNt(), vObject.getTransformationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getMinimizeFlag(), vObject.getJoinType(), vObject.getGroupId(), vObject.getCustomFlag() };
		int result=0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = con.prepareStatement(sql);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getNodeDesc()) ? vObject.getNodeDesc() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					clobData = ValidationUtil.isValid(vObject.getCustomExpression()) ? vObject.getCustomExpression() : "";
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

	protected int doInsertionUpl(EtlFeedTransformationVb vObject) {
		String sql = "Insert Into ETL_FEED_TRANSFORMATION_UPL (COUNTRY, LE_BOOK, "
				+ "FEED_ID, NODE_ID, NODE_NAME, TRANSFORMATION_ID, X_AXIS, Y_AXIS, "
				+ "TRANSFORMATION_STATUS_NT, TRANSFORMATION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, MINIMIZE_FLAG, JOIN_TYPE, GROUP_ID, "
				+ " CUSTOM_FLAG, NODE_DESCRIPTION, CUSTOM_EXPRESSION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ", ?,?,?,?,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getNodeName(), vObject.getTransformationId(), vObject.getxAxis(), vObject.getyAxis(),
				vObject.getTransformationStatusNt(), vObject.getTransformationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getMinimizeFlag(), vObject.getJoinType(), vObject.getGroupId(), vObject.getCustomFlag() };
		int result=0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = con.prepareStatement(sql);
					for (int i = 1; i <= argumentLength; i++) {
						ps.setObject(i, args[i - 1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getNodeDesc()) ? vObject.getNodeDesc() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());

					clobData = ValidationUtil.isValid(vObject.getCustomExpression()) ? vObject.getCustomExpression() : "";
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
	
	protected int doInsertionParentUpl(EtlFeedTransformationVb vObject){
		String query = "Insert Into ETL_Feed_Tran_Parent_UPL (COUNTRY, LE_BOOK, FEED_ID, NODE_ID, PARENT_NODE_ID, "
				+ "PARENT_STATUS_NT, PARENT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
				+ getDbFunction(Constants.SYSDATE, null) + ","
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getParentNodeId(), vObject.getTransformationStatusNt(), vObject.getTransformationStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };
		int result =0;
		try{
			result =getJdbcTemplate().update(query, args);
		}catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}
	protected int doInsertionParentPend(EtlFeedTransformationVb vObject){
		String query = "Insert Into ETL_Feed_Tran_Parent_PEND (COUNTRY, LE_BOOK, "
				+ "FEED_ID, NODE_ID, PARENT_NODE_ID, "
				+ "PARENT_STATUS_NT, PARENT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getParentNodeId(), vObject.getTransformationStatusNt(), vObject.getTransformationStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };
		int result =0;
		try{
			result =getJdbcTemplate().update(query, args);
		}catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}	

	protected int doInsertionParent(EtlFeedTransformationVb vObject) {
		String query = "Insert Into ETL_Feed_Tran_Parent (COUNTRY, LE_BOOK, FEED_ID, NODE_ID, PARENT_NODE_ID, "
				+ "PARENT_STATUS_NT, PARENT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getParentNodeId(), vObject.getTransformationStatusNt(), vObject.getTransformationStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };
		int result = 0;
		try {
			result = getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	protected int doInsertionChild(EtlFeedTransformationVb vObject) {
		String query = "Insert Into ETL_Feed_Tran_Child (COUNTRY, LE_BOOK, FEED_ID, NODE_ID, child_NODE_ID, "
				+ "CHILD_STATUS_NT, CHILD_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getChildNodeId(), vObject.getTransformationStatusNt(), vObject.getTransformationStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };
		int result = 0;
		try {
			result = getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}

	protected int doInsertionChildPend(EtlFeedTransformationVb vObject) {
		String query = "Insert Into ETL_Feed_Tran_Child_PEND (COUNTRY, LE_BOOK, FEED_ID, NODE_ID, child_NODE_ID, "
				+ "CHILD_STATUS_NT, CHILD_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getChildNodeId(), vObject.getTransformationStatusNt(), vObject.getTransformationStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };
		int result = 0;
		try {
			result = getJdbcTemplate().update(query, args);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}
	protected int doInsertionChildUpl(EtlFeedTransformationVb vObject){
		String query = "Insert Into ETL_Feed_Tran_Child_UPL (COUNTRY, LE_BOOK, FEED_ID, NODE_ID, child_NODE_ID, "
				+ "CHILD_STATUS_NT, CHILD_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION) Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,"
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getChildNodeId(), vObject.getTransformationStatusNt(), vObject.getTransformationStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };
		int result = 0;
		try{
			result =getJdbcTemplate().update(query, args);
		}catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
		}
		return result;
	}
	
	@Override
	protected int doInsertionPendWithDc(EtlFeedTransformationVb vObject){
		String query = "Insert Into ETL_FEED_TRANSFORMATION_pend (COUNTRY, LE_BOOK, "
				+ "FEED_ID, NODE_ID, NODE_NAME, TRANSFORMATION_ID, "
				+ "X_AXIS, Y_AXIS, TRANSFORMATION_STATUS_NT, TRANSFORMATION_STATUS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, "
				+ "DATE_CREATION, MINIMIZE_FLAG, JOIN_TYPE, GROUP_ID, CUSTOM_FLAG, NODE_DESCRIPTION, CUSTOM_EXPRESSION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + getDbFunction(Constants.SYSDATE, null)
				+ "," + getDbFunction(Constants.SYSDATE, null) + ", ?,?,?,?,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getNodeName(), vObject.getTransformationId(), vObject.getxAxis(), vObject.getyAxis(),
				vObject.getTransformationStatusNt(), vObject.getTransformationStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getMinimizeFlag(), vObject.getJoinType(), vObject.getGroupId(), vObject.getCustomFlag() };
		int result = 0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					int argumentLength = args.length;
					PreparedStatement ps = connection.prepareStatement(query);
					for(int i=1;i<=argumentLength;i++){
						ps.setObject(i,args[i-1]);
					}
					String clobData = ValidationUtil.isValid(vObject.getNodeDesc())?vObject.getNodeDesc():"";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getCustomExpression()) ? vObject.getCustomExpression() : "";
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
	protected int doUpdateAppr(EtlFeedTransformationVb vObject){
		String query = "Update ETL_FEED_TRANSFORMATION Set NODE_DESCRIPTION = ?, "
				+ "TRANSFORMATION_STATUS_NT = ?, TRANSFORMATION_STATUS = ?, NODE_NAME = ?, TRANSFORMATION_ID = ?, "
				+ "TRANSFORMATION_STATUS_NT = ?, TRANSFORMATION_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?"
				+ " VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND NODE_ID = ?";
		Object[] args = { vObject.getNodeName(), vObject.getTransformationId(), vObject.getTransformationStatusNt(),
				vObject.getTransformationStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId() };
		int result = 0;
		try{
			
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getNodeDesc())?vObject.getNodeDesc():"";
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
	protected int doUpdatePend(EtlFeedTransformationVb vObject){
		String query = "Update ETL_FEED_TRANSFORMATION_PEND Set NODE_DESCRIPTION = ?, "
				+ "TRANSFORMATION_STATUS_NT = ?, TRANSFORMATION_STATUS = ?, NODE_NAME = ?, TRANSFORMATION_ID = ?, "
				+ "TRANSFORMATION_STATUS_NT = ?, TRANSFORMATION_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?"
				+ " VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND NODE_ID = ?";
		Object[] args = { vObject.getNodeName(), vObject.getTransformationId(), vObject.getTransformationStatusNt(),
				vObject.getTransformationStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId() };
		int result = 0;
			try{
				return getJdbcTemplate().update(new PreparedStatementCreator() {
					@Override
					public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
						PreparedStatement ps = connection.prepareStatement(query);
						int psIndex = 0;
						String clobData = ValidationUtil.isValid(vObject.getNodeDesc())?vObject.getNodeDesc():"";
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
	protected int doDeleteAppr(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_FEED_TRANSFORMATION Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_FEED_TRANSFORMATION_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()  };
		return getJdbcTemplate().update(query,args);
	}
	protected int deletePendingRecordParentNode(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_Feed_Tran_Parent_pend Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()  };
		return getJdbcTemplate().update(query,args);
	}
	protected int deletePendingRecordChildNode(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_Feed_Tran_Child_pend Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()  };
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected int deleteUplRecord(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_FEED_TRANSFORMATION_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()};
		return getJdbcTemplate().update(query,args);
	}
	protected int deleteNodeParentRecord(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_Feed_Tran_Parent Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	protected int deleteNodeChildRecord(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_Feed_Tran_Child Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	protected int deleteNodeColumnRecord(EtlFeedTransformationVb vObject){
		String query = "Delete From ETL_Feed_Tran_Columns Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByParentNode(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_Feed_Tran_Parent";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Parent_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Parent_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query = query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByChildNode(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_Feed_Tran_Child";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Child_upl";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Child_pend";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_TRANSFORMATION";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TRANSFORMATION_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TRANSFORMATION_PEND";
		}
		String query = "Delete From " + table + " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if (ValidationUtil.isValid(vObject.getNodeId())) {
			query = query + " AND NODE_ID = '" + vObject.getNodeId() + "'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_TRANSFORMATION_PRS";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		if (ValidationUtil.isValid(vObject.getNodeId())) {
			query = query + " AND NODE_ID = '" + vObject.getNodeId() + "'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParentNode(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_Feed_Tran_Parent_prs";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		if (ValidationUtil.isValid(vObject.getNodeId())) {
			query = query + " AND NODE_ID = '" + vObject.getNodeId() + "'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSChildNode(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_Feed_Tran_Child_prs";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		if (ValidationUtil.isValid(vObject.getNodeId())) {
			query = query + " AND NODE_ID = '" + vObject.getNodeId() + "'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int updateStatusByParentNode(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_Feed_Tran_Parent";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Parent_upl";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Parent_pend";
		}
		String query = "update " + table
				+ " set parent_STATUS = ?,RECORD_INDICATOR =?  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int updateStatusByChildNode(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_Feed_Tran_Child";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Child_upl";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Child_pend";
		}
		String query = "update " + table
				+ " set CHILD_STATUS = ?,RECORD_INDICATOR =?  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_FEED_TRANSFORMATION";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TRANSFORMATION_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_TRANSFORMATION_PEND";
		}
		String query = "update " + table
				+ " set TRANSFORMATION_STATUS = ?,RECORD_INDICATOR =?  Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}
	
	@Override
	protected String getAuditString(EtlFeedTransformationVb vObject){
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

			if(ValidationUtil.isValid(vObject.getNodeId()))
				strAudit.append("NODE_ID"+auditDelimiterColVal+vObject.getNodeId().trim());
			else
				strAudit.append("NODE_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getNodeName()))
				strAudit.append("NODE_NAME"+auditDelimiterColVal+vObject.getNodeName().trim());
			else
				strAudit.append("NODE_NAME"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getNodeDesc()))
				strAudit.append("NODE_DESCRIPTION"+auditDelimiterColVal+vObject.getNodeDesc().trim());
			else
				strAudit.append("NODE_DESCRIPTION"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getTransformationId()))
				strAudit.append("TRANSFORMATION_ID"+auditDelimiterColVal+vObject.getTransformationId().trim());
			else
				strAudit.append("TRANSFORMATION_ID"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);
		
			if(ValidationUtil.isValid(vObject.getxAxis()))
				strAudit.append("X_AXIS"+auditDelimiterColVal+vObject.getxAxis().trim());
			else
				strAudit.append("X_AXIS"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

			if(ValidationUtil.isValid(vObject.getxAxis()))
				strAudit.append("Y_AXIS"+auditDelimiterColVal+vObject.getyAxis().trim());
			else
				strAudit.append("Y_AXIS"+auditDelimiterColVal+"NULL");
			strAudit.append(auditDelimiter);

				strAudit.append("TRANSFORMATION_STATUS_NT"+auditDelimiterColVal+vObject.getTransformationStatusNt());
			strAudit.append(auditDelimiter);

				strAudit.append("TRANSFORMATION_STATUS"+auditDelimiterColVal+vObject.getTransformationStatus());
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
		serviceName = "EtlFeedTransformation";
		serviceDesc = "ETL Feed Transformation";
		tableName = "ETL_FEED_TRANSFORMATION";
		childTableName = "ETL_FEED_TRANSFORMATION";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
		
	}

	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_FEED_TRANSFORMATION_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_TRANSFORMATION_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_FEED_TRANSFORMATION_PRS.SESSION_ID ) ";
		Object[] args = {  vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
	
	public int deleteRecordByPRSParentNodeByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_Feed_Tran_Parent_prs WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_Feed_Tran_Parent_prs.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_Feed_Tran_Parent_prs.SESSION_ID ) ";
		Object[] args = {  vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSChildNodeByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_Feed_Tran_Child_prs WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_Feed_Tran_Child_prs.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_Feed_Tran_Child_prs.SESSION_ID ) ";
		Object[] args = {  vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
}
