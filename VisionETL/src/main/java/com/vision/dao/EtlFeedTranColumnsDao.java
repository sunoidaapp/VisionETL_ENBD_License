package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFeedTranColumnVb;

@Component
public class EtlFeedTranColumnsDao extends AbstractDao<EtlFeedTranColumnVb> {

	/*******Mapper Start**********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String ColumnDatatypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TAppr.COLUMN_DATATYPE", "COLUMN_DATATYPE_DESC");
	String ColumnDatatypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TPend.COLUMN_DATATYPE", "COLUMN_DATATYPE_DESC");

	String ColumnExptypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2003, "TAppr.COL_EXPERSSION_TYPE", "COL_EXPERSSION_TYPE_DESC");
	String ColumnExptypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2003, "TPend.COL_EXPERSSION_TYPE", "COL_EXPERSSION_TYPE_DESC");
	
	String DateFormatAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TAppr.DATE_FORMAT", "DATE_FORMAT_DESC");
	String DateFormatAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TPend.DATE_FORMAT", "DATE_FORMAT_DESC");

	String FeedColumnStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TAppr.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");
	String FeedColumnStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000, "TPend.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	
	protected RowMapper getMapper(int intStatus){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedTranColumnVb vObject = new EtlFeedTranColumnVb();
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
				if(rs.getString("COLUMN_ID")!= null){ 
					vObject.setColumnId(rs.getString("COLUMN_ID"));
				}else{
					vObject.setColumnId(rs.getString("COLUMN_ID"));
				}
				if(rs.getString("COLUMN_NAME")!= null){ 
					vObject.setColumnName(rs.getString("COLUMN_NAME"));
				}else{
					vObject.setColumnName("");
				}
				if(rs.getString("COLUMN_DESCRIPTION")!= null){ 
					vObject.setColumnDesc(rs.getString("COLUMN_DESCRIPTION"));
				}else{
					vObject.setColumnDesc("");
				}
				if(rs.getString("DERIVED_COLUMN_FLAG")!= null){ 
					vObject.setDerivedColumnFlag(rs.getString("DERIVED_COLUMN_FLAG"));
				}else{
					vObject.setDerivedColumnFlag("");
				}
				vObject.setExpressionTypeAT(rs.getString("EXPRESSION_TYPE_AT"));
				if(rs.getString("EXPRESSION_TEXT")!= null){ 
					vObject.setExpressionText(rs.getString("EXPRESSION_TEXT"));
				}else{
					vObject.setExpressionText("");
				}
				vObject.setAggFunction(rs.getString("AGG_FUNCTION"));
				if(rs.getString("AGG_EXPRESSION_TEXT")!= null){ 
					vObject.setAggExpressionText(rs.getString("AGG_EXPRESSION_TEXT"));
				}else{
					vObject.setAggExpressionText("");
				}
				vObject.setGroupByFlag(rs.getString("GROUP_BY_FLAG"));
				vObject.setInputFlag(rs.getString("INPUT_FLAG"));
				vObject.setParentNodeId(rs.getString("PARENT_NODE_ID"));
				vObject.setParentColumnId(rs.getString("PARENT_COLUMN_ID"));
				vObject.setOutputFlag(rs.getString("OUTPUT_FLAG"));
				vObject.setDataTypeAT(rs.getString("DATA_TYPE_AT"));
				vObject.setDataType(rs.getString("DATA_TYPE"));
				vObject.setSize(rs.getString("COLUMN_SIZE"));
				vObject.setScale(rs.getString("COLUMN_SCALE"));
				vObject.setNumberFormatNT(rs.getString("NUMBER_FORMAT_NT"));
				vObject.setNumberFormat(rs.getString("NUMBER_FORMAT"));
				vObject.setDateFormatNT(rs.getString("DATE_FORMAT_NT"));
				vObject.setDateFormat(rs.getString("DATE_FORMAT"));
				vObject.setColumnStatusNt(rs.getInt("COLUMN_STATUS_NT"));
				vObject.setColumnStatus(rs.getInt("COLUMN_STATUS"));
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
				vObject.setColumnSortOrder(rs.getInt("COLUMN_SORT_ORDER"));
				vObject.setSlabId(ValidationUtil.isValid(rs.getString("SLAB_ID"))?rs.getString("SLAB_ID"):"");
				vObject.setAliasName(ValidationUtil.isValid(rs.getString("ALIAS_NAME"))?rs.getString("ALIAS_NAME"):"");
				vObject.setLinkedColumn(ValidationUtil.isValid(rs.getString("LINKED_COLUMN"))?rs.getString("LINKED_COLUMN"):"");
				vObject.setJoinOperator(ValidationUtil.isValid(rs.getString("JOIN_OPERATOR"))?rs.getString("JOIN_OPERATOR"):"");
				vObject.setFilterCriteria(ValidationUtil.isValid(rs.getString("FILTER_CRITERIA"))?rs.getString("FILTER_CRITERIA"):"");
				vObject.setFilterValue1(ValidationUtil.isValid(rs.getString("FILTER_VALUE_1"))?rs.getString("FILTER_VALUE_1"):"");
				vObject.setFilterValue2(ValidationUtil.isValid(rs.getString("FILTER_VALUE_2"))?rs.getString("FILTER_VALUE_2"):"");
				vObject.setOutputColumnName(ValidationUtil.isValid(rs.getString("OUTPUT_COLUMN_NAME"))?rs.getString("OUTPUT_COLUMN_NAME"):"");
				return vObject;
			}
		};
		return mapper;
	}

/*******Mapper End**********/
	/*public List<EtlFeedTranColumnVb> getQueryPopupResults(EtlFeedTranColumnVb dObj){
		return null;
	}*/

	public List<EtlFeedTranColumnVb> getQueryResults(EtlFeedTranColumnVb dObj, int intStatus){

		setServiceDefaults();
		List<EtlFeedTranColumnVb> collTemp = null;
		 int intKeyFieldsCount = 4;
			 
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.NODE_ID"
			+ ",TAppr.COLUMN_ID"
			+ ",TAppr.COLUMN_NAME"
			+ ",TAppr.COLUMN_DESCRIPTION"
			+ ",TAppr.DERIVED_COLUMN_FLAG"
			+ ",TAppr.EXPRESSION_TYPE_AT"
			+ ",TAppr.EXPRESSION_TEXT"
			+ ",TAppr.AGG_FUNCTION"
			+ ",TAppr.AGG_EXPRESSION_TEXT"
			+ ",TAppr.GROUP_BY_FLAG"
			+ ",TAppr.INPUT_FLAG"
			+ ",TAppr.PARENT_NODE_ID"
			+ ",TAppr.PARENT_COLUMN_ID"
			+ ",TAppr.OUTPUT_FLAG"
			+ ",TAppr.DATA_TYPE_AT"
			+ ",TAppr.DATA_TYPE"
			+ ",TAppr.COLUMN_SIZE"
			+ ",TAppr.COLUMN_SCALE"
			+ ",TAppr.NUMBER_FORMAT_NT"
			+ ",TAppr.NUMBER_FORMAT"
			+ ",TAppr.DATE_FORMAT_NT"
			+ ",TAppr.DATE_FORMAT"
			+ ",TAppr.COLUMN_STATUS_NT"
			+ ",TAppr.COLUMN_STATUS"
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
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION, "
			+ " TAppr.COLUMN_SORT_ORDER, TAppr.SLAB_ID, TAppr.ALIAS_NAME, TAppr.LINKED_COLUMN, TAppr.JOIN_OPERATOR, "
			+ " TAppr.FILTER_CRITERIA, TAppr.FILTER_VALUE_1, TAppr.FILTER_VALUE_2, TAppr.OUTPUT_COLUMN_NAME "
			+ " From ETL_Feed_Tran_Columns TAppr " + " WHERE TAppr.COUNTRY = ?  AND TAppr.LE_BOOK = ?  "
			+ "AND TAppr.FEED_ID = ? AND TAppr.NODE_ID = ?");
		String strQueryPend = new String("Select TPend.COUNTRY"
				+ ",TPend.LE_BOOK"
				+ ",TPend.FEED_ID"
				+ ",TPend.NODE_ID"
				+ ",TPend.COLUMN_ID"
				+ ",TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_DESCRIPTION"
				+ ",TPend.DERIVED_COLUMN_FLAG"
				+ ",TPend.EXPRESSION_TYPE_AT"
				+ ",TPend.AGG_FUNCTION"
				+ ",TPend.GROUP_BY_FLAG"
				+ ",TPend.INPUT_FLAG"
				+ ",TPend.PARENT_NODE_ID"
				+ ",TPend.PARENT_COLUMN_ID"
				+ ",TPend.OUTPUT_FLAG"
				+ ",TPend.DATA_TYPE_AT"
				+ ",TPend.DATA_TYPE"
				+ ",TPend.COLUMN_SIZE"
				+ ",TPend.COLUMN_SCALE"
				+ ",TPend.NUMBER_FORMAT_NT"
				+ ",TPend.NUMBER_FORMAT"
				+ ",TPend.DATE_FORMAT_NT"
				+ ",TPend.DATE_FORMAT"
				+ ",TPend.COLUMN_STATUS_NT"
				+ ",TPend.COLUMN_STATUS"
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
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION, "
				+ " TPend.COLUMN_SORT_ORDER, TPend.SLAB_ID, TPend.ALIAS_NAME, TPend.LINKED_COLUMN, TPend.JOIN_OPERATOR, "
				+ " TPend.FILTER_CRITERIA, TPend.FILTER_VALUE_1, TPend.FILTER_VALUE_2, TPend.OUTPUT_COLUMN_NAME "
				+ " From ETL_Feed_Tran_Columns_PEND TPend " + " WHERE TPend.COUNTRY = ?  AND TPend.LE_BOOK = ?  "
				+ "AND TPend.FEED_ID = ? AND TPend.NODE_ID = ?");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
				+ ",TUpl.LE_BOOK"
				+ ",TUpl.FEED_ID"
				+ ",TUpl.NODE_ID"
				+ ",TUpl.COLUMN_ID"
				+ ",TUpl.COLUMN_NAME"
				+ ",TUpl.COLUMN_DESCRIPTION"
				+ ",TUpl.DERIVED_COLUMN_FLAG"
				+ ",TUpl.EXPRESSION_TYPE_AT"
				+ ",TUpl.EXPRESSION_TEXT"
				+ ",TUpl.AGG_FUNCTION"
				+ ",TUpl.AGG_EXPRESSION_TEXT"
				+ ",TUpl.GROUP_BY_FLAG"
				+ ",TUpl.INPUT_FLAG"
				+ ",TUpl.PARENT_NODE_ID"
				+ ",TUpl.PARENT_COLUMN_ID"
				+ ",TUpl.OUTPUT_FLAG"
				+ ",TUpl.DATA_TYPE_AT"
				+ ",TUpl.DATA_TYPE"
				+ ",TUpl.COLUMN_SIZE"
				+ ",TUpl.COLUMN_SCALE"
				+ ",TUpl.NUMBER_FORMAT_NT"
				+ ",TUpl.NUMBER_FORMAT"
				+ ",TUpl.DATE_FORMAT_NT"
				+ ",TUpl.DATE_FORMAT"
				+ ",TUpl.COLUMN_STATUS_NT"
				+ ",TUpl.COLUMN_STATUS"
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
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION,"
				+ " TUpl.COLUMN_SORT_ORDER, TUpl.SLAB_ID,  TUpl.ALIAS_NAME, TUpl.LINKED_COLUMN, TUpl.JOIN_OPERATOR,"
				+ " TUpl.FILTER_CRITERIA, TUpl.FILTER_VALUE_1, TUpl.FILTER_VALUE_2, TUpl.OUTPUT_COLUMN_NAME "
				+ " From ETL_Feed_Tran_Columns_UPL TUpl " + " WHERE TUpl.COUNTRY = ?  AND TUpl.LE_BOOK = ?  "
				+ "AND TUpl.FEED_ID = ? AND TUpl.NODE_ID = ?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();
		objParams[3] = dObj.getNodeId();

		if(ValidationUtil.isValid(dObj.getParentNodeId())) {
			intKeyFieldsCount = 5;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getNodeId();
			objParams[4] = dObj.getParentNodeId();
			
			strQueryUpl = strQueryUpl+"  AND  PARENT_NODE_ID =?  ";
			strQueryAppr = strQueryAppr+" AND  PARENT_NODE_ID =?  ";
			strQueryPend = strQueryPend+" AND   PARENT_NODE_ID =?";
		}

		
		if (ValidationUtil.isValid(dObj.getParentColumnId())) {
			intKeyFieldsCount = 6;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getNodeId();
			objParams[4] = dObj.getParentNodeId();
			objParams[5] = dObj.getParentColumnId();

			strQueryUpl = strQueryUpl + "  AND  PARENT_COLUMN_ID =?  ";
			strQueryAppr = strQueryAppr + " AND  PARENT_COLUMN_ID =?  ";
			strQueryPend = strQueryPend + " AND   PARENT_COLUMN_ID =?";
		}
		strQueryUpl = strQueryUpl + " order by COLUMN_SORT_ORDER ";
		strQueryAppr = strQueryAppr + " order by COLUMN_SORT_ORDER ";
		strQueryPend = strQueryPend + " order by COLUMN_SORT_ORDER ";

		try {
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getMapper(intStatus));
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getMapper(intStatus));
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getMapper(intStatus));
			}
			return collTemp;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}
	
	public List<EtlFeedTranColumnVb> getQueryResultsByParent(EtlFeedTranColumnVb dObj, int intStatus){
		setServiceDefaults();
		List<EtlFeedTranColumnVb> collTemp = null;
		 int intKeyFieldsCount = 4;
			 
		String strQueryAppr = new String("Select TAppr.COUNTRY"
			+ ",TAppr.LE_BOOK"
			+ ",TAppr.FEED_ID"
			+ ",TAppr.NODE_ID"
			+ ",TAppr.COLUMN_ID"
			+ ",TAppr.COLUMN_NAME"
			+ ",TAppr.COLUMN_DESCRIPTION"
			+ ",TAppr.DERIVED_COLUMN_FLAG"
			+ ",TAppr.EXPRESSION_TYPE_AT"
			+ ",TAppr.EXPRESSION_TEXT"
			+ ",TAppr.AGG_FUNCTION"
			+ ",TAppr.AGG_EXPRESSION_TEXT"
			+ ",TAppr.GROUP_BY_FLAG"
			+ ",TAppr.INPUT_FLAG"
			+ ",TAppr.PARENT_NODE_ID"
			+ ",TAppr.PARENT_COLUMN_ID"
			+ ",TAppr.OUTPUT_FLAG"
			+ ",TAppr.DATA_TYPE_AT"
			+ ",TAppr.DATA_TYPE"
			+ ",TAppr.COLUMN_SIZE"
			+ ",TAppr.COLUMN_SCALE"
			+ ",TAppr.NUMBER_FORMAT_NT"
			+ ",TAppr.NUMBER_FORMAT"
			+ ",TAppr.DATE_FORMAT_NT"
			+ ",TAppr.DATE_FORMAT"
			+ ",TAppr.COLUMN_STATUS_NT"
			+ ",TAppr.COLUMN_STATUS"
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
			+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION, "
			+ " TAppr.COLUMN_SORT_ORDER, TAppr.SLAB_ID, TAppr.ALIAS_NAME, TAppr.LINKED_COLUMN, TAppr.JOIN_OPERATOR"
			+ " ,TAppr.FILTER_CRITERIA, TAppr.FILTER_VALUE_1, TAppr.FILTER_VALUE_2, TAppr.OUTPUT_COLUMN_NAME "
			+ " From ETL_Feed_Tran_Columns TAppr " + " WHERE TAppr.COUNTRY = ?  AND TAppr.LE_BOOK = ?  "
			+ "AND TAppr.FEED_ID = ? AND TAppr.NODE_ID = ?");
		String strQueryPend = new String("Select TPend.COUNTRY"
				+ ",TPend.LE_BOOK"
				+ ",TPend.FEED_ID"
				+ ",TPend.NODE_ID"
				+ ",TPend.COLUMN_ID"
				+ ",TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_DESCRIPTION"
				+ ",TPend.DERIVED_COLUMN_FLAG"
				+ ",TPend.EXPRESSION_TYPE_AT"
				+ ",TPend.EXPRESSION_TEXT"
				+ ",TPend.AGG_FUNCTION"
				+ ",TPend.AGG_EXPRESSION_TEXT"
				+ ",TPend.GROUP_BY_FLAG"
				+ ",TPend.INPUT_FLAG"
				+ ",TPend.PARENT_NODE_ID"
				+ ",TPend.PARENT_COLUMN_ID"
				+ ",TPend.OUTPUT_FLAG"
				+ ",TPend.DATA_TYPE_AT"
				+ ",TPend.DATA_TYPE"
				+ ",TPend.COLUMN_SIZE"
				+ ",TPend.COLUMN_SCALE"
				+ ",TPend.NUMBER_FORMAT_NT"
				+ ",TPend.NUMBER_FORMAT"
				+ ",TPend.DATE_FORMAT_NT"
				+ ",TPend.DATE_FORMAT"
				+ ",TPend.COLUMN_STATUS_NT"
				+ ",TPend.COLUMN_STATUS"
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
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION,"
				+ " TPend.COLUMN_SORT_ORDER, TPend.SLAB_ID, TPend.ALIAS_NAME, TPend.LINKED_COLUMN, TPend.JOIN_OPERATOR "
				+ ", TPend.FILTER_CRITERIA, TPend.FILTER_VALUE_1, TPend.FILTER_VALUE_2, TPend.OUTPUT_COLUMN_NAME "
				+ " From ETL_Feed_Tran_Columns_PEND TPend " + " WHERE TPend.COUNTRY = ?  AND TPend.LE_BOOK = ?  "
				+ "AND TPend.FEED_ID = ? AND TPend.NODE_ID = ?");
		String strQueryUpl = new String("Select TUpl.COUNTRY"
				+ ",TUpl.LE_BOOK"
				+ ",TUpl.FEED_ID"
				+ ",TUpl.NODE_ID"
				+ ",TUpl.COLUMN_ID"
				+ ",TUpl.COLUMN_NAME"
				+ ",TUpl.COLUMN_DESCRIPTION"
				+ ",TUpl.DERIVED_COLUMN_FLAG"
				+ ",TUpl.EXPRESSION_TYPE_AT"
				+ ",TUpl.EXPRESSION_TEXT"
				+ ",TUpl.AGG_FUNCTION"
				+ ",TUpl.AGG_EXPRESSION_TEXT"
				+ ",TUpl.GROUP_BY_FLAG"
				+ ",TUpl.INPUT_FLAG"
				+ ",TUpl.PARENT_NODE_ID"
				+ ",TUpl.PARENT_COLUMN_ID"
				+ ",TUpl.OUTPUT_FLAG"
				+ ",TUpl.DATA_TYPE_AT"
				+ ",TUpl.DATA_TYPE"
				+ ",TUpl.COLUMN_SIZE"
				+ ",TUpl.COLUMN_SCALE"
				+ ",TUpl.NUMBER_FORMAT_NT"
				+ ",TUpl.NUMBER_FORMAT"
				+ ",TUpl.DATE_FORMAT_NT"
				+ ",TUpl.DATE_FORMAT"
				+ ",TUpl.COLUMN_STATUS_NT"
				+ ",TUpl.COLUMN_STATUS"
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
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION,"
				+ " TUpl.COLUMN_SORT_ORDER, TUpl.SLAB_ID, TUpl.ALIAS_NAME, TUpl.LINKED_COLUMN, TUpl.JOIN_OPERATOR "
				+ ", TUpl.FILTER_CRITERIA, TUpl.FILTER_VALUE_1, TUpl.FILTER_VALUE_2, TUpl.OUTPUT_COLUMN_NAME "
				+ " From ETL_Feed_Tran_Columns_UPL TUpl " + " WHERE TUpl.COUNTRY = ?  AND TUpl.LE_BOOK = ?  "
				+ "AND TUpl.FEED_ID = ? AND TUpl.NODE_ID = ?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();
		objParams[3] = dObj.getNodeId();

		if (ValidationUtil.isValid(dObj.getParentNodeId())) {
			intKeyFieldsCount = 5;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getNodeId();
			objParams[4] = dObj.getParentNodeId();

			strQueryUpl = strQueryUpl + "  AND  PARENT_NODE_ID =?  ";
			strQueryAppr = strQueryAppr + " AND  PARENT_NODE_ID =?  ";
			strQueryPend = strQueryPend + " AND   PARENT_NODE_ID =?";
		}

		if (ValidationUtil.isValid(dObj.getParentColumnId())) {
			intKeyFieldsCount = 6;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getNodeId();
			objParams[4] = dObj.getParentNodeId();
			objParams[5] = dObj.getParentColumnId();

			strQueryUpl = strQueryUpl + "  AND  PARENT_COLUMN_ID =?  ";
			strQueryAppr = strQueryAppr + " AND  PARENT_COLUMN_ID =?  ";
			strQueryPend = strQueryPend + " AND   PARENT_COLUMN_ID =?";
		}
		strQueryUpl = strQueryUpl + " order by COLUMN_SORT_ORDER ";
		strQueryAppr = strQueryAppr + " order by COLUMN_SORT_ORDER ";
		strQueryPend = strQueryPend + " order by COLUMN_SORT_ORDER ";

		try {
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getMapper(intStatus));
			} else if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getMapper(intStatus));
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getMapper(intStatus));
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
	protected List<EtlFeedTranColumnVb> selectApprovedRecord(EtlFeedTranColumnVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<EtlFeedTranColumnVb> doSelectPendingRecord(EtlFeedTranColumnVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}


	@Override
	protected List<EtlFeedTranColumnVb> doSelectUplRecord(EtlFeedTranColumnVb vObject){
		return getQueryResults(vObject, 9999);
	}


	@Override
	protected int getStatus(EtlFeedTranColumnVb records){return records.getColumnStatus();}


	@Override
	protected void setStatus(EtlFeedTranColumnVb vObject,int status){vObject.setColumnStatus(status);}


	@Override
	protected int doInsertionAppr(EtlFeedTranColumnVb vObject){
		String query = "Insert Into ETL_FEED_TRAN_COLUMNS (COUNTRY, LE_BOOK, FEED_ID, "
				+ "NODE_ID, COLUMN_ID, COLUMN_NAME, COLUMN_DESCRIPTION, "
				+ "DERIVED_COLUMN_FLAG, EXPRESSION_TYPE_AT, AGG_FUNCTION, "
				+ "GROUP_BY_FLAG, INPUT_FLAG, PARENT_NODE_ID, PARENT_COLUMN_ID,"
				+ "OUTPUT_FLAG, DATA_TYPE_AT, DATA_TYPE, " + "COLUMN_SIZE,COLUMN_SCALE,NUMBER_FORMAT_NT,"
				+ "NUMBER_FORMAT, DATE_FORMAT_NT, DATE_FORMAT,  "
				+ "COLUMN_STATUS_NT, COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, COLUMN_SORT_ORDER, SLAB_ID, ALIAS_NAME, "
				+ "LINKED_COLUMN, JOIN_OPERATOR, FILTER_CRITERIA, FILTER_VALUE_1, FILTER_VALUE_2, OUTPUT_COLUMN_NAME, EXPRESSION_TEXT, AGG_EXPRESSION_TEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getColumnId(), vObject.getColumnName(), vObject.getColumnDesc(), vObject.getDerivedColumnFlag(),
				vObject.getExpressionTypeAT(), vObject.getAggFunction(), vObject.getGroupByFlag(),
				vObject.getInputFlag(), vObject.getParentNodeId(), vObject.getParentColumnId(), vObject.getOutputFlag(),
				vObject.getDataTypeAT(), vObject.getDataType(), vObject.getSize(), vObject.getScale(),
				vObject.getNumberFormatNT(), vObject.getNumberFormat(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColumnStatusNt(), vObject.getColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getColumnSortOrder(), vObject.getSlabId(), vObject.getAliasName(),
				vObject.getLinkedColumn(), vObject.getJoinOperator(), vObject.getFilterCriteria(),
				vObject.getFilterValue1(), vObject.getFilterValue2(), vObject.getOutputColumnName() };
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
					String clobData = ValidationUtil.isValid(vObject.getExpressionText())?vObject.getExpressionText():"";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getAggExpressionText())?vObject.getAggExpressionText():"";
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
	protected int doInsertionPend(EtlFeedTranColumnVb vObject){
		String query = "Insert Into ETL_FEED_TRAN_COLUMNS_PEND (COUNTRY, LE_BOOK, FEED_ID, "
				+ "NODE_ID, COLUMN_ID, COLUMN_NAME, COLUMN_DESCRIPTION, "
				+ "DERIVED_COLUMN_FLAG, EXPRESSION_TYPE_AT, AGG_FUNCTION, "
				+ "GROUP_BY_FLAG, INPUT_FLAG, PARENT_NODE_ID, PARENT_COLUMN_ID,"
				+ "OUTPUT_FLAG, DATA_TYPE_AT, DATA_TYPE, " + "COLUMN_SIZE,COLUMN_SCALE,NUMBER_FORMAT_NT,"
				+ "NUMBER_FORMAT, DATE_FORMAT_NT, DATE_FORMAT,  "
				+ "COLUMN_STATUS_NT, COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, COLUMN_SORT_ORDER, SLAB_ID, ALIAS_NAME, LINKED_COLUMN, JOIN_OPERATOR, "
				+ " FILTER_CRITERIA, FILTER_VALUE_1, FILTER_VALUE_2, OUTPUT_COLUMN_NAME, EXPRESSION_TEXT, AGG_EXPRESSION_TEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getColumnId(), vObject.getColumnName(), vObject.getColumnDesc(), vObject.getDerivedColumnFlag(),
				vObject.getExpressionTypeAT(), vObject.getAggFunction(), vObject.getGroupByFlag(),
				vObject.getInputFlag(), vObject.getParentNodeId(), vObject.getParentColumnId(), vObject.getOutputFlag(),
				vObject.getDataTypeAT(), vObject.getDataType(), vObject.getSize(), vObject.getScale(),
				vObject.getNumberFormatNT(), vObject.getNumberFormat(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColumnStatusNt(), vObject.getColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getColumnSortOrder(), vObject.getSlabId(), vObject.getAliasName(),
				vObject.getLinkedColumn(), vObject.getJoinOperator(), vObject.getFilterCriteria(),
				vObject.getFilterValue1(), vObject.getFilterValue2(), vObject.getOutputColumnName() };
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
					String clobData = ValidationUtil.isValid(vObject.getExpressionText())?vObject.getExpressionText():"";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getAggExpressionText())?vObject.getAggExpressionText():"";
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
	protected int doInsertionUpl(EtlFeedTranColumnVb vObject){
		String query = "Insert Into ETL_FEED_TRAN_COLUMNS_UPL (COUNTRY, LE_BOOK, FEED_ID, "
				+ "NODE_ID, COLUMN_ID, COLUMN_NAME, COLUMN_DESCRIPTION, "
				+ "DERIVED_COLUMN_FLAG, EXPRESSION_TYPE_AT, AGG_FUNCTION, "
				+ "GROUP_BY_FLAG, INPUT_FLAG, PARENT_NODE_ID, PARENT_COLUMN_ID,"
				+ "OUTPUT_FLAG, DATA_TYPE_AT, DATA_TYPE, " + "COLUMN_SIZE,COLUMN_SCALE,NUMBER_FORMAT_NT,"
				+ "NUMBER_FORMAT, DATE_FORMAT_NT, DATE_FORMAT,  "
				+ "COLUMN_STATUS_NT, COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, COLUMN_SORT_ORDER, SLAB_ID, ALIAS_NAME, LINKED_COLUMN, JOIN_OPERATOR,"
				+ "FILTER_CRITERIA, FILTER_VALUE_1, FILTER_VALUE_2, OUTPUT_COLUMN_NAME, EXPRESSION_TEXT, AGG_EXPRESSION_TEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getColumnId(), vObject.getColumnName(), vObject.getColumnDesc(), vObject.getDerivedColumnFlag(),
				vObject.getExpressionTypeAT(), vObject.getAggFunction(), vObject.getGroupByFlag(),
				vObject.getInputFlag(), vObject.getParentNodeId(), vObject.getParentColumnId(), vObject.getOutputFlag(),
				vObject.getDataTypeAT(), vObject.getDataType(), vObject.getSize(), vObject.getScale(),
				vObject.getNumberFormatNT(), vObject.getNumberFormat(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColumnStatusNt(), vObject.getColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getColumnSortOrder(), vObject.getSlabId(), vObject.getAliasName(),
				vObject.getLinkedColumn(), vObject.getJoinOperator(), vObject.getFilterCriteria(),
				vObject.getFilterValue1(), vObject.getFilterValue2(), vObject.getOutputColumnName() };
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
					String clobData = ValidationUtil.isValid(vObject.getExpressionText())?vObject.getExpressionText():"";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getAggExpressionText())?vObject.getAggExpressionText():"";
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
	protected int doInsertionPendWithDc(EtlFeedTranColumnVb vObject){
		String query = "Insert Into ETL_FEED_TRAN_COLUMNS_PEND (COUNTRY, LE_BOOK, FEED_ID, "
				+ "NODE_ID, COLUMN_ID, COLUMN_NAME, COLUMN_DESCRIPTION, "
				+ "DERIVED_COLUMN_FLAG, EXPRESSION_TYPE_AT, AGG_FUNCTION, "
				+ "GROUP_BY_FLAG, INPUT_FLAG, PARENT_NODE_ID, PARENT_COLUMN_ID,"
				+ "OUTPUT_FLAG, DATA_TYPE_AT, DATA_TYPE, " + "COLUMN_SIZE,COLUMN_SCALE,NUMBER_FORMAT_NT,"
				+ "NUMBER_FORMAT, DATE_FORMAT_NT, DATE_FORMAT,  "
				+ "COLUMN_STATUS_NT, COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, COLUMN_SORT_ORDER, SLAB_ID, ALIAS_NAME, LINKED_COLUMN, JOIN_OPERATOR,"
				+ "FILTER_CRITERIA, FILTER_VALUE_1, FILTER_VALUE_2, OUTPUT_COLUMN_NAME, EXPRESSION_TEXT, AGG_EXPRESSION_TEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?,?,?,?,?,?,?,?,?,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getNodeId(),
				vObject.getColumnId(), vObject.getColumnName(), vObject.getColumnDesc(), vObject.getDerivedColumnFlag(),
				vObject.getExpressionTypeAT(), vObject.getAggFunction(), vObject.getGroupByFlag(),
				vObject.getInputFlag(), vObject.getParentNodeId(), vObject.getParentColumnId(), vObject.getOutputFlag(),
				vObject.getDataTypeAT(), vObject.getDataType(), vObject.getSize(), vObject.getScale(),
				vObject.getNumberFormatNT(), vObject.getNumberFormat(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColumnStatusNt(), vObject.getColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getColumnSortOrder(), vObject.getSlabId(), vObject.getAliasName(),
				vObject.getLinkedColumn(), vObject.getJoinOperator(), vObject.getFilterCriteria(),
				vObject.getFilterValue1(), vObject.getFilterValue2(), vObject.getOutputColumnName() };
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
					String clobData = ValidationUtil.isValid(vObject.getExpressionText())?vObject.getExpressionText():"";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getAggExpressionText())?vObject.getAggExpressionText():"";
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
	protected int doUpdateAppr(EtlFeedTranColumnVb vObject){
		String query = "Update ETL_FEED_COLUMNS Set EXPRESSION_TEXT = ?, AGG_EXPRESSION_TEXT = ?, "
				+ " COLUMN_NAME = ?, COLUMN_DESCRIPTION = ?," + " DERIVED_COLUMN_FLAG = ?, "
				+ " EXPRESSION_TYPE_AT = ?, AGG_FUNCTION = ?, GROUP_BY_FLAG = ?,"
				+ " INPUT_FLAG = ?, PARENT_NODE_ID = ?," + " PARENT_COLUMN_ID =?, OUTPUT_FLAG =? ,DATA_TYPE_AT = ?, "
				+ " DATA_TYPE = ?," + " COLUMN_SIZE =?, COLUMN_SCALE =? ,NUMBER_FORMAT_NT = ?, "
				+ " NUMBER_FORMAT =?, DATE_FORMAT_NT =?, DATE_FORMAT = ?, "
				+ " COLUMN_STATUS_NT = ?, COLUMN_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,"
				+ " INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ "  " + " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND NODE_ID = ?  AND COLUMN_ID = ? ";

		Object[] args = { vObject.getColumnName(), vObject.getColumnDesc(), vObject.getDerivedColumnFlag(),
				vObject.getExpressionTypeAT(), vObject.getAggFunction(), vObject.getGroupByFlag(),
				vObject.getInputFlag(), vObject.getParentNodeId(), vObject.getParentColumnId(), vObject.getOutputFlag(),
				vObject.getDataTypeAT(), vObject.getDataType(), vObject.getSize(), vObject.getScale(),
				vObject.getNumberFormatNT(), vObject.getNumberFormat(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColumnStatusNt(), vObject.getColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getNodeId(), vObject.getColumnId() };

		int result=0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getExpressionText())?vObject.getExpressionText():"";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getAggExpressionText())?vObject.getExpressionText():"";
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
	protected int doUpdateUpl(EtlFeedTranColumnVb vObject){
		String query = "Update ETL_FEED_COLUMNS_UPL Set EXPRESSION_TEXT = ?, AGG_EXPRESSION_TEXT = ?, "
				+ " COLUMN_NAME = ?, COLUMN_DESCRIPTION = ?," + " DERIVED_COLUMN_FLAG = ?, "
				+ " EXPRESSION_TYPE_AT = ?, AGG_FUNCTION = ?, GROUP_BY_FLAG = ?,"
				+ " INPUT_FLAG = ?, PARENT_NODE_ID = ?," + " PARENT_COLUMN_ID =?, OUTPUT_FLAG =? ,DATA_TYPE_AT = ?, "
				+ " DATA_TYPE = ?," + " COLUMN_SIZE =?, COLUMN_SCALE =? ,NUMBER_FORMAT_NT = ?, "
				+ " NUMBER_FORMAT =?, DATE_FORMAT_NT =? ,DATE_FORMAT = ?, "
				+ " COLUMN_STATUS_NT = ?, COLUMN_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,"
				+ " INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ "  " + " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND NODE_ID = ?  AND COLUMN_ID = ? ";

		Object[] args = { vObject.getColumnName(), vObject.getColumnDesc(), vObject.getDerivedColumnFlag(),
				vObject.getExpressionTypeAT(), vObject.getAggFunction(), vObject.getGroupByFlag(),
				vObject.getInputFlag(), vObject.getParentNodeId(), vObject.getParentColumnId(), vObject.getOutputFlag(),
				vObject.getDataTypeAT(), vObject.getDataType(), vObject.getSize(), vObject.getScale(),
				vObject.getNumberFormatNT(), vObject.getNumberFormat(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColumnStatusNt(), vObject.getColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getNodeId(), vObject.getColumnId() };

		int result=0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getExpressionText())?vObject.getExpressionText():"";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getAggExpressionText())?vObject.getExpressionText():"";
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
	protected int doUpdatePend(EtlFeedTranColumnVb vObject){
		String query = "Update ETL_FEED_COLUMNS_PEND Set EXPRESSION_TEXT = ?, AGG_EXPRESSION_TEXT = ?, "
				+ " COLUMN_NAME = ?, COLUMN_DESCRIPTION = ?," + " DERIVED_COLUMN_FLAG = ?, "
				+ " EXPRESSION_TYPE_AT = ?, AGG_FUNCTION = ?, GROUP_BY_FLAG = ?,"
				+ " INPUT_FLAG = ?, PARENT_NODE_ID = ?," + " PARENT_COLUMN_ID =?, OUTPUT_FLAG =? ,DATA_TYPE_AT = ?, "
				+ " DATA_TYPE = ?," + " COLUMN_SIZE =?, COLUMN_SCALE =? ,NUMBER_FORMAT_NT = ?, "
				+ " NUMBER_FORMAT =?, DATE_FORMAT_NT =? ,DATE_FORMAT = ?, "
				+ " COLUMN_STATUS_NT = ?, COLUMN_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,"
				+ " INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ "  " + " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND NODE_ID = ?  AND COLUMN_ID = ? ";

		Object[] args = { vObject.getColumnName(), vObject.getColumnDesc(), vObject.getDerivedColumnFlag(),
				vObject.getExpressionTypeAT(), vObject.getAggFunction(), vObject.getGroupByFlag(),
				vObject.getInputFlag(), vObject.getParentNodeId(), vObject.getParentColumnId(), vObject.getOutputFlag(),
				vObject.getDataTypeAT(), vObject.getDataType(), vObject.getSize(), vObject.getScale(),
				vObject.getNumberFormatNT(), vObject.getNumberFormat(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColumnStatusNt(), vObject.getColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getNodeId(), vObject.getColumnId() };

		int result=0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getExpressionText())?vObject.getExpressionText():"";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());
					
					clobData = ValidationUtil.isValid(vObject.getAggExpressionText())?vObject.getExpressionText():"";
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
	protected int doDeleteAppr(EtlFeedTranColumnVb vObject){
		String query = "Delete From ETL_Feed_Tran_Columns Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deletePendingRecord(EtlFeedTranColumnVb vObject){
		String query = "Delete From ETL_Feed_Tran_Columns_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()  };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deleteUplRecord(EtlFeedTranColumnVb vObject){
		String query = "Delete From ETL_Feed_Tran_Columns_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()  };
		return getJdbcTemplate().update(query,args);
	}
	protected int deleteUplRecordByParent(EtlFeedMainVb vObject){
		String query = "Delete From ETL_Feed_Tran_Columns_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_Feed_Tran_Columns";
		if("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Columns_UPL";
		}else if("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Columns_PEND";
		}
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId()  };
		return getJdbcTemplate().update(query,args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_Feed_Tran_Columns_PRS";
		String query = "Delete From "+table+" Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND SESSION_ID=? ";
		if(ValidationUtil.isValid(vObject.getNodeId())) {
			query =query +" AND NODE_ID = '"+vObject.getNodeId()+"'";
		}
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() ,  vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_Feed_Tran_Columns";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Columns_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_Feed_Tran_Columns_PEND";
		}
		String query = "update " + table
				+ " set COLUMN_STATUS = ? ,RECORD_INDICATOR =? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		if (ValidationUtil.isValid(vObject.getNodeId())) {
			query = query + " AND NODE_ID = '" + vObject.getNodeId() + "'";
		}
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedTranColumnVb vObject){
		StringBuffer strAudit = new StringBuffer("");	
		return strAudit.toString();
	}

	@Override
	public void setServiceDefaults(){
		serviceName = "EtlFeedColumns";
		serviceDesc = "ETL Feed Columns";
		tableName = "ETL_Feed_Tran_Columns";
		childTableName = "ETL_Feed_Tran_Columns";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String query = "delete FROM ETL_Feed_Tran_Columns_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_Feed_Tran_Columns_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_Feed_Tran_Columns_PRS.SESSION_ID ) ";
		Object[] args = { vObject.getFeedCategory() ,  vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
}
