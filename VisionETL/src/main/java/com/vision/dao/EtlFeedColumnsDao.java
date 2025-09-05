package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.ETLFeedColumnsVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.ETLFeedTablesVb;

@Component
public class EtlFeedColumnsDao extends AbstractDao<ETLFeedColumnsVb> {

	/******* Mapper Start **********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";

	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String ColumnDatatypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TAppr.COLUMN_DATATYPE",
			"COLUMN_DATATYPE_DESC");
	String ColumnDatatypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2104, "TPend.COLUMN_DATATYPE",
			"COLUMN_DATATYPE_DESC");

	String ColumnExptypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2003, "TAppr.COL_EXPERSSION_TYPE",
			"COL_EXPERSSION_TYPE_DESC");
	String ColumnExptypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2003, "TPend.COL_EXPERSSION_TYPE",
			"COL_EXPERSSION_TYPE_DESC");

	String DateFormatAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TAppr.DATE_FORMAT",
			"DATE_FORMAT_DESC");
	String DateFormatAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TPend.DATE_FORMAT",
			"DATE_FORMAT_DESC");

	String FeedColumnStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000,
			"TAppr.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");
	String FeedColumnStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000,
			"TPend.FEED_COLUMN_STATUS", "FEED_COLUMN_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Autowired
	private EtlFeedTablesDao etlFeedTablesDao;

	protected RowMapper getMapper(int intStatus) {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				ETLFeedColumnsVb vObject = new ETLFeedColumnsVb();
				if (rs.getString("COUNTRY") != null) {
					vObject.setCountry(rs.getString("COUNTRY"));
				} else {
					vObject.setCountry("");
				}
				if (rs.getString("LE_BOOK") != null) {
					vObject.setLeBook(rs.getString("LE_BOOK"));
				} else {
					vObject.setLeBook("");
				}
				if (rs.getString("FEED_ID") != null) {
					vObject.setFeedId(rs.getString("FEED_ID"));
				} else {
					vObject.setFeedId("");
				}
				if (rs.getString("SOURCE_CONNECTOR_ID") != null) {
					vObject.setSourceConnectorId(rs.getString("SOURCE_CONNECTOR_ID"));
				} else {
					vObject.setSourceConnectorId("");
				}
				if (rs.getString("TABLE_ID") != null) {
					vObject.setTableId(rs.getString("TABLE_ID"));
				} else {
					vObject.setTableId("");
				}
				if (rs.getString("COLUMN_ID") != null) {
					vObject.setColumnId(rs.getString("COLUMN_ID"));
				} else {
					vObject.setColumnId("");
				}
				if (rs.getString("COLUMN_NAME") != null) {
					vObject.setColumnName(rs.getString("COLUMN_NAME"));
				} else {
					vObject.setColumnName("");
				}
				if (rs.getString("COLUMN_ALIAS_NAME") != null) {
					vObject.setColumnAliasName(rs.getString("COLUMN_ALIAS_NAME"));
				} else {
					vObject.setColumnAliasName("");
				}
				vObject.setColumnSortOrder(rs.getInt("COLUMN_SORT_ORDER"));
				vObject.setColumnDataTypeAT(rs.getInt("COLUMN_DATATYPE_AT"));
				if (rs.getString("COLUMN_DATATYPE") != null) {
					vObject.setColumnDataType(rs.getString("COLUMN_DATATYPE"));
				} else {
					vObject.setColumnDataType("");
				}
				vObject.setColumnLength(rs.getString("COLUMN_LENGTH"));
				vObject.setDateFormatNT(rs.getInt("DATE_FORMAT_NT"));
				if (rs.getString("DATE_FORMAT") != null) {
					vObject.setDateFormat(rs.getString("DATE_FORMAT"));
				} else {
					vObject.setDateFormat("");
				}
				vObject.setColExperssionTypeAt(rs.getInt("COL_EXPERSSION_TYPE_AT"));
				if (rs.getString("COL_EXPERSSION_TYPE") != null) {
					vObject.setColExperssionType(rs.getString("COL_EXPERSSION_TYPE"));
				} else {
					vObject.setColExperssionType("");
				}
				vObject.setFormatTypeDesc(rs.getString("FORMAT_TYPE_DESC"));
				vObject.setDateFormattingSyntax(rs.getString("DATE_FORMATTING_SYNTAX"));
				vObject.setDateConversionSyntax(rs.getString("DATE_CONVERSION_SYNTAX"));
				vObject.setJavaFormatDesc(rs.getString("JAVA_DATE_FORMAT"));

				if (ValidationUtil.isValid(rs.getString("EXPERSSION_TEXT"))) {
					if (rs.getString("EXPERSSION_TEXT").contains("#TABLE_ALIAS#")) {
						String tableAliasName = "";
						String tableName = "";
						List<ETLFeedTablesVb> collTemp = null;

						EtlFeedMainVb vb = new EtlFeedMainVb();
						vb.setCountry(vObject.getCountry());
						vb.setLeBook(vObject.getLeBook());
						vb.setFeedId(vObject.getFeedId());
						if (intStatus == 8888) {
							collTemp = etlFeedTablesDao.getQueryResultsByParent(vb, 9999);
							if (collTemp == null) {
								collTemp = etlFeedTablesDao.getQueryResultsByParent(vb, 0);
								if (collTemp == null) {
									collTemp = etlFeedTablesDao.getQueryResultsByParent(vb, 1);
								}
							}
						} else if (intStatus == 9999) {
							collTemp = etlFeedTablesDao.getQueryResultsByParent(vb, intStatus);
						} else if (intStatus == 0) {
							collTemp = etlFeedTablesDao.getQueryResultsByParent(vb, intStatus);
						} else {
							collTemp = etlFeedTablesDao.getQueryResultsByParent(vb, intStatus);
						}
						if (collTemp != null && collTemp.size() > 0) {
							tableAliasName = collTemp.get(0).getTableAliasName();
							tableName = collTemp.get(0).getTableAliasName();
						}
						vObject.setExperssionText(rs.getString("EXPERSSION_TEXT").replaceAll("#TABLE_ALIAS#",
								ValidationUtil.isValid(tableAliasName) ? tableAliasName : tableName));
					} else {
						vObject.setExperssionText(rs.getString("EXPERSSION_TEXT"));
					}
				}
				if (rs.getString("PRIMARY_KEY_FLAG") != null) {
					vObject.setPrimaryKeyFlag(rs.getString("PRIMARY_KEY_FLAG"));
				} else {
					vObject.setPrimaryKeyFlag("");
				}
				if (rs.getString("PARTITION_COLUMN_FLAG") != null) {
					vObject.setPartitionColumnFlag(rs.getString("PARTITION_COLUMN_FLAG"));
				} else {
					vObject.setPartitionColumnFlag("");
				}
				if (rs.getString("FILTER_CONTEXT") != null) {
					vObject.setFilterContext(rs.getString("FILTER_CONTEXT"));
				} else {
					vObject.setFilterContext("");
				}
				vObject.setFeedColumnStatusNT(rs.getInt("FEED_COLUMN_STATUS_NT"));
				vObject.setFeedColumnStatus(rs.getInt("FEED_COLUMN_STATUS"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				if (rs.getString("DATE_LAST_MODIFIED") != null) {
					vObject.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				} else {
					vObject.setDateLastModified("");
				}
				if (rs.getString("DATE_CREATION") != null) {
					vObject.setDateCreation(rs.getString("DATE_CREATION"));
				} else {
					vObject.setDateCreation("");
				}
				return vObject;
			}
		};
		return mapper;
	}

	/******* Mapper End **********/
	public List<ETLFeedColumnsVb> getQueryPopupResults(ETLFeedColumnsVb dObj) {

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.FEED_ID"
				+ ",TAppr.SOURCE_CONNECTOR_ID,TAppr.TABLE_ID,TAppr.COLUMN_ID,TAppr.COLUMN_NAME"
				+ ",TAppr.COLUMN_ALIAS_NAME,TAppr.COLUMN_SORT_ORDER,TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE,TAppr.COLUMN_LENGTH,TAppr.DATE_FORMAT_NT,TAppr.DATE_FORMAT"
				+ ",TAppr.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TAppr.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TAppr.COL_EXPERSSION_TYPE_AT,TAppr.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TAppr.EXPERSSION_TEXT") + " EXPERSSION_TEXT"
				+ ",TAppr.PRIMARY_KEY_FLAG,TAppr.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TAppr.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TAppr.FEED_COLUMN_STATUS_NT,TAppr.FEED_COLUMN_STATUS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_FEED_COLUMNS TAppr ");
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_FEED_COLUMNS_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.FEED_ID = TPend.FEED_ID AND TAppr.TABLE_ID = TPend.TABLE_ID AND TAppr.COLUMN_ID = TPend.COLUMN_ID AND TAppr.SOURCE_CONNECTOR_ID = TPend.SOURCE_CONNECTOR_ID)");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.FEED_ID"
				+ ",TPend.SOURCE_CONNECTOR_ID,TPend.TABLE_ID,TPend.COLUMN_ID,TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_ALIAS_NAME,TPend.COLUMN_SORT_ORDER,TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE,TPend.COLUMN_LENGTH,TPend.DATE_FORMAT_NT,TPend.DATE_FORMAT"
				+ ",TPend.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TPend.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TPend.COL_EXPERSSION_TYPE_AT,TPend.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TPend.EXPERSSION_TEXT") + " EXPERSSION_TEXT"
				+ ",TPend.PRIMARY_KEY_FLAG,TPend.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TPend.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TPend.FEED_COLUMN_STATUS_NT,TPend.FEED_COLUMN_STATUS,TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_FEED_COLUMNS_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.FEED_ID"
				+ ",TUpl.SOURCE_CONNECTOR_ID,TUpl.TABLE_ID,TUpl.COLUMN_ID,TUpl.COLUMN_NAME"
				+ ",TUpl.COLUMN_ALIAS_NAME,TUpl.COLUMN_SORT_ORDER,TUpl.COLUMN_DATATYPE_AT"
				+ ",TUpl.COLUMN_DATATYPE,TUpl.COLUMN_LENGTH,TUpl.DATE_FORMAT_NT,TUpl.DATE_FORMAT"
				+ ",TUpl.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TUpl.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TUpl.COL_EXPERSSION_TYPE_AT,TUpl.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TUpl.EXPERSSION_TEXT") + " EXPERSSION_TEXT"
				+ ",TUpl.PRIMARY_KEY_FLAG,TUpl.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TUpl.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TUpl.FEED_COLUMN_STATUS_NT,TUpl.FEED_COLUMN_STATUS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_FEED_COLUMNS_UPL TUpl ");

		String strWhereNotExistsApprInUpl = new String(
				" Not Exists (Select 'X' From ETL_FEED_COLUMNS_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY "
						+ "AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.FEED_ID = TUpl.FEED_ID AND TAppr.TABLE_ID = TUpl.TABLE_ID AND TAppr.COLUMN_ID = TUpl.COLUMN_ID AND TAppr.SOURCE_CONNECTOR_ID = TUpl.SOURCE_CONNECTOR_ID) ");
		String strWhereNotExistsPendInUpl = new String(
				" Not Exists (Select 'X' From ETL_FEED_COLUMNS_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY "
						+ "AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.FEED_ID = TPend.FEED_ID AND TUpl.TABLE_ID = TPend.TABLE_ID AND TUpl.COLUMN_ID = TPend.COLUMN_ID AND TUpl.SOURCE_CONNECTOR_ID = TPend.SOURCE_CONNECTOR_ID) ");
		try {
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
			if (ValidationUtil.isValid(dObj.getSourceConnectorId())) {
				params.addElement(dObj.getSourceConnectorId());
				CommonUtils.addToQuery("UPPER(TAppr.SOURCE_CONNECTOR_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.SOURCE_CONNECTOR_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.SOURCE_CONNECTOR_ID) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getTableId())) {
				params.addElement(dObj.getTableId());
				CommonUtils.addToQuery("UPPER(TAppr.TABLE_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.TABLE_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.TABLE_ID) = ?", strBufUpl);
			}
			/*
			 * if (ValidationUtil.isValid(dObj.getColumnId())){
			 * params.addElement(dObj.getColumnId());
			 * CommonUtils.addToQuery("UPPER(TAppr.COLUMN_ID) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.COLUMN_ID) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.COLUMN_ID) = ?", strBufPending); } if
			 * (ValidationUtil.isValid(dObj.getColumnName())){
			 * params.addElement(dObj.getColumnName());
			 * CommonUtils.addToQuery("UPPER(TAppr.COLUMN_NAME) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.COLUMN_NAME) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.COLUMN_NAME) = ?", strBufPending); } if
			 * (ValidationUtil.isValid(dObj.getColumnAliasName())){
			 * params.addElement(dObj.getColumnAliasName());
			 * CommonUtils.addToQuery("UPPER(TAppr.COLUMN_ALIAS_NAME) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.COLUMN_ALIAS_NAME) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.COLUMN_ALIAS_NAME) = ?", strBufPending); }
			 * if (ValidationUtil.isValid(dObj.getColumnSortOrder())){
			 * params.addElement(dObj.getColumnSortOrder());
			 * CommonUtils.addToQuery("TAppr.COLUMN_SORT_ORDER = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.COLUMN_SORT_ORDER = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.COLUMN_SORT_ORDER = ?", strBufPending); } if
			 * (ValidationUtil.isValid(dObj.getColumnDataType())){
			 * params.addElement(dObj.getColumnDataType());
			 * CommonUtils.addToQuery("UPPER(TAppr.COLUMN_DATATYPE) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.COLUMN_DATATYPE) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.COLUMN_DATATYPE) = ?", strBufPending); }
			 * if (ValidationUtil.isValid(dObj.getColumnLength())){
			 * params.addElement(dObj.getColumnLength());
			 * CommonUtils.addToQuery("TAppr.COLUMN_LENGTH = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.COLUMN_LENGTH = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.COLUMN_LENGTH = ?", strBufPending); } if
			 * (ValidationUtil.isValid(dObj.getDateFormatNT())){
			 * params.addElement(dObj.getDateFormatNT());
			 * CommonUtils.addToQuery("UPPER(TAppr.DATE_FORMAT) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.DATE_FORMAT) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.DATE_FORMAT) = ?", strBufPending); } if
			 * (ValidationUtil.isValid(dObj.getNumberFormAT())){
			 * params.addElement(dObj.getNumberFormAT());
			 * CommonUtils.addToQuery("UPPER(TAppr.NUMBER_FORMAT) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.NUMBER_FORMAT) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.NUMBER_FORMAT) = ?", strBufPending); } if
			 * (ValidationUtil.isValid(dObj.getPrimaryKeyFlag())){
			 * params.addElement(dObj.getPrimaryKeyFlag());
			 * CommonUtils.addToQuery("UPPER(TAppr.PRIMARY_KEY_FLAG) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.PRIMARY_KEY_FLAG) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.PRIMARY_KEY_FLAG) = ?", strBufPending); }
			 * if (ValidationUtil.isValid(dObj.getPartitionColumnFlag())){
			 * params.addElement(dObj.getPartitionColumnFlag());
			 * CommonUtils.addToQuery("UPPER(TAppr.PARTITION_COLUMN_FLAG) = ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.PARTITION_COLUMN_FLAG) = ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.PARTITION_COLUMN_FLAG) = ?",
			 * strBufPending); } if (ValidationUtil.isValid(dObj.getFilterContext())){
			 * params.addElement(dObj.getFilterContext());
			 * CommonUtils.addToQuery("UPPER(TAppr.FILTER_CONTEXT) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.FILTER_CONTEXT) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.FILTER_CONTEXT) = ?", strBufPending); } if
			 * (ValidationUtil.isValid(dObj.getFeedColumnStatus())){
			 * params.addElement(dObj.getFeedColumnStatus());
			 * CommonUtils.addToQuery("TAppr.FEED_COLUMN_STATUS = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.FEED_COLUMN_STATUS = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.FEED_COLUMN_STATUS = ?", strBufPending); } if
			 * (dObj.getRecordIndicator() != -1){ if (dObj.getRecordIndicator() > 3){
			 * params.addElement(new Integer(0));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufPending); }else{
			 * params.addElement(new Integer(dObj.getRecordIndicator()));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufPending); } }
			 */
			String orderBy = " Order By COUNTRY, LE_BOOK, FEED_ID, TABLE_ID, COLUMN_ID ";
			CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);
			CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);
			StringBuffer lPageQuery = new StringBuffer();
			if (dObj.isVerificationRequired())
				lPageQuery.append(strBufPending.toString() + " Union " + strBufUpl.toString());
			else
				lPageQuery.append(strBufApprove.toString() + " Union " + strBufUpl.toString());
			return getQueryPopupResultsPgn(dObj, lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,
					getMapper(8888));
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;

		}
	}

	public List<ETLFeedColumnsVb> getQueryResults(ETLFeedColumnsVb dObj, int intStatus) {
		setServiceDefaults();
		List<ETLFeedColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.FEED_ID"
				+ ",TAppr.SOURCE_CONNECTOR_ID,TAppr.TABLE_ID,TAppr.COLUMN_ID,TAppr.COLUMN_NAME"
				+ ",TAppr.COLUMN_ALIAS_NAME,TAppr.COLUMN_SORT_ORDER,TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE,TAppr.COLUMN_LENGTH,TAppr.DATE_FORMAT_NT,TAppr.DATE_FORMAT"
				+ ",TAppr.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TAppr.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TAppr.COL_EXPERSSION_TYPE_AT,TAppr.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TAppr.EXPERSSION_TEXT") + " EXPERSSION_TEXT"
				+ ",TAppr.PRIMARY_KEY_FLAG,TAppr.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TAppr.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TAppr.FEED_COLUMN_STATUS_NT,TAppr.FEED_COLUMN_STATUS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION From ETL_FEED_COLUMNS TAppr "
				+ "  WHERE TAppr.COUNTRY = ?  AND TAppr.LE_BOOK = ?  AND TAppr.FEED_ID = ?   ");
		String strQueryPend = new String("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.FEED_ID"
				+ ",TPend.SOURCE_CONNECTOR_ID,TPend.TABLE_ID,TPend.COLUMN_ID,TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_ALIAS_NAME,TPend.COLUMN_SORT_ORDER,TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE,TPend.COLUMN_LENGTH,TPend.DATE_FORMAT_NT,TPend.DATE_FORMAT"
				+ ",TPend.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TPend.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TPend.COL_EXPERSSION_TYPE_AT,TPend.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TPend.EXPERSSION_TEXT") + "  EXPERSSION_TEXT"
				+ ",TPend.PRIMARY_KEY_FLAG,TPend.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TPend.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TPend.FEED_COLUMN_STATUS_NT,TPend.FEED_COLUMN_STATUS,TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION From ETL_FEED_COLUMNS_PEND TPend"
				+ "  WHERE TPend.COUNTRY = ?  AND TPend.LE_BOOK = ?  AND TPend.FEED_ID = ?  ");
		String strQueryUpl = new String("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.FEED_ID"
				+ ",TUpl.SOURCE_CONNECTOR_ID,TUpl.TABLE_ID,TUpl.COLUMN_ID,TUpl.COLUMN_NAME"
				+ ",TUpl.COLUMN_ALIAS_NAME,TUpl.COLUMN_SORT_ORDER,TUpl.COLUMN_DATATYPE_AT"
				+ ",TUpl.COLUMN_DATATYPE,TUpl.COLUMN_LENGTH,TUpl.DATE_FORMAT_NT,TUpl.DATE_FORMAT"
				+ ",TUpl.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TUpl.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TUpl.COL_EXPERSSION_TYPE_AT,TUpl.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TUpl.EXPERSSION_TEXT") + "  EXPERSSION_TEXT"
				+ ",TUpl.PRIMARY_KEY_FLAG,TUpl.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TUpl.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TUpl.FEED_COLUMN_STATUS_NT,TUpl.FEED_COLUMN_STATUS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION From ETL_FEED_COLUMNS_UPL TUpl "
				+ "  WHERE TUpl.COUNTRY = ?  AND TUpl.LE_BOOK = ?  AND TUpl.FEED_ID = ?  ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
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

	public List<ETLFeedColumnsVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus) {
		setServiceDefaults();
		List<ETLFeedColumnsVb> collTemp = null;
		int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.FEED_ID"
				+ ",TAppr.SOURCE_CONNECTOR_ID,TAppr.TABLE_ID,TAppr.COLUMN_ID,TAppr.COLUMN_NAME"
				+ ",TAppr.COLUMN_ALIAS_NAME,TAppr.COLUMN_SORT_ORDER,TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE,TAppr.COLUMN_LENGTH,TAppr.DATE_FORMAT_NT,TAppr.DATE_FORMAT"
				+ ",TAppr.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TAppr.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TAppr.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TAppr.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TAppr.COL_EXPERSSION_TYPE_AT,TAppr.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TAppr.EXPERSSION_TEXT") + " EXPERSSION_TEXT"
				+ ",TAppr.PRIMARY_KEY_FLAG,TAppr.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TAppr.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TAppr.FEED_COLUMN_STATUS_NT,TAppr.FEED_COLUMN_STATUS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION From ETL_FEED_COLUMNS TAppr "
				+ " WHERE TAppr.COUNTRY = ?  AND TAppr.LE_BOOK = ?  AND TAppr.FEED_ID = ? ");
		String strQueryPend = new String("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.FEED_ID"
				+ ",TPend.SOURCE_CONNECTOR_ID,TPend.TABLE_ID,TPend.COLUMN_ID,TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_ALIAS_NAME,TPend.COLUMN_SORT_ORDER,TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE,TPend.COLUMN_LENGTH,TPend.DATE_FORMAT_NT,TPend.DATE_FORMAT"
				+ ",TPend.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TPend.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TPend.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TPend.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TPend.COL_EXPERSSION_TYPE_AT,TPend.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TPend.EXPERSSION_TEXT") + "  EXPERSSION_TEXT"
				+ ",TPend.PRIMARY_KEY_FLAG,TPend.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TPend.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TPend.FEED_COLUMN_STATUS_NT,TPend.FEED_COLUMN_STATUS,TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION From ETL_FEED_COLUMNS_PEND TPend "
				+ " WHERE TPend.COUNTRY = ?  AND TPend.LE_BOOK = ?  AND TPend.FEED_ID = ?");
		String strQueryUpl = new String("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.FEED_ID"
				+ ",TUpl.SOURCE_CONNECTOR_ID,TUpl.TABLE_ID,TUpl.COLUMN_ID,TUpl.COLUMN_NAME"
				+ ",TUpl.COLUMN_ALIAS_NAME,TUpl.COLUMN_SORT_ORDER,TUpl.COLUMN_DATATYPE_AT"
				+ ",TUpl.COLUMN_DATATYPE,TUpl.COLUMN_LENGTH,TUpl.DATE_FORMAT_NT,TUpl.DATE_FORMAT"
				+ ",TUpl.DATE_FORMAT FORMAT_TYPE"
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) FORMAT_TYPE_DESC "
				+ ",(SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE NUM_TAB = 2012 AND NUM_SUB_TAB = TUpl.DATE_FORMAT) DYNAMIC_DATE_FORMAT "
				+ ",(SELECT MACROVAR_DESC FROM MACROVAR_TAGGING WHERE MACROVAR_NAME = 'JAVA' AND TAG_NAME = TUpl.DATE_FORMAT) JAVA_DATE_FORMAT "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_FORMAT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_FORMATTING_SYNTAX "
				+ ",(select DISPLAY_NAME from MACROVAR_TAGGING where MACROVAR_NAME = 'ORACLE' AND MACROVAR_TYPE = 'DATE_CONVERT' AND TAG_NAME = TUpl.DATE_FORMAT) DATE_CONVERSION_SYNTAX"
				+ ",TUpl.COL_EXPERSSION_TYPE_AT,TUpl.COL_EXPERSSION_TYPE, "
				+ getDbFunction(Constants.TO_CHAR, "TUpl.EXPERSSION_TEXT") + "  EXPERSSION_TEXT"
				+ ",TUpl.PRIMARY_KEY_FLAG,TUpl.PARTITION_COLUMN_FLAG, "
				+ getDbFunction(Constants.TO_CHAR, "TUpl.FILTER_CONTEXT") + " FILTER_CONTEXT"
				+ ",TUpl.FEED_COLUMN_STATUS_NT,TUpl.FEED_COLUMN_STATUS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION From ETL_FEED_COLUMNS_UPL TUpl"
				+ " WHERE TUpl.COUNTRY = ?  AND TUpl.LE_BOOK = ?  AND TUpl.FEED_ID = ? ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getFeedId();

		if (ValidationUtil.isValid(dObj.getConnectorId())) {
			intKeyFieldsCount = 5;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = dObj.getCountry();
			objParams[1] = dObj.getLeBook();
			objParams[2] = dObj.getFeedId();
			objParams[3] = dObj.getTableId();
			objParams[4] = dObj.getConnectorId();

			strQueryUpl = strQueryUpl + "  AND  table_id =? and SOURCE_CONNECTOR_ID = ? ";
			strQueryAppr = strQueryAppr + " AND table_id =? and SOURCE_CONNECTOR_ID = ? ";
			strQueryPend = strQueryPend + " AND  table_id =? and SOURCE_CONNECTOR_ID = ? ";
		}

		String orderby = " ORDER BY COUNTRY, LE_BOOK, FEED_ID, TABLE_ID, COLUMN_ID ";
		strQueryUpl = strQueryUpl + orderby;
		strQueryAppr = strQueryAppr + orderby;
		strQueryPend = strQueryPend + orderby;

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
	protected List<ETLFeedColumnsVb> selectApprovedRecord(ETLFeedColumnsVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<ETLFeedColumnsVb> doSelectPendingRecord(ETLFeedColumnsVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<ETLFeedColumnsVb> doSelectUplRecord(ETLFeedColumnsVb vObject) {
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(ETLFeedColumnsVb records) {
		return records.getFeedColumnStatus();
	}

	@Override
	protected void setStatus(ETLFeedColumnsVb vObject, int status) {
		vObject.setFeedColumnStatus(status);
	}

	@Override
	protected int doInsertionAppr(ETLFeedColumnsVb vObject) {
		String query = "Insert Into ETL_FEED_COLUMNS (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, TABLE_ID, COLUMN_ID, COLUMN_NAME, "
				+ "COLUMN_ALIAS_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT,"
				+ "COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,   "
				+ "FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, EXPERSSION_TEXT, FILTER_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ", ?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getSourceConnectorId(), vObject.getTableId(), vObject.getColumnId(), vObject.getColumnName(),
				vObject.getColumnAliasName(), vObject.getColumnSortOrder(), vObject.getColumnDataTypeAT(),
				vObject.getColumnDataType(), vObject.getColumnLength(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColExperssionTypeAt(), vObject.getColExperssionType(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNT(),
				vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
		int result = 0;
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

					clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext() : "";
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
	protected int doInsertionPend(ETLFeedColumnsVb vObject) {
		String query = "Insert Into ETL_FEED_COLUMNS_PEND (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, TABLE_ID, COLUMN_ID, COLUMN_NAME, "
				+ "COLUMN_ALIAS_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT,"
				+ "COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,   "
				+ "FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, EXPERSSION_TEXT, FILTER_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ", ?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getSourceConnectorId(), vObject.getTableId(), vObject.getColumnId(), vObject.getColumnName(),
				vObject.getColumnAliasName(), vObject.getColumnSortOrder(), vObject.getColumnDataTypeAT(),
				vObject.getColumnDataType(), vObject.getColumnLength(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColExperssionTypeAt(), vObject.getColExperssionType(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNT(),
				vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
		int result = 0;
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

					clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext() : "";
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
	protected int doInsertionUpl(ETLFeedColumnsVb vObject) {
		String query = "Insert Into ETL_FEED_COLUMNS_UPL (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, TABLE_ID, COLUMN_ID, COLUMN_NAME, "
				+ "COLUMN_ALIAS_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT,"
				+ "COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,   "
				+ "FEED_COLUMN_STATUS_NT, FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, EXPERSSION_TEXT, FILTER_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ", ?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getSourceConnectorId(), vObject.getTableId(), vObject.getColumnId(), vObject.getColumnName(),
				vObject.getColumnAliasName(), vObject.getColumnSortOrder(), vObject.getColumnDataTypeAT(),
				vObject.getColumnDataType(), vObject.getColumnLength(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColExperssionTypeAt(), vObject.getColExperssionType(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNT(),
				vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };
		int result = 0;
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

					clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext() : "";
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
	protected int doInsertionPendWithDc(ETLFeedColumnsVb vObject) {
		String query = "Insert Into ETL_FEED_COLUMNS_PEND (COUNTRY, LE_BOOK, FEED_ID, SOURCE_CONNECTOR_ID, TABLE_ID, COLUMN_ID, "
				+ "COLUMN_NAME, COLUMN_ALIAS_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, "
				+ " COL_EXPERSSION_TYPE_AT, COL_EXPERSSION_TYPE, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,   FEED_COLUMN_STATUS_NT, "
				+ "FEED_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,EXPERSSION_TEXT, FILTER_CONTEXT)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " ,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getSourceConnectorId(), vObject.getTableId(), vObject.getColumnId(), vObject.getColumnName(),
				vObject.getColumnAliasName(), vObject.getColumnSortOrder(), vObject.getColumnDataTypeAT(),
				vObject.getColumnDataType(), vObject.getColumnLength(), vObject.getDateFormatNT(),
				vObject.getDateFormat(), vObject.getColExperssionTypeAt(), vObject.getColExperssionType(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFilterContext(),
				vObject.getFeedColumnStatusNT(), vObject.getFeedColumnStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		int result = 0;
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

					clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext() : "";
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
	protected int doUpdateAppr(ETLFeedColumnsVb vObject) {
		String query = "Update ETL_FEED_COLUMNS Set EXPERSSION_TEXT = ?, FILTER_CONTEXT = ?, SOURCE_CONNECTOR_ID = ?, COLUMN_NAME = ?, COLUMN_ALIAS_NAME = ?,"
				+ " COLUMN_SORT_ORDER = ?, COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, COLUMN_LENGTH = ?,"
				+ " DATE_FORMAT_NT = ?, DATE_FORMAT = ?,"
				+ " COL_EXPERSSION_TYPE_AT =?, COL_EXPERSSION_TYPE =? ,PRIMARY_KEY_FLAG = ?, "
				+ " PARTITION_COLUMN_FLAG = ?, FEED_COLUMN_STATUS_NT = ?, FEED_COLUMN_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,"
				+ " INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TABLE_ID = ?  AND COLUMN_ID = ? ";
		Object[] args = { vObject.getSourceConnectorId(), vObject.getColumnName(), vObject.getColumnAliasName(),
				vObject.getColumnSortOrder(), vObject.getColumnDataTypeAT(), vObject.getColumnDataType(),
				vObject.getColumnLength(), vObject.getDateFormatNT(), vObject.getDateFormat(),
				vObject.getColExperssionTypeAt(), vObject.getColExperssionType(), vObject.getPrimaryKeyFlag(),
				vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNT(), vObject.getFeedColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getTableId(), vObject.getColumnId() };

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

					clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext() : "";
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
	protected int doUpdatePend(ETLFeedColumnsVb vObject) {
		String query = "Update ETL_FEED_COLUMNS_PEND Set EXPERSSION_TEXT = ?, FILTER_CONTEXT = ?,SOURCE_CONNECTOR_ID = ?, COLUMN_NAME = ?, COLUMN_ALIAS_NAME = ?,"
				+ " COLUMN_SORT_ORDER = ?, "
				+ " COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, COLUMN_LENGTH = ?, DATE_FORMAT_NT = ?, DATE_FORMAT = ?,"
				+ " COL_EXPERSSION_TYPE_AT =?,  COL_EXPERSSION_TYPE =? ,PRIMARY_KEY_FLAG = ?, "
				+ " PARTITION_COLUMN_FLAG = ?,+ FEED_COLUMN_STATUS_NT = ?, FEED_COLUMN_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,"
				+ " INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  AND TABLE_ID = ?  AND COLUMN_ID = ? ";

		Object[] args = { vObject.getSourceConnectorId(), vObject.getColumnName(), vObject.getColumnAliasName(),
				vObject.getColumnSortOrder(), vObject.getColumnDataTypeAT(), vObject.getColumnDataType(),
				vObject.getColumnLength(), vObject.getDateFormatNT(), vObject.getDateFormat(),
				vObject.getColExperssionTypeAt(), vObject.getColExperssionType(), vObject.getPrimaryKeyFlag(),
				vObject.getPartitionColumnFlag(), vObject.getFeedColumnStatusNT(), vObject.getFeedColumnStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(),
				vObject.getTableId(), vObject.getColumnId() };

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

					clobData = ValidationUtil.isValid(vObject.getFilterContext()) ? vObject.getFilterContext() : "";
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
	protected int doDeleteAppr(ETLFeedColumnsVb vObject) {
		String query = "Delete From ETL_FEED_COLUMNS Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(ETLFeedColumnsVb vObject) {
		String query = "Delete From ETL_FEED_COLUMNS_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?   ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deleteUplRecord(ETLFeedColumnsVb vObject) {
		String query = "Delete From ETL_FEED_COLUMNS_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_COLUMNS";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_COLUMNS_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_COLUMNS_PEND";
		}
		String query = "Delete From " + table + " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_COLUMNS_PRS";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_FEED_COLUMNS";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_COLUMNS_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_COLUMNS_PEND";
		}
		String query = "update " + table
				+ " set FEED_COLUMN_STATUS = ? ,RECORD_INDICATOR =? Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(ETLFeedColumnsVb vObject) {
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");
		if (ValidationUtil.isValid(vObject.getCountry()))
			strAudit.append("COUNTRY" + auditDelimiterColVal + vObject.getCountry().trim());
		else
			strAudit.append("COUNTRY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLeBook()))
			strAudit.append("LE_BOOK" + auditDelimiterColVal + vObject.getLeBook().trim());
		else
			strAudit.append("LE_BOOK" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFeedId()))
			strAudit.append("FEED_ID" + auditDelimiterColVal + vObject.getFeedId().trim());
		else
			strAudit.append("FEED_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSourceConnectorId()))
			strAudit.append("SOURCE_CONNECTOR_ID" + auditDelimiterColVal + vObject.getSourceConnectorId().trim());
		else
			strAudit.append("SOURCE_CONNECTOR_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getTableId()))
			strAudit.append("TABLE_ID" + auditDelimiterColVal + vObject.getTableId().trim());
		else
			strAudit.append("TABLE_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColumnId()))
			strAudit.append("COLUMN_ID" + auditDelimiterColVal + vObject.getColumnId().trim());
		else
			strAudit.append("COLUMN_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColumnName()))
			strAudit.append("COLUMN_NAME" + auditDelimiterColVal + vObject.getColumnName().trim());
		else
			strAudit.append("COLUMN_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColumnAliasName()))
			strAudit.append("COLUMN_ALIAS_NAME" + auditDelimiterColVal + vObject.getColumnAliasName().trim());
		else
			strAudit.append("COLUMN_ALIAS_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COLUMN_SORT_ORDER" + auditDelimiterColVal + vObject.getColumnSortOrder());
		strAudit.append(auditDelimiter);

		strAudit.append("COLUMN_DATATYPE_AT" + auditDelimiterColVal + vObject.getColumnDataTypeAT());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColumnDataType()))
			strAudit.append("COLUMN_DATATYPE" + auditDelimiterColVal + vObject.getColumnDataType().trim());
		else
			strAudit.append("COLUMN_DATATYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COLUMN_LENGTH" + auditDelimiterColVal + vObject.getColumnLength());
		strAudit.append(auditDelimiter);

		strAudit.append("DATE_FORMAT_NT" + auditDelimiterColVal + vObject.getDateFormatNT());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateFormat()))
			strAudit.append("DATE_FORMAT" + auditDelimiterColVal + vObject.getDateFormat().trim());
		else
			strAudit.append("DATE_FORMAT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COL_EXPERSSION_TYPE_AT" + auditDelimiterColVal + vObject.getColExperssionTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColExperssionType()))
			strAudit.append("COL_EXPERSSION_TYPE" + auditDelimiterColVal + vObject.getColExperssionType().trim());
		else
			strAudit.append("COL_EXPERSSION_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getExperssionText()))
			strAudit.append("EXPERSSION_TEXT" + auditDelimiterColVal + vObject.getExperssionText().trim());
		else
			strAudit.append("EXPERSSION_TEXT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getPrimaryKeyFlag()))
			strAudit.append("PRIMARY_KEY_FLAG" + auditDelimiterColVal + vObject.getPrimaryKeyFlag().trim());
		else
			strAudit.append("PRIMARY_KEY_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getPartitionColumnFlag()))
			strAudit.append("PARTITION_COLUMN_FLAG" + auditDelimiterColVal + vObject.getPartitionColumnFlag().trim());
		else
			strAudit.append("PARTITION_COLUMN_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFilterContext()))
			strAudit.append("FILTER_CONTEXT" + auditDelimiterColVal + vObject.getFilterContext().trim());
		else
			strAudit.append("FILTER_CONTEXT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_COLUMN_STATUS_NT" + auditDelimiterColVal + vObject.getFeedColumnStatusNT());
		strAudit.append(auditDelimiter);

		strAudit.append("FEED_COLUMN_STATUS" + auditDelimiterColVal + vObject.getFeedColumnStatus());
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

		return strAudit.toString();
	}

	@Override
	public void setServiceDefaults() {
		serviceName = "EtlFeedColumns";
		serviceDesc = "ETL Feed Columns";
		tableName = "ETL_FEED_COLUMNS";
		childTableName = "ETL_FEED_COLUMNS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_FEED_COLUMNS_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_COLUMNS_PRS.FEED_ID AND "
				+ "ETL_FEED_MAIN_PRS.SESSION_ID = ETL_FEED_COLUMNS_PRS.SESSION_ID) ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

}
