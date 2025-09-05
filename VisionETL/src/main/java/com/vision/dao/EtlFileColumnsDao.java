package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFileColumnsVb;

@Component
public class EtlFileColumnsDao extends AbstractDao<EtlFileColumnsVb> {

	/******* Mapper Start **********/

	String ColumnDatatypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2014, "TAppr.COLUMN_DATATYPE",
			"COLUMN_DATATYPE_DESC");
	String ColumnDatatypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2014, "TPend.COLUMN_DATATYPE",
			"COLUMN_DATATYPE_DESC");

	String DateFormatNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1102, "TAppr.DATE_FORMAT",
			"DATE_FORMAT_DESC");
	String DateFormatNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1102, "TPend.DATE_FORMAT",
			"DATE_FORMAT_DESC");

	String NumberFormatNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TAppr.NUMBER_FORMAT",
			"NUMBER_FORMAT_DESC");
	String NumberFormatNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TPend.NUMBER_FORMAT",
			"NUMBER_FORMAT_DESC");

	String FileColumnStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.FILE_COLUMN_STATUS",
			"FILE_COLUMN_STATUS_DESC");
	String FileColumnStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.FILE_COLUMN_STATUS",
			"FILE_COLUMN_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFileColumnsVb vObject = new EtlFileColumnsVb();
				vObject.setViewName(ValidationUtil.isValid(rs.getString("VIEW_NAME"))?rs.getString("VIEW_NAME"):"");
				vObject.setConnectorId(ValidationUtil.isValid(rs.getString("CONNECTOR_ID"))?rs.getString("CONNECTOR_ID"):"");
				vObject.setColumnId(rs.getInt("COLUMN_ID"));
				vObject.setColumnName(ValidationUtil.isValid(rs.getString("COLUMN_NAME"))?rs.getString("COLUMN_NAME"):"");
				vObject.setColumnSortOrder(rs.getInt("COLUMN_SORT_ORDER"));
				vObject.setColumnDatatypeAt(rs.getInt("COLUMN_DATATYPE_AT"));
				vObject.setColumnDatatype(ValidationUtil.isValid(rs.getString("COLUMN_DATATYPE"))?rs.getString("COLUMN_DATATYPE"):"");
				vObject.setColumnLength(rs.getInt("COLUMN_LENGTH"));
				vObject.setDateFormatNt(rs.getInt("DATE_FORMAT_NT"));
				vObject.setDateFormat(ValidationUtil.isValid(rs.getString("DATE_FORMAT"))?rs.getString("DATE_FORMAT"):"");
				vObject.setNumberFormatNt(rs.getInt("NUMBER_FORMAT_NT"));
				vObject.setNumberFormat(ValidationUtil.isValid(rs.getString("NUMBER_FORMAT"))?rs.getString("NUMBER_FORMAT"):"");
				vObject.setPrimaryKeyFlag(ValidationUtil.isValid(rs.getString("PRIMARY_KEY_FLAG"))?rs.getString("PRIMARY_KEY_FLAG"):"");
				vObject.setPartitionColumnFlag(ValidationUtil.isValid(rs.getString("PARTITION_COLUMN_FLAG"))?rs.getString("PARTITION_COLUMN_FLAG"):"");
				vObject.setFileColumnStatusNt(rs.getInt("FILE_COLUMN_STATUS_NT"));
				vObject.setFileColumnStatus(rs.getInt("FILE_COLUMN_STATUS"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				vObject.setDateLastModified(ValidationUtil.isValid(rs.getString("DATE_LAST_MODIFIED"))?rs.getString("DATE_LAST_MODIFIED"):"");
				vObject.setDateCreation(ValidationUtil.isValid(rs.getString("DATE_CREATION"))?rs.getString("DATE_CREATION"):"");
				return vObject;
			}
		};
		return mapper;
	}

	/******* Mapper End **********/
	public List<EtlFileColumnsVb> getQueryPopupResults(EtlFileColumnsVb dObj) {

		Vector<Object> params = new Vector<Object>();
		
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.CONNECTOR_ID"
				+ ",TAppr.VIEW_NAME"
				+ ",TAppr.COLUMN_ID"
				+ ",TAppr.COLUMN_NAME"
				+ ",TAppr.COLUMN_SORT_ORDER"
				+ ",TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE"
				+ ",TAppr.COLUMN_LENGTH"
				+ ",TAppr.DATE_FORMAT_NT"
				+ ",TAppr.DATE_FORMAT"
				+ ",TAppr.NUMBER_FORMAT_NT"
				+ ",TAppr.NUMBER_FORMAT"
				+ ",TAppr.PRIMARY_KEY_FLAG"
				+ ",TAppr.PARTITION_COLUMN_FLAG"
				+ ",TAppr.FILE_COLUMN_STATUS_NT"
				+ ",TAppr.FILE_COLUMN_STATUS"
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
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION from ETL_FILE_COLUMNS TAppr ");
			String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FILE_COLUMNS_PEND TPend WHERE TAppr.CONNECTOR_ID = TPend.CONNECTOR_ID AND TAppr.VIEW_NAME = TPend.VIEW_NAME AND TAppr.COLUMN_ID = TPend.COLUMN_ID )");
			StringBuffer strBufPending = new StringBuffer("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.COLUMN_ID"
				+ ",TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_SORT_ORDER"
				+ ",TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE"
				+ ",TPend.COLUMN_LENGTH"
				+ ",TPend.DATE_FORMAT_NT"
				+ ",TPend.DATE_FORMAT"
				+ ",TPend.NUMBER_FORMAT_NT"
				+ ",TPend.NUMBER_FORMAT"
				+ ",TPend.PRIMARY_KEY_FLAG"
				+ ",TPend.PARTITION_COLUMN_FLAG"
				+ ",TPend.FILE_COLUMN_STATUS_NT"
				+ ",TPend.FILE_COLUMN_STATUS"
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
				+ " from ETL_FILE_COLUMNS_PEND TPend ");
		
		try {
			if (ValidationUtil.isValid(dObj.getConnectorId())) {
				params.addElement(dObj.getConnectorId());
				CommonUtils.addToQuery("UPPER(TAppr.CONNECTOR_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.CONNECTOR_ID) = ?", strBufPending);
			}
			if (ValidationUtil.isValid(dObj.getViewName())) {
				params.addElement(dObj.getViewName().toUpperCase());
				CommonUtils.addToQuery("UPPER(TAppr.VIEW_NAME) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.VIEW_NAME) = ?", strBufPending);
			}
			String orderBy = "ORDER BY CONNECTOR_ID, VIEW_NAME, COLUMN_ID";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);

		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;

		}
	}

	public List<EtlFileColumnsVb> getQueryResults(EtlFileColumnsVb dObj, int intStatus) {

		setServiceDefaults();
		List<EtlFileColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID"
				+ ",TAppr.VIEW_NAME"
				+ ",TAppr.COLUMN_ID"
				+ ",TAppr.COLUMN_NAME"
				+ ",TAppr.COLUMN_SORT_ORDER"
				+ ",TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE"
				+ ",TAppr.COLUMN_LENGTH"
				+ ",TAppr.DATE_FORMAT_NT"
				+ ",TAppr.DATE_FORMAT"
				+ ",TAppr.NUMBER_FORMAT_NT"
				+ ",TAppr.NUMBER_FORMAT"
				+ ",TAppr.PRIMARY_KEY_FLAG"
				+ ",TAppr.PARTITION_COLUMN_FLAG"
				+ ",TAppr.FILE_COLUMN_STATUS_NT"
				+ ",TAppr.FILE_COLUMN_STATUS"
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
				+ " From ETL_FILE_COLUMNS TAppr WHERE UPPER(TAPPR.CONNECTOR_ID) = ?  AND UPPER(TAPPR.VIEW_NAME) = ?  AND COLUMN_ID = ?  ");
			String strQueryPend = new String("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.COLUMN_ID"
				+ ",TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_SORT_ORDER"
				+ ",TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE"
				+ ",TPend.COLUMN_LENGTH"
				+ ",TPend.DATE_FORMAT_NT"
				+ ",TPend.DATE_FORMAT"
				+ ",TPend.NUMBER_FORMAT_NT"
				+ ",TPend.NUMBER_FORMAT"
				+ ",TPend.PRIMARY_KEY_FLAG"
				+ ",TPend.PARTITION_COLUMN_FLAG"
				+ ",TPend.FILE_COLUMN_STATUS_NT"
				+ ",TPend.FILE_COLUMN_STATUS"
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
				+ " From ETL_FILE_COLUMNS_PEND TPend WHERE  UPPER(TPEND.CONNECTOR_ID) = ?  AND  UPPER(TPEND.VIEW_NAME) = ?  AND COLUMN_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getConnectorId().toUpperCase();
		objParams[1] = dObj.getViewName().toUpperCase();
		objParams[2] = dObj.getColumnId();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 0) {
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
	protected List<EtlFileColumnsVb> selectApprovedRecord(EtlFileColumnsVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFileColumnsVb> doSelectPendingRecord(EtlFileColumnsVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlFileColumnsVb records) {
		return records.getFileColumnStatus();
	}

	@Override
	protected void setStatus(EtlFileColumnsVb vObject, int status) {
		vObject.setFileColumnStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFileColumnsVb vObject) {
		String query = "Insert Into ETL_FILE_COLUMNS (CONNECTOR_ID, VIEW_NAME, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FILE_COLUMN_STATUS_NT, FILE_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getColumnId(),
				vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFileColumnStatusNt(),
				vObject.getFileColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlFileColumnsVb vObject) {
		String query = "Insert Into ETL_FILE_COLUMNS_PEND (CONNECTOR_ID, VIEW_NAME, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FILE_COLUMN_STATUS_NT, FILE_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+getDbFunction(Constants.SYSDATE, null)+", "+getDbFunction(Constants.SYSDATE, null)+")";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getColumnId(),
				vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFileColumnStatusNt(),
				vObject.getFileColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFileColumnsVb vObject) {
		String query = "Insert Into ETL_FILE_COLUMNS_PEND (CONNECTOR_ID, VIEW_NAME, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, "
				+ "COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, FILE_COLUMN_STATUS_NT, "
				+ " FILE_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getColumnId(),
				vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFileColumnStatusNt(),
				vObject.getFileColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFileColumnsVb vObject) {
		String query = "Update ETL_FILE_COLUMNS Set COLUMN_NAME = ?, COLUMN_SORT_ORDER = ?, COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, COLUMN_LENGTH = ?, "
				+ " DATE_FORMAT_NT = ?, DATE_FORMAT = ?, NUMBER_FORMAT_NT = ?, NUMBER_FORMAT = ?, PRIMARY_KEY_FLAG = ?, PARTITION_COLUMN_FLAG = ?, "
				+ " FILE_COLUMN_STATUS_NT = ?, FILE_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,"
				+ " INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ "  WHERE CONNECTOR_ID = ?  AND VIEW_NAME = ?  AND COLUMN_ID = ? ";
		Object[] args = { vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFileColumnStatusNt(),
				vObject.getFileColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getConnectorId(),
				vObject.getViewName(), vObject.getColumnId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFileColumnsVb vObject) {
		String query = "Update ETL_FILE_COLUMNS_PEND Set COLUMN_NAME = ?, COLUMN_SORT_ORDER = ?, COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?,"
				+ " COLUMN_LENGTH = ?, DATE_FORMAT_NT = ?, DATE_FORMAT = ?, NUMBER_FORMAT_NT = ?, NUMBER_FORMAT = ?, PRIMARY_KEY_FLAG = ?,"
				+ " PARTITION_COLUMN_FLAG = ?, FILE_COLUMN_STATUS_NT = ?, FILE_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?,"
				+ " RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + ""
				+ " WHERE CONNECTOR_ID = ?  AND VIEW_NAME = ?  AND COLUMN_ID = ? ";
		Object[] args = { vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getFileColumnStatusNt(),
				vObject.getFileColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getConnectorId(),
				vObject.getViewName(), vObject.getColumnId() };

		return getJdbcTemplate().update(query, args);
	}
	protected int doDeleteApprAll(String connectorId) {
		String query = "Delete From ETL_FILE_COLUMNS Where CONNECTOR_ID = ?     ";
		Object[] args = { connectorId};
		return getJdbcTemplate().update(query, args);
	}
	protected int deletePendingRecordAll(String connectorId) {
		String query = "Delete From ETL_FILE_COLUMNS_PEND Where CONNECTOR_ID = ? ";
		Object[] args = {connectorId};
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFileColumnsVb vObject) {
		String query = "Delete From ETL_FILE_COLUMNS Where CONNECTOR_ID = ?  AND VIEW_NAME = ?  AND COLUMN_ID = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getColumnId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFileColumnsVb vObject) {
		String query = "Delete From ETL_FILE_COLUMNS_PEND Where CONNECTOR_ID = ?  AND VIEW_NAME = ?  AND COLUMN_ID = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getColumnId() };
		return getJdbcTemplate().update(query, args);
	}
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FILE_COLUMNS_PRS";
		String query = "Delete From "+table+" Where FEED_CATEGORY =?  AND FEED_ID = ? AND  SESSION_ID=? ";
		Object[] args = {   vObject.getFeedCategory(), vObject.getFeedId(), vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected String getAuditString(EtlFileColumnsVb vObject) {
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");
		if (ValidationUtil.isValid(vObject.getConnectorId()))
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + vObject.getConnectorId().trim());
		else
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getViewName()))
			strAudit.append("VIEW_NAME" + auditDelimiterColVal + vObject.getViewName().trim());
		else
			strAudit.append("VIEW_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COLUMN_ID" + auditDelimiterColVal + vObject.getColumnId());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColumnName()))
			strAudit.append("COLUMN_NAME" + auditDelimiterColVal + vObject.getColumnName().trim());
		else
			strAudit.append("COLUMN_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COLUMN_SORT_ORDER" + auditDelimiterColVal + vObject.getColumnSortOrder());
		strAudit.append(auditDelimiter);

		strAudit.append("COLUMN_DATATYPE_AT" + auditDelimiterColVal + vObject.getColumnDatatypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getColumnDatatype()))
			strAudit.append("COLUMN_DATATYPE" + auditDelimiterColVal + vObject.getColumnDatatype().trim());
		else
			strAudit.append("COLUMN_DATATYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("COLUMN_LENGTH" + auditDelimiterColVal + vObject.getColumnLength());
		strAudit.append(auditDelimiter);

		strAudit.append("DATE_FORMAT_NT" + auditDelimiterColVal + vObject.getDateFormatNt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateFormat()))
			strAudit.append("DATE_FORMAT" + auditDelimiterColVal + vObject.getDateFormat().trim());
		else
			strAudit.append("DATE_FORMAT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("NUMBER_FORMAT_NT" + auditDelimiterColVal + vObject.getNumberFormatNt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getNumberFormat()))
			strAudit.append("NUMBER_FORMAT" + auditDelimiterColVal + vObject.getNumberFormat().trim());
		else
			strAudit.append("NUMBER_FORMAT" + auditDelimiterColVal + "NULL");
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

		strAudit.append("FILE_COLUMN_STATUS_NT" + auditDelimiterColVal + vObject.getFileColumnStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("FILE_COLUMN_STATUS" + auditDelimiterColVal + vObject.getFileColumnStatus());
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
	protected void setServiceDefaults() {
		serviceName = "EtlFileColumns";
		serviceDesc = "ETL File Columns";
		tableName = "ETL_FILE_COLUMNS";
		childTableName = "ETL_FILE_COLUMNS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}
	public List<EtlFileColumnsVb> getQueryResultsByParent(String ConnectorID, int intStatus, String viewName) {

		setServiceDefaults();
		List<EtlFileColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 2;
		
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID"
				+ ",TAppr.VIEW_NAME"
				+ ",TAppr.COLUMN_ID"
				+ ",TAppr.COLUMN_NAME"
				+ ",TAppr.COLUMN_SORT_ORDER"
				+ ",TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE"
				+ ",TAppr.COLUMN_LENGTH"
				+ ",TAppr.DATE_FORMAT_NT"
				+ ",TAppr.DATE_FORMAT"
				+ ",TAppr.NUMBER_FORMAT_NT"
				+ ",TAppr.NUMBER_FORMAT"
				+ ",TAppr.PRIMARY_KEY_FLAG"
				+ ",TAppr.PARTITION_COLUMN_FLAG"
				+ ",TAppr.FILE_COLUMN_STATUS_NT"
				+ ",TAppr.FILE_COLUMN_STATUS"
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
				+ " From ETL_FILE_COLUMNS TAppr WHERE UPPER(TAPPR.CONNECTOR_ID) = ?  AND UPPER(TAPPR.VIEW_NAME) = ?  ");
			String strQueryPend = new String("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.COLUMN_ID"
				+ ",TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_SORT_ORDER"
				+ ",TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE"
				+ ",TPend.COLUMN_LENGTH"
				+ ",TPend.DATE_FORMAT_NT"
				+ ",TPend.DATE_FORMAT"
				+ ",TPend.NUMBER_FORMAT_NT"
				+ ",TPend.NUMBER_FORMAT"
				+ ",TPend.PRIMARY_KEY_FLAG"
				+ ",TPend.PARTITION_COLUMN_FLAG"
				+ ",TPend.FILE_COLUMN_STATUS_NT"
				+ ",TPend.FILE_COLUMN_STATUS"
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
				+ " From ETL_FILE_COLUMNS_PEND TPend WHERE  UPPER(TPEND.CONNECTOR_ID) = ?  AND UPPER(TPEND.VIEW_NAME) = ? ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = ConnectorID.toUpperCase();
		objParams[1] = viewName.toUpperCase();

		try {
			if (intStatus == 0) {
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
	public List<EtlFileColumnsVb> getQueryResultsByParentAppr(String ConnectorID, int intStatus) {

		setServiceDefaults();
		List<EtlFileColumnsVb> collTemp = null;
		final int intKeyFieldsCount = 1;
		
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID"
				+ ",TAppr.VIEW_NAME"
				+ ",TAppr.COLUMN_ID"
				+ ",TAppr.COLUMN_NAME"
				+ ",TAppr.COLUMN_SORT_ORDER"
				+ ",TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE"
				+ ",TAppr.COLUMN_LENGTH"
				+ ",TAppr.DATE_FORMAT_NT"
				+ ",TAppr.DATE_FORMAT"
				+ ",TAppr.NUMBER_FORMAT_NT"
				+ ",TAppr.NUMBER_FORMAT"
				+ ",TAppr.PRIMARY_KEY_FLAG"
				+ ",TAppr.PARTITION_COLUMN_FLAG"
				+ ",TAppr.FILE_COLUMN_STATUS_NT"
				+ ",TAppr.FILE_COLUMN_STATUS"
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
				+ " From ETL_FILE_COLUMNS TAppr WHERE UPPER(TAPPR.CONNECTOR_ID) = ?  ");
			String strQueryPend = new String("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.COLUMN_ID"
				+ ",TPend.COLUMN_NAME"
				+ ",TPend.COLUMN_SORT_ORDER"
				+ ",TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE"
				+ ",TPend.COLUMN_LENGTH"
				+ ",TPend.DATE_FORMAT_NT"
				+ ",TPend.DATE_FORMAT"
				+ ",TPend.NUMBER_FORMAT_NT"
				+ ",TPend.NUMBER_FORMAT"
				+ ",TPend.PRIMARY_KEY_FLAG"
				+ ",TPend.PARTITION_COLUMN_FLAG"
				+ ",TPend.FILE_COLUMN_STATUS_NT"
				+ ",TPend.FILE_COLUMN_STATUS"
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
				+ " From ETL_FILE_COLUMNS_PEND TPend WHERE  UPPER(TPEND.CONNECTOR_ID) = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = ConnectorID.toUpperCase();

		try {
			if (intStatus == 0) {
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
	
	
	@Transactional(rollbackForClassName={"com.vision.exception.RuntimeCustomException"})
	protected ExceptionCode doInsertRecordForNonTrans(EtlFileColumnsVb vObject) throws RuntimeCustomException{
		List<EtlFileColumnsVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc  = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		/*if (collTemp == null){
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0 ){
			int staticDeletionFlag = getStatus(((ArrayList<EtlFileColumnsVb>)collTemp).get(0));
			if (staticDeletionFlag == Constants.PASSIVATE){
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			}
			else
			{
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}*/

		// Try to see if the record already exists in the pending table, but not in approved table
		collTemp = null;
		collTemp = doSelectPendingRecord(vObject);

		// The collTemp variable could not be null.  If so, there is no problem fetching data
		// return back error code to calling routine
		if (collTemp == null){
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// if record already exists in pending table, modify the record
		if (collTemp.size() > 0){
			EtlFileColumnsVb vObjectLocal = ((ArrayList<EtlFileColumnsVb>)collTemp).get(0);
			if (vObjectLocal.getRecordIndicator() == Constants.STATUS_INSERT){
				exceptionCode = getResultObject(Constants.PENDING_FOR_ADD_ALREADY);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}else{
			// Try inserting the record
			/*vObject.setRecordIndicator(Constants.STATUS_INSERT);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
			vObject.setDateCreation(systemDate);*/
			retVal = doInsertionPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION){
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(retVal);
			return exceptionCode;
		}
	}

	public ExceptionCode doApproveRecord(EtlFileColumnsVb vObject, boolean staticDelete) throws RuntimeCustomException {
		EtlFileColumnsVb oldContents = null;
		EtlFileColumnsVb vObjectlocal = null;
		List<EtlFileColumnsVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		try {
			if("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))){
				exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING_APPROVE);
				throw buildRuntimeCustomException(exceptionCode);
			}
			// See if such a pending request exists in the pending table
			vObject.setVerifier(getIntCurrentUserId());
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			collTemp = doSelectPendingRecord(vObject);
			if (collTemp == null){
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			if (collTemp.size() == 0){
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}

			vObjectlocal = ((ArrayList<EtlFileColumnsVb>)collTemp).get(0);

			if (vObjectlocal.getMaker() == getIntCurrentUserId()){
				exceptionCode = getResultObject(Constants.MAKER_CANNOT_APPROVE);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// If it's NOT addition, collect the existing record contents from the
			// Approved table and keep it aside, for writing audit information later.
			if (vObjectlocal.getRecordIndicator() != Constants.STATUS_INSERT){
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null || collTemp.isEmpty()){
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				oldContents = ((ArrayList<EtlFileColumnsVb>)collTemp).get(0);
			}

			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT){  // Add authorization
				// Write the contents of the Pending table record to the Approved table
				vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
				vObjectlocal.setVerifier(getIntCurrentUserId());
				retVal = doInsertionAppr(vObjectlocal);
				if (retVal != Constants.SUCCESSFUL_OPERATION){
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}

				String systemDate = getSystemDate();
				vObject.setDateLastModified(systemDate);
				vObject.setDateCreation(systemDate);
				strApproveOperation = Constants.ADD;
			}else if (vObjectlocal.getRecordIndicator() == Constants.STATUS_UPDATE){ // Modify authorization
			
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null || collTemp.isEmpty()){
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}	

				// If record already exists in the approved table, reject the addition
				if (collTemp.size() > 0 ){
					//retVal = doUpdateAppr(vObjectlocal, MISConstants.ACTIVATE);
					vObjectlocal.setVerifier(getIntCurrentUserId());
					vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
					retVal = doUpdateAppr(vObjectlocal);
				}
				// Modify the existing contents of the record in Approved table
				if (retVal != Constants.SUCCESSFUL_OPERATION){
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
				String systemDate = getSystemDate();
				vObject.setDateLastModified(systemDate);
				// Set the current operation to write to audit log
				strApproveOperation = Constants.MODIFY;
			}
			else if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE){ // Delete authorization
				if(staticDelete){
					// Update the existing record status in the Approved table to delete 
					setStatus(vObjectlocal, Constants.PASSIVATE);
					vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
					vObjectlocal.setVerifier(getIntCurrentUserId());
					retVal = doUpdateAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION){
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					setStatus(vObject, Constants.PASSIVATE);
					String systemDate = getSystemDate();
					vObject.setDateLastModified(systemDate);

				}else{
					// Delete the existing record from the Approved table 
					retVal = doDeleteAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION){
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					String systemDate = getSystemDate();
					vObject.setDateLastModified(systemDate);
				}
				// Set the current operation to write to audit log
				strApproveOperation = Constants.DELETE;
			}
			else{
				exceptionCode = getResultObject(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
				throw buildRuntimeCustomException(exceptionCode);
			}	

			// Delete the record from the Pending table
			retVal = deletePendingRecord(vObjectlocal);

			if (retVal != Constants.SUCCESSFUL_OPERATION){
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Set the internal status to Approved
			vObject.setInternalStatus(0);
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE && !staticDelete){
				exceptionCode = writeAuditLog(null, oldContents);
				vObject.setRecordIndicator(-1);
			} else {
//				exceptionCode = writeAuditLog(vObjectlocal, oldContents);
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}

			if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION){
				exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
				throw buildRuntimeCustomException(exceptionCode);
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		}catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}catch(Exception ex){
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String table = "ETL_FILE_COLUMNS_PRS";
		String query = "Delete From "+table+" Where FEED_CATEGORY =?  AND  SESSION_ID=? ";
		Object[] args = {   vObject.getFeedCategory(), vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
}
