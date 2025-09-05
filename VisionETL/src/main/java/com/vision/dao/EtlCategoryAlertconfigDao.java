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
import com.vision.vb.EtlFeedCategoryAlertConfigVb;
import com.vision.vb.EtlFeedMainVb;

@Component
public class EtlCategoryAlertconfigDao extends AbstractDao<EtlFeedCategoryAlertConfigVb> {

	/******* Mapper Start **********/

	String FileStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.CATEGORY_ALERT_STATUS",
			"FILE_STATUS_DESC");
	String FileStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.CATEGORY_ALERT_STATUS",
			"FILE_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	String EventTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2130, "TAppr.EVENT_TYPE",
			"EVENT_TYPE_DESC");
	String EventTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2130, "TPend.EVENT_TYPE",
			"EVENT_TYPE_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryAlertConfigVb vObject = new EtlFeedCategoryAlertConfigVb();
				vObject.setCountry(rs.getString("COUNTRY"));
				vObject.setLeBook(rs.getString("LE_BOOK"));
				vObject.setCategoryId(rs.getString("CATEGORY_ID"));
				vObject.setEventType(rs.getString("EVENT_TYPE"));
				vObject.setEventTypeDesc(rs.getString("EVENT_TYPE_DESC"));
				vObject.setMatrixDesc(rs.getString("MATRIX_dESCRIPTION"));
				vObject.setMatrixID(rs.getString("MATRIX_ID"));
				vObject.setCategoryAlertStatusNt(rs.getInt("CATEGORY_ALERT_STATUS_NT"));
				vObject.setCategoryAlertStatus(rs.getInt("CATEGORY_ALERT_STATUS"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				vObject.setDateLastModified(
						ValidationUtil.isValid(rs.getString("DATE_LAST_MODIFIED")) ? rs.getString("DATE_LAST_MODIFIED")
								: "");
				vObject.setDateCreation(
						ValidationUtil.isValid(rs.getString("DATE_CREATION")) ? rs.getString("DATE_CREATION") : "");
				return vObject;
			}
		};
		return mapper;
	}

	/******* Mapper End **********/
	public List<EtlFeedCategoryAlertConfigVb> getQueryPopupResults(EtlFeedCategoryAlertConfigVb dObj) {

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("SELECT TAPPR.COUNTRY, TAPPR.LE_BOOK"
				+ ",TAPPR.CATEGORY_ID,TAPPR.MATRIX_ID,TAPPR.EVENT_TYPE,TAPPR.MATRIX_ID"
				+ ",TAPPR.CATEGORY_ALERT_STATUS_NT,TAPPR.CATEGORY_ALERT_STATUS,TAPPR.RECORD_INDICATOR_NT"
				+ ",TAPPR.RECORD_INDICATOR," + RecordIndicatorNtApprDesc + ",TAPPR.MAKER," + makerApprDesc
				+ ",TAPPR.VERIFIER," + verifierApprDesc + ",TAPPR.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_CATEGORY_ALERT_CONFIG TAPPR ");

		StringBuffer strBufPending = new StringBuffer("SELECT TPEND.COUNTRY, TPEND.LE_BOOK"
				+ ",TPEND.CATEGORY_ID,TPEND.MATRIX_ID,TPEND.EVENT_TYPE,TPEND.MATRIX_ID"
				+ ",TPEND.CATEGORY_ALERT_STATUS_NT,TPEND.CATEGORY_ALERT_STATUS,TPEND.RECORD_INDICATOR_NT"
				+ ",TPEND.RECORD_INDICATOR," + RecordIndicatorNtPendDesc + ",TPEND.MAKER," + makerPendDesc
				+ ",TPEND.VERIFIER," + verifierPendDesc + ",TPEND.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_CATEGORY_ALERT_CONFIG_PEND TPEND ");

		if (ValidationUtil.isValid(dObj.getCategoryId())) {
			params.addElement(dObj.getCategoryId().toUpperCase());
			CommonUtils.addToQuery("UPPER(TAPPR.CATEGORY_ID) = ?", strBufApprove);
			CommonUtils.addToQuery("UPPER(TPEND.CATEGORY_ID) = ?", strBufPending);
		}
		String strWhereNotExists = new String(
				" NOT EXISTS (SELECT 'X' FROM ETL_CATEGORY_ALERT_CONFIG_PEND TPEND WHERE TAPPR.COUNTRY = TPEND.COUNTRY AND TAPPR.LE_BOOK = TPEND.LE_BOOK AND TAPPR.CATEGORY_ID = TPEND.CATEGORY_ID )");
		try {
			String orderBy = " Order By COUNTRY, LE_BOOK, CATEGORY_ID  ";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlFeedCategoryAlertConfigVb> getQueryResults(EtlFeedCategoryAlertConfigVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;

		final int intKeyFieldsCount = 3;
		Vector<Object> params = new Vector<Object>();
		StringBuffer strQueryAppr = new StringBuffer("SELECT TAPPR.COUNTRY, TAPPR.LE_BOOK"
				+ ",TAPPR.CATEGORY_ID,TAPPR.MATRIX_ID, T2.MATRIX_dESCRIPTION, TAPPR.EVENT_TYPE," + EventTypeAtApprDesc
				+ ",TAPPR.MATRIX_ID"
				+ ",TAPPR.CATEGORY_ALERT_STATUS_NT,TAPPR.CATEGORY_ALERT_STATUS,TAPPR.RECORD_INDICATOR_NT"
				+ ",TAPPR.RECORD_INDICATOR," + RecordIndicatorNtApprDesc + ",TAPPR.MAKER," + makerApprDesc
				+ ",TAPPR.VERIFIER," + verifierApprDesc + ",TAPPR.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_CATEGORY_ALERT_CONFIG TAPPR INNER JOIN ETL_ALERT_MATRIX_MASTER T2 "
				+ " ON TAPPR.COUNTRY =T2.COUNTRY AND TAPPR.LE_BOOK =T2.LE_BOOK AND TAPPR.MATRIX_ID =T2.MATRIX_ID"
				+ " WHERE UPPER(TAPPR.COUNTRY) = ?  AND UPPER(TAPPR.LE_BOOK) = ?  AND UPPER(TAPPR.CATEGORY_ID) = ?");

		StringBuffer strQueryPend = new StringBuffer("SELECT TPEND.COUNTRY, TPEND.LE_BOOK"
				+ ",TPEND.CATEGORY_ID,TPEND.MATRIX_ID, T2.MATRIX_dESCRIPTION,TPEND.EVENT_TYPE," + EventTypeAtPendDesc
				+ ",TPEND.MATRIX_ID"
				+ ",TPEND.CATEGORY_ALERT_STATUS_NT,TPEND.CATEGORY_ALERT_STATUS,TPEND.RECORD_INDICATOR_NT"
				+ ",TPEND.RECORD_INDICATOR," + RecordIndicatorNtPendDesc + ",TPEND.MAKER," + makerPendDesc
				+ ",TPEND.VERIFIER," + verifierPendDesc + ",TPEND.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_CATEGORY_ALERT_CONFIG_PEND TPEND INNER JOIN ETL_ALERT_MATRIX_MASTER T2 "
				+ " ON TPEND.COUNTRY =T2.COUNTRY AND TPEND.LE_BOOK =T2.LE_BOOK AND TPEND.MATRIX_ID =T2.MATRIX_ID"
				+ " WHERE UPPER(TPEND.COUNTRY) = ?  AND UPPER(TPEND.LE_BOOK) = ?  AND UPPER(TPEND.CATEGORY_ID) = ?");

		if (ValidationUtil.isValid(dObj.getEventType()) && ValidationUtil.isValid(dObj.getMatrixID())) {
			strQueryPend = new StringBuffer(strQueryPend.toString() + " and TPEND.EVENT_TYPE='" + dObj.getEventType()
					+ "' and TPEND.MATRIX_ID='" + dObj.getMatrixID() + "' ");
			strQueryAppr = new StringBuffer(strQueryAppr.toString() + " and TAPPR.EVENT_TYPE='" + dObj.getEventType()
					+ "' and TAPPR.MATRIX_ID='" + dObj.getMatrixID() + "' ");
		}

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry().toUpperCase();
		objParams[1] = dObj.getLeBook().toUpperCase();
		objParams[2] = dObj.getCategoryId().toUpperCase();

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
	protected List<EtlFeedCategoryAlertConfigVb> selectApprovedRecord(EtlFeedCategoryAlertConfigVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedCategoryAlertConfigVb> doSelectPendingRecord(EtlFeedCategoryAlertConfigVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlFeedCategoryAlertConfigVb records) {
		return records.getCategoryAlertStatus();
	}

	@Override
	protected void setStatus(EtlFeedCategoryAlertConfigVb vObject, int status) {
		vObject.setCategoryAlertStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "Insert Into ETL_CATEGORY_ALERT_CONFIG ( COUNTRY, LE_BOOK, CATEGORY_ID, EVENT_TYPE, MATRIX_ID, "
				+ "CATEGORY_ALERT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?," + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getEventType(),
				vObject.getMatrixID(), vObject.getCategoryAlertStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "Insert Into ETL_CATEGORY_ALERT_CONFIG_PEND ( COUNTRY, LE_BOOK, CATEGORY_ID, EVENT_TYPE, MATRIX_ID, "
				+ "CATEGORY_ALERT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?," + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getEventType(),
				vObject.getMatrixID(), vObject.getCategoryAlertStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "Insert Into ETL_CATEGORY_ALERT_CONFIG_PEND ( COUNTRY, LE_BOOK, CATEGORY_ID, EVENT_TYPE, MATRIX_ID, "
				+ "CATEGORY_ALERT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?," + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getEventType(),
				vObject.getMatrixID(), vObject.getCategoryAlertStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFeedCategoryAlertConfigVb vObject) {

		String query = "UPDATE ETL_CATEGORY_ALERT_CONFIG SET EVENT_TYPE = ?, MATRIX_ID = ?, "
				+ " CATEGORY_ALERT_STATUS_NT = ?, CATEGORY_ALERT_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + ""
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ? AND CATEGORY_ID =?";
		Object[] args = { vObject.getEventType(), vObject.getMatrixID(), vObject.getCategoryAlertStatusNt(),
				vObject.getCategoryAlertStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getCategoryId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "UPDATE ETL_CATEGORY_ALERT_CONFIG_PEND SET EVENT_TYPE = ?, MATRIX_ID = ?, "
				+ " CATEGORY_ALERT_STATUS_NT = ?, CATEGORY_ALERT_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?,"
				+ " DATE_LAST_MODIFIED = "+ getDbFunction(Constants.SYSDATE, null) 
				+ " WHERE COUNTRY = ?  AND LE_BOOK = ? AND CATEGORY_ID =?";
		Object[] args = { vObject.getEventType(), vObject.getMatrixID(), vObject.getCategoryAlertStatusNt(),
				vObject.getCategoryAlertStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "DELETE FROM ETL_CATEGORY_ALERT_CONFIG WHERE COUNTRY = ?  AND LE_BOOK = ? AND CATEGORY_ID=? AND EVENT_TYPE=? AND MATRIX_ID=?";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getEventType(),
				vObject.getMatrixID() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "DELETE FROM ETL_CATEGORY_ALERT_CONFIG_PEND WHERE COUNTRY = ?  AND LE_BOOK = ? AND CATEGORY_ID=? AND EVENT_TYPE=? AND MATRIX_ID=?";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getEventType(),
				vObject.getMatrixID() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_CATEGORY_ALERT_CONFIG_PRS";
		String query = "Delete From "+table+" Where COUNTRY =?  AND LE_BOOK = ? AND CATEGORY_ID =? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedCategory(),
				vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	protected int doDeleteApprAll(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "DELETE FROM ETL_CATEGORY_ALERT_CONFIG WHERE COUNTRY = ?  AND LE_BOOK = ? AND CATEGORY_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	protected int deletePendingRecordAll(EtlFeedCategoryAlertConfigVb vObject) {
		String query = "DELETE FROM ETL_CATEGORY_ALERT_CONFIG_PEND WHERE COUNTRY = ?  AND LE_BOOK = ? AND CATEGORY_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedCategoryAlertConfigVb vObject) {

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

		if (ValidationUtil.isValid(vObject.getCategoryId()))
			strAudit.append("CATEGORY_ID" + auditDelimiterColVal + vObject.getCategoryId().trim());
		else
			strAudit.append("CATEGORY_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("EVENT_TYPE" + auditDelimiterColVal + vObject.getEventType());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getMatrixID()))
			strAudit.append("MATRIX_ID" + auditDelimiterColVal + vObject.getMatrixID().trim());
		else
			strAudit.append("MATRIX_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("CATEGORY_ALERT_STATUS_NT" + auditDelimiterColVal + vObject.getCategoryAlertStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("CATEGORY_ALERT_STATUS" + auditDelimiterColVal + vObject.getCategoryAlertStatus());
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
		serviceName = "EtlAlertConfig";
		serviceDesc = "ETL Category Alert Config";
		tableName = "ETL_CATEGORY_ALERT_CONFIG";
		childTableName = "ETL_CATEGORY_ALERT_CONFIG";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}

	@Override
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doInsertApprRecordForNonTrans(EtlFeedCategoryAlertConfigVb vObject)
			throws RuntimeCustomException {
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.ADD;
		strApproveOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		// Try inserting the record
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setVerifier(getIntCurrentUserId());
		retVal = doInsertionAppr(vObject);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		vObject.setDateCreation(systemDate);
		exceptionCode = writeAuditLog(vObject, null);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doInsertRecordForNonTrans(EtlFeedCategoryAlertConfigVb vObject)
			throws RuntimeCustomException {
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());

		collTemp = selectApprovedRecord(vObject);
		/*
		 * if (collTemp == null) {
		 * logger.error("Collection is null for Select Approved Record"); exceptionCode
		 * = getResultObject(Constants.ERRONEOUS_OPERATION); throw
		 * buildRuntimeCustomException(exceptionCode); } // If record already exists in
		 * the approved table, reject the addition if (collTemp.size() > 0) { int
		 * staticDeletionFlag = getStatus(((ArrayList<EtlFeedCategoryAlertConfigVb>)
		 * collTemp).get(0)); if (staticDeletionFlag == Constants.PASSIVATE) { logger.
		 * info("Collection size is greater than zero - Duplicate record found, but inactive"
		 * ); exceptionCode =
		 * getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE); throw
		 * buildRuntimeCustomException(exceptionCode); } else {
		 * logger.info("Collection size is greater than zero - Duplicate record found");
		 * exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION); throw
		 * buildRuntimeCustomException(exceptionCode); } }
		 */

		// Try to see if the record already exists in the pending table, but not in
		// approved table
		collTemp = null;
		collTemp = doSelectPendingRecord(vObject);

		// The collTemp variable could not be null. If so, there is no problem fetching
		// data
		// return back error code to calling routine
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// if record already exists in pending table, modify the record
		if (collTemp.size() > 0) {
			EtlFeedCategoryAlertConfigVb vObjectLocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
			if (vObjectLocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				exceptionCode = getResultObject(Constants.PENDING_FOR_ADD_ALREADY);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			// Try inserting the record
			/*
			 * vObject.setVerifier(0); vObject.setRecordIndicator(Constants.STATUS_INSERT);
			 * String systemDate = getSystemDate(); vObject.setDateLastModified(systemDate);
			 * vObject.setDateCreation(systemDate);
			 */
			retVal = doInsertionPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

		exceptionCode = getResultObject(retVal);
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doDeleteApprRecordForNonTrans(EtlFeedCategoryAlertConfigVb vObject)
			throws RuntimeCustomException {
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		EtlFeedCategoryAlertConfigVb vObjectlocal = null;
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
		vObject.setDateCreation(vObjectlocal.getDateCreation());
		if (vObject.isStaticDelete()) {
			vObjectlocal.setMaker(getIntCurrentUserId());
			vObject.setVerifier(getIntCurrentUserId());
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
//			setStatus(vObject, Constants.PASSIVATE);
			setStatus(vObjectlocal, Constants.PASSIVATE);
			vObjectlocal.setVerifier(getIntCurrentUserId());
			vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
			retVal = doUpdateAppr(vObjectlocal);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
		} else {
			// delete the record from the Approve Table
			retVal = doDeleteAppr(vObject);
//			vObject.setRecordIndicator(-1);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);

		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (vObject.isStaticDelete()) {
			setStatus(vObjectlocal, Constants.STATUS_ZERO);
			setStatus(vObject, Constants.PASSIVATE);
			exceptionCode = writeAuditLog(vObject, vObjectlocal);
		} else {
			exceptionCode = writeAuditLog(null, vObject);
			vObject.setRecordIndicator(-1);
		}
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doRejectRecord(EtlFeedCategoryAlertConfigVb vObject) throws RuntimeCustomException {
		EtlFeedCategoryAlertConfigVb vObjectlocal = null;
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strErrorDesc = "";
		strCurrentOperation = Constants.REJECT;
		vObject.setMaker(getIntCurrentUserId());
		try {
			if (vObject.getRecordIndicator() == 1 || vObject.getRecordIndicator() == 3)
				vObject.setRecordIndicator(0);
			else
				vObject.setRecordIndicator(-1);
			// See if such a pending request exists in the pending table
			collTemp = doSelectPendingRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
			// Delete the record from the Pending table
			if (deletePendingRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doApproveRecord(EtlFeedCategoryAlertConfigVb vObject, boolean staticDelete)
			throws RuntimeCustomException {
		EtlFeedCategoryAlertConfigVb oldContents = null;
		EtlFeedCategoryAlertConfigVb vObjectlocal = null;
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		try {
			// See if such a pending request exists in the pending table
			vObject.setVerifier(getIntCurrentUserId());
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			collTemp = doSelectPendingRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}

			vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);

			if (vObjectlocal.getMaker() == getIntCurrentUserId()) {
				exceptionCode = getResultObject(Constants.MAKER_CANNOT_APPROVE);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// If it's NOT addition, collect the existing record contents from the
			// Approved table and keep it aside, for writing audit information later.
			if (vObjectlocal.getRecordIndicator() != Constants.STATUS_INSERT) {
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null || collTemp.isEmpty()) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				oldContents = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
			}

			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) { // Add authorization
				// Write the contents of the Pending table record to the Approved table
				vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
				vObjectlocal.setVerifier(getIntCurrentUserId());
				retVal = doInsertionAppr(vObjectlocal);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}

				String systemDate = getSystemDate();
				vObject.setDateLastModified(systemDate);
				vObject.setDateCreation(systemDate);
				strApproveOperation = Constants.ADD;
			} else if (vObjectlocal.getRecordIndicator() == Constants.STATUS_UPDATE) { // Modify authorization
				collTemp = selectApprovedRecord(vObject);
				if (collTemp == null || collTemp.isEmpty()) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				// If record already exists in the approved table, reject the addition
				if (collTemp.size() > 0) {
					// retVal = doUpdateAppr(vObjectlocal, MISConstants.ACTIVATE);
					vObjectlocal.setVerifier(getIntCurrentUserId());
					vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
					retVal = doUpdateAppr(vObjectlocal);
				}
				// Modify the existing contents of the record in Approved table
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
				String systemDate = getSystemDate();
				vObject.setDateLastModified(systemDate);
				// Set the current operation to write to audit log
				strApproveOperation = Constants.MODIFY;
			} else if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE) { // Delete authorization
				if (staticDelete) {
					// Update the existing record status in the Approved table to delete
					setStatus(vObjectlocal, Constants.PASSIVATE);
					vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
					vObjectlocal.setVerifier(getIntCurrentUserId());
					retVal = doUpdateAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					setStatus(vObject, Constants.PASSIVATE);
					String systemDate = getSystemDate();
					vObject.setDateLastModified(systemDate);

				} else {
					// Delete the existing record from the Approved table
					retVal = doDeleteAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
					String systemDate = getSystemDate();
					vObject.setDateLastModified(systemDate);
				}
				// Set the current operation to write to audit log
				strApproveOperation = Constants.DELETE;
			} else {
				exceptionCode = getResultObject(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Delete the record from the Pending table
			retVal = deletePendingRecord(vObjectlocal);

			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Set the internal status to Approved
			vObject.setInternalStatus(0);
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE && !staticDelete) {
				exceptionCode = writeAuditLog(null, oldContents);
				vObject.setRecordIndicator(-1);
			} else {
//				exceptionCode = writeAuditLog(vObjectlocal, oldContents);
				exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
			}

			if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
				throw buildRuntimeCustomException(exceptionCode);
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			strErrorDesc = ex.getMessage();
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doDeleteRecordForNonTrans(EtlFeedCategoryAlertConfigVb vObject)
			throws RuntimeCustomException {
		EtlFeedCategoryAlertConfigVb vObjectlocal = null;
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		collTemp = selectApprovedRecord(vObject);

		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
			int intStaticDeletionFlag = getStatus(vObjectlocal);
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// check to see if the record already exists in the pending table
		collTemp = doSelectPendingRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// If records are there, check for the status and decide what error to return
		// back
		if (collTemp.size() > 0) {
			exceptionCode = getResultObject(Constants.TRYING_TO_DELETE_APPROVAL_PENDING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}

		// insert the record into pending table with status 3 - deletion
		if (vObjectlocal == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
			vObjectlocal.setDateCreation(vObject.getDateCreation());
		}
		// vObjectlocal.setDateCreation(vObject.getDateCreation());
		vObjectlocal.setMaker(getIntCurrentUserId());
		vObjectlocal.setRecordIndicator(Constants.STATUS_DELETE);
		vObjectlocal.setVerifier(0);
		retVal = doInsertionPendWithDc(vObjectlocal);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObject.setRecordIndicator(Constants.STATUS_DELETE);
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doUpdateApprRecordForNonTrans(EtlFeedCategoryAlertConfigVb vObject)
			throws RuntimeCustomException {
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		EtlFeedCategoryAlertConfigVb vObjectlocal = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
		// Even if record is not there in Appr. table reject the record
		if (collTemp.size() == 0) {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setVerifier(getIntCurrentUserId());
		vObject.setDateCreation(vObjectlocal.getDateCreation());
		retVal = doUpdateAppr(vObject);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		exceptionCode = writeAuditLog(vObject, vObjectlocal);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doUpdateRecordForNonTrans(EtlFeedCategoryAlertConfigVb vObject)
			throws RuntimeCustomException {
		List<EtlFeedCategoryAlertConfigVb> collTemp = null;
		EtlFeedCategoryAlertConfigVb vObjectlocal = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		// Search if record already exists in pending. If it already exists, check for
		// status
		collTemp = doSelectPendingRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);

			// Check if the record is pending for deletion. If so return the error
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE) {
				exceptionCode = getResultObject(Constants.RECORD_PENDING_FOR_DELETION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObject.setDateCreation(vObjectlocal.getDateCreation());
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				vObject.setVerifier(0);
				vObject.setRecordIndicator(Constants.STATUS_INSERT);
				retVal = doUpdatePend(vObject);
			} else {
				vObject.setVerifier(0);
				vObject.setRecordIndicator(Constants.STATUS_UPDATE);
				retVal = doUpdatePend(vObject);
			}
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			collTemp = null;
			collTemp = selectApprovedRecord(vObject);

			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Even if record is not there in Appr. table reject the record
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
			// This is required for Audit Trail.
			if (collTemp.size() > 0) {
				vObjectlocal = ((ArrayList<EtlFeedCategoryAlertConfigVb>) collTemp).get(0);
				vObject.setDateCreation(vObjectlocal.getDateCreation());
			}
			vObject.setDateCreation(vObjectlocal.getDateCreation());
			// Record is there in approved, but not in pending. So add it to pending
			vObject.setVerifier(0);
			vObject.setRecordIndicator(Constants.STATUS_UPDATE);
			retVal = doInsertionPendWithDc(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String table = "ETL_CATEGORY_ALERT_CONFIG_PRS";
		String query = "Delete From "+table+" Where COUNTRY =?  AND LE_BOOK = ? AND CATEGORY_ID =? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedCategory(),
				vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
}
