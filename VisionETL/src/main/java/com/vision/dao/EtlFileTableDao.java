package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
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
import com.vision.vb.EtlFileTableVb;

@Component
public class EtlFileTableDao extends AbstractDao<EtlFileTableVb> {

	/******* Mapper Start **********/
	@Autowired
	public EtlFileColumnsDao etlFileColumnsDao;
	@Autowired
	CommonUtils commonUtils;

	String FileStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.FILE_STATUS",
			"FILE_STATUS_DESC");
	String FileStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.FILE_STATUS",
			"FILE_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFileTableVb vObject = new EtlFileTableVb();
				vObject.setConnectorId(ValidationUtil.isValid(rs.getString("CONNECTOR_ID"))?rs.getString("CONNECTOR_ID"):"");
				vObject.setViewName(ValidationUtil.isValid(rs.getString("VIEW_NAME"))?rs.getString("VIEW_NAME"):"");
				vObject.setFileId(ValidationUtil.isValid(rs.getString("FILE_ID"))?rs.getString("FILE_ID"):"");
				vObject.setSheetName(ValidationUtil.isValid(rs.getString("SHEET_NAME"))?rs.getString("SHEET_NAME"):"");
				vObject.setFileStatusNt(rs.getInt("FILE_STATUS_NT"));
				vObject.setFileStatus(rs.getInt("FILE_STATUS"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				vObject.setDateLastModified(ValidationUtil.isValid(rs.getString("DATE_LAST_MODIFIED"))?rs.getString("DATE_LAST_MODIFIED"):"");
				vObject.setDateCreation(ValidationUtil.isValid(rs.getString("DATE_CREATION"))?rs.getString("DATE_CREATION"):"");
				return vObject;
			}
		};
		return mapper;
	}

	/******* Mapper End **********/
	public List<EtlFileTableVb> getQueryPopupResults(EtlFileTableVb dObj) {

		Vector<Object> params = new Vector<Object>();
		
		StringBuffer strBufApprove = new StringBuffer("SELECT TAPPR.CONNECTOR_ID"
				+ ",TAPPR.VIEW_NAME"
				+ ",TAppr.FILE_ID"
				+ ",TAppr.SHEET_NAME"
				+ ",TAppr.FILE_STATUS_NT"
				+ ",TAppr.FILE_STATUS"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc
				+ ",TAppr.MAKER,"
				+ makerApprDesc
				+ ",TAppr.VERIFIER,"
				+ verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION" + " from ETL_FILE_TABLE TAppr ");
			String strWhereNotExists = new String( " Not Exists (Select 'X' From ETL_FILE_TABLE_PEND TPend WHERE TAppr.CONNECTOR_ID = TPend.CONNECTOR_ID AND TAppr.VIEW_NAME = TPend.VIEW_NAME )");
			StringBuffer strBufPending = new StringBuffer("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.FILE_ID"
				+ ",TPend.SHEET_NAME"
				+ ",TPend.FILE_STATUS_NT"
				+ ",TPend.FILE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc
				+ ",TPend.MAKER,"
				+ makerPendDesc
				+ ",TPend.VERIFIER,"
				+ verifierPendDesc
				+ ",TPend.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " from ETL_FILE_TABLE_PEND TPend ");
				
		try {
			if (ValidationUtil.isValid(dObj.getConnectorId())) {
				params.addElement(dObj.getConnectorId());
				CommonUtils.addToQuery("UPPER(TAppr.CONNECTOR_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.CONNECTOR_ID) = ?", strBufPending);
			}
			if (ValidationUtil.isValid(dObj.getViewName())) {
				params.addElement(dObj.getViewName());
				CommonUtils.addToQuery("UPPER(TAppr.VIEW_NAME) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.VIEW_NAME) = ?", strBufPending);
			}
			if (ValidationUtil.isValid(dObj.getFileId())) {
				params.addElement(dObj.getFileId());
				CommonUtils.addToQuery("UPPER(TAppr.FILE_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.FILE_ID) = ?", strBufPending);
			}
			String orderBy = " Order By CONNECTOR_ID, VIEW_NAME   ";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);

		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;

		}
	}

	public List<EtlFileTableVb> getQueryResults(EtlFileTableVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFileTableVb> collTemp = null;
		final int intKeyFieldsCount = 2;
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID"
				+ ",TAppr.VIEW_NAME"
				+ ",TAppr.FILE_ID"
				+ ",TAppr.SHEET_NAME"
				+ ",TAppr.FILE_STATUS_NT"
				+ ",TAppr.FILE_STATUS"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc
				+ ",TAppr.MAKER,"
				+ makerApprDesc
				+ ",TAppr.VERIFIER,"
				+ verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " From ETL_FILE_TABLE TAppr WHERE UPPER(TAppr.CONNECTOR_ID) = ?  AND UPPER(TAppr.VIEW_NAME) = ?  ");
			String strQueryPend = new String("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.FILE_ID"
				+ ",TPend.SHEET_NAME"
				+ ",TPend.FILE_STATUS_NT"
				+ ",TPend.FILE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc
				+ ",TPend.MAKER,"
				+ makerPendDesc
				+ ",TPend.VERIFIER,"
				+ verifierPendDesc
				+ ",TPend.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " From ETL_FILE_TABLE_PEND TPend WHERE UPPER(TPend.CONNECTOR_ID) = ?  AND UPPER(TPend.VIEW_NAME) = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getConnectorId().toUpperCase();
		objParams[1] = dObj.getViewName().toUpperCase();

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
	protected List<EtlFileTableVb> selectApprovedRecord(EtlFileTableVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFileTableVb> doSelectPendingRecord(EtlFileTableVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlFileTableVb records) {
		return records.getFileStatus();
	}

	@Override
	protected void setStatus(EtlFileTableVb vObject, int status) {
		vObject.setFileStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFileTableVb vObject) {
		String query = "Insert Into ETL_FILE_TABLE (CONNECTOR_ID, VIEW_NAME, FILE_ID, SHEET_NAME, FILE_STATUS_NT, FILE_STATUS, RECORD_INDICATOR_NT, "
				+ " RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getFileId(), vObject.getSheetName(),
				vObject.getFileStatusNt(), vObject.getFileStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlFileTableVb vObject) {
		String query = "Insert Into ETL_FILE_TABLE_PEND (CONNECTOR_ID, VIEW_NAME, FILE_ID, SHEET_NAME, FILE_STATUS_NT, FILE_STATUS, RECORD_INDICATOR_NT,"
				+ " RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getFileId(), vObject.getSheetName(),
				vObject.getFileStatusNt(), vObject.getFileStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFileTableVb vObject) {
		String query = "Insert Into ETL_FILE_TABLE_PEND (CONNECTOR_ID, VIEW_NAME, FILE_ID, SHEET_NAME, FILE_STATUS_NT, FILE_STATUS, RECORD_INDICATOR_NT, "
				+ " RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + "  )";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName(), vObject.getFileId(), vObject.getSheetName(),
				vObject.getFileStatusNt(), vObject.getFileStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFileTableVb vObject) {
		String query = "Update ETL_FILE_TABLE Set FILE_ID = ?, SHEET_NAME = ?, FILE_STATUS_NT = ?, FILE_STATUS = ?, RECORD_INDICATOR_NT = ?, "
				+ " RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  WHERE CONNECTOR_ID = ?  AND VIEW_NAME = ? ";
		Object[] args = { vObject.getFileId(), vObject.getSheetName(), vObject.getFileStatusNt(),
				vObject.getFileStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getConnectorId(),
				vObject.getViewName() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFileTableVb vObject) {
		String query = "Update ETL_FILE_TABLE_PEND Set FILE_ID = ?, SHEET_NAME = ?, FILE_STATUS_NT = ?, FILE_STATUS = ?, RECORD_INDICATOR_NT = ?, "
				+ " RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  WHERE CONNECTOR_ID = ?  AND VIEW_NAME = ? ";
		Object[] args = { vObject.getFileId(), vObject.getSheetName(), vObject.getFileStatusNt(),
				vObject.getFileStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getConnectorId(),
				vObject.getViewName() };

		return getJdbcTemplate().update(query, args);
	}
	protected int doDeleteApprAll(String connectorId) {
		String query = "Delete From ETL_FILE_TABLE Where CONNECTOR_ID = ?   ";
		Object[] args = { connectorId };
		return getJdbcTemplate().update(query, args);
	}

	protected int deletePendingRecordAll(String connectorId) {
		String query = "Delete From ETL_FILE_TABLE_PEND Where CONNECTOR_ID = ? ";
		Object[] args = { connectorId};
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFileTableVb vObject) {
		String query = "Delete From ETL_FILE_TABLE Where CONNECTOR_ID = ?  AND VIEW_NAME = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFileTableVb vObject) {
		String query = "Delete From ETL_FILE_TABLE_PEND Where CONNECTOR_ID = ?  AND VIEW_NAME = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getViewName() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_FILE_TABLE_PRS";
		String query = "Delete From "+table+" Where FEED_CATEGORY =?  AND FEED_ID = ? AND  SESSION_ID=? ";
		Object[] args = {  vObject.getFeedCategory(), vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}

	@Override
	protected String getAuditString(EtlFileTableVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getFileId()))
			strAudit.append("FILE_ID" + auditDelimiterColVal + vObject.getFileId().trim());
		else
			strAudit.append("FILE_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSheetName()))
			strAudit.append("SHEET_NAME" + auditDelimiterColVal + vObject.getSheetName().trim());
		else
			strAudit.append("SHEET_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FILE_STATUS_NT" + auditDelimiterColVal + vObject.getFileStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("FILE_STATUS" + auditDelimiterColVal + vObject.getFileStatus());
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
		serviceName = "EtlFileTable";
		serviceDesc = "ETL File Table";
		tableName = "ETL_FILE_TABLE";
		childTableName = "ETL_FILE_TABLE";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}
	@Transactional(rollbackForClassName={"com.vision.exception.RuntimeCustomException"})
	protected ExceptionCode doInsertRecordForNonTrans(EtlFileTableVb vObject) throws RuntimeCustomException {
		List<EtlFileTableVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		/*if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int staticDeletionFlag = getStatus(((ArrayList<EtlFileTableVb>) collTemp).get(0));
			if (staticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}*/

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
			EtlFileTableVb vObjectLocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
			if (vObjectLocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				exceptionCode = getResultObject(Constants.PENDING_FOR_ADD_ALREADY);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			// Try inserting the record
			/*vObject.setRecordIndicator(Constants.STATUS_INSERT);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
			vObject.setDateCreation(systemDate);*/
			retVal = doInsertionPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		// Insert Data for ETL_File_Columns
		if (vObject.getFileColumnsList() != null && vObject.getFileColumnsList().size() > 0) {
			int i=1;
			for (EtlFileColumnsVb lObject : vObject.getFileColumnsList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setViewName(vObject.getViewName());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setColumnSortOrder(i);
				lObject.setVerifier(vObject.getVerifier());
				lObject.setDateCreation(vObject.getDateCreation());
				lObject.setDateLastModified(vObject.getDateLastModified());
				lObject.setRecordIndicator(vObject.getRecordIndicator());
				exceptionCode = etlFileColumnsDao.doInsertRecordForNonTrans(lObject);
				retVal = exceptionCode.getErrorCode();
				i++;
			}
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		}
		exceptionCode = getResultObject(retVal);
		return exceptionCode;
	}

	protected ExceptionCode doInsertApprRecordForNonTrans(EtlFileTableVb vObject) throws RuntimeCustomException {
		List<EtlFileTableVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.ADD;
		strApproveOperation = Constants.ADD;
		setServiceDefaults();
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFileTableVb>) collTemp).get(0));
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
		// Insert Data for ETL_File_Columns
		if (vObject.getFileColumnsList() != null && vObject.getFileColumnsList().size() > 0) {
			int i=1;
			for (EtlFileColumnsVb lObject : vObject.getFileColumnsList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setViewName(vObject.getViewName());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setColumnSortOrder(i);
				exceptionCode = etlFileColumnsDao.doInsertApprRecordForNonTrans(lObject);
				retVal = exceptionCode.getErrorCode();
				i++;
			}
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(retVal);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}
	@Transactional(rollbackForClassName={"com.vision.exception.RuntimeCustomException"})
	protected ExceptionCode doDeleteApprRecordForNonTrans(EtlFileTableVb vObject) throws RuntimeCustomException {
		List<EtlFileTableVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		EtlFileTableVb vObjectlocal = null;
		vObject.setMaker(getIntCurrentUserId());
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFileTableVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
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
		List<EtlFileColumnsVb>  mainTableList = etlFileColumnsDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_ZERO,vObject.getViewName());
		for (EtlFileColumnsVb lObject : mainTableList) {
			lObject.setStaticDelete(false);
			exceptionCode = etlFileColumnsDao.doDeleteApprRecordForNonTrans(lObject);
			retVal = exceptionCode.getErrorCode();
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		List<EtlFileColumnsVb>  pendTbleList = etlFileColumnsDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING,vObject.getViewName());
		for (EtlFileColumnsVb lObject : pendTbleList) {
			lObject.setStaticDelete(false);
			exceptionCode = etlFileColumnsDao.doRejectRecord(lObject);
			retVal = exceptionCode.getErrorCode();
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return exceptionCode;
	}
	@Transactional(rollbackForClassName={"com.vision.exception.RuntimeCustomException"})
	public ExceptionCode doRejectRecord(EtlFileTableVb vObject) throws RuntimeCustomException {
		EtlFileTableVb vObjectlocal = null;
		List<EtlFileTableVb> collTemp = null;
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
			vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
			// Delete the record from the Pending table
			if (deletePendingRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			List<EtlFileColumnsVb> fileColumnsList = etlFileColumnsDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING, vObject.getViewName());
			if (fileColumnsList != null && fileColumnsList.size() > 0) {
				for (EtlFileColumnsVb lObject : fileColumnsList) {
					lObject.setStaticDelete(false);
					exceptionCode = etlFileColumnsDao.doRejectRecord(lObject);
					retVal = exceptionCode.getErrorCode();
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
			/*fileColumnsList = etlFileColumnsDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_ZERO, vObject.getViewName());
			if (fileColumnsList != null && fileColumnsList.size() > 0) {
				for (EtlFileColumnsVb lObject : fileColumnsList) {
					lObject.setStaticDelete(false);
					exceptionCode = etlFileColumnsDao.doDeleteApprRecordForNonTrans(lObject);
					retVal = exceptionCode.getErrorCode();
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}*/
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
	@Transactional(rollbackForClassName={"com.vision.exception.RuntimeCustomException"})
	public ExceptionCode doApproveRecord(EtlFileTableVb vObject, boolean staticDelete) throws RuntimeCustomException {
		EtlFileTableVb oldContents = null;
		EtlFileTableVb vObjectlocal = null;
		List<EtlFileTableVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		try {
			if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
				exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING_APPROVE);
				throw buildRuntimeCustomException(exceptionCode);
			}
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

			vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);

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
				oldContents = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
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
	
	public List<EtlFileTableVb> getQueryResultsByParent(String ConnectorID, int intStatus) {

		setServiceDefaults();
		List<EtlFileTableVb> collTemp = null;
		final int intKeyFieldsCount = 1;
		
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID"
				+ ",TAppr.VIEW_NAME"
				+ ",TAppr.FILE_ID"
				+ ",TAppr.SHEET_NAME"
				+ ",TAppr.FILE_STATUS_NT"
				+ ",TAppr.FILE_STATUS"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc
				+ ",TAppr.MAKER,"
				+ makerApprDesc
				+ ",TAppr.VERIFIER,"
				+ verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TAppr.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
				+" From ETL_FILE_TABLE TAppr WHERE UPPER(TAppr.CONNECTOR_ID) = ?  ");
		
			String strQueryPend = new String("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.FILE_ID"
				+ ",TPend.SHEET_NAME"
				+ ",TPend.FILE_STATUS_NT"
				+ ",TPend.FILE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc
				+ ",TPend.MAKER,"
				+ makerPendDesc
				+ ",TPend.VERIFIER,"
				+ verifierPendDesc
				+ ",TPend.INTERNAL_STATUS"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_LAST_MODIFIED, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED"
				+ ", "+getDbFunction(Constants.DATEFUNC, null)+"(TPend.DATE_CREATION, '"+getDbFunction(Constants.DD_Mon_RRRR, null) +" "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION"
				+" From ETL_FILE_TABLE_PEND TPend WHERE UPPER(TPend.CONNECTOR_ID) = ?  ");

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
	protected ExceptionCode doDeleteRecordForNonTrans(EtlFileTableVb vObject) throws RuntimeCustomException {
		EtlFileTableVb vObjectlocal = null;
		List<EtlFileTableVb> collTemp = null;
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
			vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
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
			vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
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
		vObject.setVerifier(0);
		List<EtlFileColumnsVb> mainTableList = etlFileColumnsDao.getQueryResultsByParent(vObject.getConnectorId(),
				Constants.STATUS_ZERO, vObject.getViewName());
		for (EtlFileColumnsVb lObject : mainTableList) {
			lObject.setStaticDelete(vObject.isStaticDelete());
			exceptionCode = etlFileColumnsDao.doDeleteRecordForNonTrans(lObject);
			retVal = exceptionCode.getErrorCode();
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}
	public List<EtlFileTableVb> getQueryResultsByFileID(EtlFileTableVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFileTableVb> collTemp = null;
		final int intKeyFieldsCount = 2;
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID"
				+ ",TAppr.VIEW_NAME"
				+ ",TAppr.FILE_ID"
				+ ",TAppr.SHEET_NAME"
				+ ",TAppr.FILE_STATUS_NT"
				+ ",TAppr.FILE_STATUS"
				+ ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc
				+ ",TAppr.MAKER,"
				+ makerApprDesc
				+ ",TAppr.VERIFIER,"
				+ verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_FILE_TABLE TAPPR WHERE UPPER(TAPPR.CONNECTOR_ID) = ?  AND UPPER(TAPPR.FILE_ID) = ?  ");
			String strQueryPend = new String("Select TPend.CONNECTOR_ID"
				+ ",TPend.VIEW_NAME"
				+ ",TPend.FILE_ID"
				+ ",TPend.SHEET_NAME"
				+ ",TPend.FILE_STATUS_NT"
				+ ",TPend.FILE_STATUS"
				+ ",TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc
				+ ",TPend.MAKER,"
				+ makerPendDesc
				+ ",TPEND.VERIFIER,"
				+ verifierPendDesc
				+ ",TPEND.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_FILE_TABLE_PEND TPEND WHERE UPPER(TPEND.CONNECTOR_ID) = ?  AND UPPER(TPEND.FILE_ID) = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getConnectorId().toUpperCase();
		objParams[1] = dObj.getFileId().toUpperCase();

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
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doUpdateRecordForNonTrans(EtlFileTableVb vObject) throws RuntimeCustomException {
		List<EtlFileTableVb> collTemp = null;
		EtlFileTableVb vObjectlocal = null;
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
			vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);

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
				vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
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
		if (vObject.getFileColumnsList() != null && vObject.getFileColumnsList().size() > 0) {
			int i = 1;
			for (EtlFileColumnsVb lObject : vObject.getFileColumnsList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setViewName(vObject.getViewName());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setColumnSortOrder(i);
				if (lObject.isNewRecord()) {
					strCurrentOperation = Constants.ADD;
					exceptionCode = etlFileColumnsDao.doInsertRecordForNonTrans(lObject);
				} else {
					strCurrentOperation = Constants.MODIFY;
					
					exceptionCode = etlFileColumnsDao.doUpdateRecordForNonTrans(lObject);
				}
				i++;
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doUpdateApprRecordForNonTrans(EtlFileTableVb vObject) throws RuntimeCustomException {
		List<EtlFileTableVb> collTemp = null;
		EtlFileTableVb vObjectlocal = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.MODIFY;
		strErrorDesc = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		if ("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))) {
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlFileTableVb>) collTemp).get(0);
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
		if (vObject.getFileColumnsList() != null && vObject.getFileColumnsList().size() > 0) {
			int i = 1;
			for (EtlFileColumnsVb lObject : vObject.getFileColumnsList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setViewName(vObject.getViewName());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setColumnSortOrder(i);
				if (lObject.isNewRecord()) {
					strCurrentOperation = Constants.ADD;
					exceptionCode = etlFileColumnsDao.doInsertApprRecordForNonTrans(lObject);
				} else {
					strCurrentOperation = Constants.MODIFY;
					exceptionCode = etlFileColumnsDao.doUpdateApprRecordForNonTrans(lObject);
				}
				i++;
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return exceptionCode;
	}
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String table = "ETL_FILE_TABLE_PRS";
		String query = "Delete From "+table+" Where FEED_CATEGORY =?  AND  SESSION_ID=? ";
		Object[] args = {  vObject.getFeedCategory(), vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
}
