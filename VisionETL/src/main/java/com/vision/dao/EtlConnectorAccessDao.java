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
import com.vision.vb.EtlConnectorAccessVb;
import com.vision.vb.SmartSearchVb;

@Component

public class EtlConnectorAccessDao extends AbstractDao<EtlConnectorAccessVb> {

	/******* Mapper Start **********/
	String UserGroupAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 1, "TAppr.USER_GROUP",
			"USER_GROUP_DESC");
	String UserGroupAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 1, "TPend.USER_GROUP",
			"USER_GROUP_DESC");

	String UserProfileAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2, "TAppr.USER_PROFILE",
			"USER_PROFILE_DESC");
	String UserProfileAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2, "TPend.USER_PROFILE",
			"USER_PROFILE_DESC");

	String DataConnectorStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1,
			"TAppr.DATACONNECTOR_STATUS", "DATACONNECTOR_STATUS_DESC");
	String DataConnectorStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1,
			"TPend.DATACONNECTOR_STATUS", "DATACONNECTOR_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorAccessVb vObject = new EtlConnectorAccessVb();
				if (rs.getString("CONNECTOR_ID") != null) {
					vObject.setConnectorId(rs.getString("CONNECTOR_ID"));
				} else {
					vObject.setConnectorId("");
				}
				vObject.setUserGroupAt(rs.getInt("USER_GROUP_AT"));
				if (rs.getString("USER_GROUP") != null) {
					vObject.setUserGroup(rs.getString("USER_GROUP"));
				} else {
					vObject.setUserGroup("");
				}
				vObject.setUserProfileAt(rs.getInt("USER_PROFILE_AT"));
				if (rs.getString("USER_PROFILE") != null) {
					vObject.setUserProfile(rs.getString("USER_PROFILE"));
				} else {
					vObject.setUserProfile("");
				}
				vObject.setVisionId(rs.getInt("VISION_ID"));
				if (rs.getString("WRITE_FLAG") != null) {
					vObject.setWriteFlag(rs.getString("WRITE_FLAG"));
				} else {
					vObject.setWriteFlag("");
				}
				vObject.setDataConnectorStatusNt(rs.getInt("DATACONNECTOR_STATUS_NT"));
				vObject.setDataConnectorStatus(rs.getInt("DATACONNECTOR_STATUS"));
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
	public List<EtlConnectorAccessVb> getQueryPopupResults(EtlConnectorAccessVb dObj) {

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer(
				"Select TAppr.CONNECTOR_ID,TAppr.USER_GROUP_AT,TAppr.USER_GROUP,TAppr.USER_PROFILE_AT,TAppr.USER_PROFILE,TAppr.VISION_ID,TAppr.WRITE_FLAG,TAppr.DATACONNECTOR_STATUS_NT,TAppr.DATACONNECTOR_STATUS,TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null)
						+ "') DATE_CREATION from ETL_CONNECTOR_ACCESS TAppr ");
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_CONNECTOR_ACCESS_PEND TPend WHERE TAppr.CONNECTOR_ID = TPend.CONNECTOR_ID )");
		StringBuffer strBufPending = new StringBuffer(
				"Select TPend.CONNECTOR_ID,TPend.USER_GROUP_AT,TPend.USER_GROUP,TPend.USER_PROFILE_AT,TPend.USER_PROFILE,TPend.VISION_ID,TPend.WRITE_FLAG,TPend.DATACONNECTOR_STATUS_NT,TPend.DATACONNECTOR_STATUS,TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null)
						+ "') DATE_CREATION from ETL_CONNECTOR_ACCESS_PEND TPend ");
		try {
			if (dObj.getSmartSearchOpt() != null && dObj.getSmartSearchOpt().size() > 0) {
				int count = 1;
				for (SmartSearchVb data : dObj.getSmartSearchOpt()) {
					if (count == dObj.getSmartSearchOpt().size()) {
						data.setJoinType("");
					} else {
						if (!ValidationUtil.isValid(data.getJoinType()) && !("AND".equalsIgnoreCase(data.getJoinType())
								|| "OR".equalsIgnoreCase(data.getJoinType()))) {
							data.setJoinType("AND");
						}
					}
					String val = CommonUtils.criteriaBasedVal(data.getCriteria(), data.getValue());
					switch (data.getObject()) {

					case "connectorId":
						CommonUtils.addToQuerySearch(" upper(TAppr.CONNECTOR_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.CONNECTOR_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "userGroup":
						CommonUtils.addToQuerySearch(" upper(TAppr.USER_GROUP) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.USER_GROUP) " + val, strBufPending,
								data.getJoinType());
						break;

					case "userProfile":
						CommonUtils.addToQuerySearch(" upper(TAppr.USER_PROFILE) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.USER_PROFILE) " + val, strBufPending,
								data.getJoinType());
						break;

					case "visionId":
						CommonUtils.addToQuerySearch(" upper(TAppr.VISION_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.VISION_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "writeFlag":
						CommonUtils.addToQuerySearch(" upper(TAppr.WRITE_FLAG) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.WRITE_FLAG) " + val, strBufPending,
								data.getJoinType());
						break;

					case "dataConnectorStatus":
						CommonUtils.addToQuerySearch(" upper(TAppr.DATACONNECTOR_STATUS) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.DATACONNECTOR_STATUS) " + val, strBufPending,
								data.getJoinType());
						break;

					case "recordIndicator":
						CommonUtils.addToQuerySearch(" upper(TAppr.RECORD_INDICATOR) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.RECORD_INDICATOR) " + val, strBufPending,
								data.getJoinType());
						break;

					default:
					}
					count++;
				}
			}
			String orderBy = " Order By  CONNECTOR_ID  ";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);

		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlConnectorAccessVb> getQueryResults(EtlConnectorAccessVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlConnectorAccessVb> collTemp = null;

		final int intKeyFieldsCount = 1;
		String strQueryAppr = new String(
				"Select TAppr.CONNECTOR_ID,TAppr.USER_GROUP_AT,TAppr.USER_GROUP,TAppr.USER_PROFILE_AT, "
						+ " TAppr.USER_PROFILE,TAppr.VISION_ID,TAppr.WRITE_FLAG,TAppr.DATACONNECTOR_STATUS_NT,TAppr.DATACONNECTOR_STATUS, "
						+ " TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
						+ " " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED,  "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null)
						+ "') DATE_CREATION From ETL_CONNECTOR_ACCESS TAppr "
						+ " WHERE UPPER(TAppr.CONNECTOR_ID) = ?  ");
		String strQueryPend = new String("Select TPend.CONNECTOR_ID,TPend.USER_GROUP_AT,TPend.USER_GROUP, "
				+ " TPend.USER_PROFILE_AT,TPend.USER_PROFILE,TPend.VISION_ID,TPend.WRITE_FLAG,TPend.DATACONNECTOR_STATUS_NT, "
				+ " TPend.DATACONNECTOR_STATUS,TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER, "
				+ " TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TPend.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED,  "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION From ETL_CONNECTOR_ACCESS_PEND TPend  WHERE UPPER(TPend.CONNECTOR_ID) = ?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getConnectorId();

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
	protected List<EtlConnectorAccessVb> selectApprovedRecord(EtlConnectorAccessVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlConnectorAccessVb> doSelectPendingRecord(EtlConnectorAccessVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlConnectorAccessVb records) {
		return records.getDataConnectorStatus();
	}

	@Override
	protected void setStatus(EtlConnectorAccessVb vObject, int status) {
		vObject.setDataConnectorStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlConnectorAccessVb vObject) {
		String query = "Insert Into ETL_CONNECTOR_ACCESS ( CONNECTOR_ID, USER_GROUP_AT, USER_GROUP, USER_PROFILE_AT, "
				+ " USER_PROFILE, VISION_ID, WRITE_FLAG, DATACONNECTOR_STATUS_NT, DATACONNECTOR_STATUS, RECORD_INDICATOR_NT, "
				+ " RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getConnectorId(), vObject.getUserGroupAt(), vObject.getUserGroup(),
				vObject.getUserProfileAt(), vObject.getUserProfile(), vObject.getVisionId(), vObject.getWriteFlag(),
				vObject.getDataConnectorStatusNt(), vObject.getDataConnectorStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlConnectorAccessVb vObject) {
		String query = "Update ETL_CONNECTOR_ACCESS Set  USER_GROUP_AT = ?, USER_GROUP = ? , USER_PROFILE_AT = ?, USER_PROFILE = ?,  VISION_ID = ?, WRITE_FLAG = ?, DATACONNECTOR_STATUS_NT = ?, DATACONNECTOR_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  WHERE  CONNECTOR_ID = ?   ";
		Object[] args = { vObject.getUserGroupAt(), vObject.getUserGroup(), vObject.getUserProfileAt(),
				vObject.getUserProfile(), vObject.getVisionId(), vObject.getWriteFlag(),
				vObject.getDataConnectorStatusNt(), vObject.getDataConnectorStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getConnectorId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlConnectorAccessVb vObject) {
		String query = "Delete From ETL_CONNECTOR_ACCESS Where CONNECTOR_ID = ?   ";
		Object[] args = { vObject.getConnectorId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlConnectorAccessVb vObject) {
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");

		if (ValidationUtil.isValid(vObject.getConnectorId()))
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + vObject.getConnectorId().trim());
		else
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("USER_GROUP_AT" + auditDelimiterColVal + vObject.getUserGroupAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getUserGroup()))
			strAudit.append("USER_GROUP" + auditDelimiterColVal + vObject.getUserGroup().trim());
		else
			strAudit.append("USER_GROUP" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("USER_PROFILE_AT" + auditDelimiterColVal + vObject.getUserProfileAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getUserProfile()))
			strAudit.append("USER_PROFILE" + auditDelimiterColVal + vObject.getUserProfile().trim());
		else
			strAudit.append("USER_PROFILE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("VISION_ID" + auditDelimiterColVal + vObject.getVisionId());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getWriteFlag()))
			strAudit.append("WRITE_FLAG" + auditDelimiterColVal + vObject.getWriteFlag().trim());
		else
			strAudit.append("WRITE_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("DATACONNECTOR_STATUS_NT" + auditDelimiterColVal + vObject.getDataConnectorStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("DATACONNECTOR_STATUS" + auditDelimiterColVal + vObject.getDataConnectorStatus());
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
		serviceName = "EtlConnectorAccess";
		serviceDesc = "ETL Connector Access";
		tableName = "ETL_CONNECTOR_ACCESS";
		childTableName = "ETL_CONNECTOR_ACCESS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

}
