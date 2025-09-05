package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlMqColumnsVb;
import com.vision.vb.EtlMqTableVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlMqColumnsDao extends AbstractDao<EtlMqColumnsVb> {

	@Autowired
	private NumSubTabDao numSubTabDao;
	@Autowired
	private VisionUsersDao visionUsersDao;

	/******* Mapper Start **********/

	String ColumnDatatypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2014, "TAppr.COLUMN_DATATYPE",
			"COLUMN_DATATYPE_DESC");
	String ColumnDatatypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2014, "TPend.COLUMN_DATATYPE",
			"COLUMN_DATATYPE_DESC");

	/*
	 * String DateFormatNtApprDesc =
	 * ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TAppr.DATE_FORMAT",
	 * "DATE_FORMAT_DESC"); String DateFormatNtPendDesc =
	 * ValidationUtil.numAlphaTabDescritpionQuery("NT", 1103, "TPend.DATE_FORMAT",
	 * "DATE_FORMAT_DESC");
	 * 
	 * String NumberFormatNtApprDesc =
	 * ValidationUtil.numAlphaTabDescritpionQuery("NT", 1102, "TAppr.NUMBER_FORMAT",
	 * "NUMBER_FORMAT_DESC"); String NumberFormatNtPendDesc =
	 * ValidationUtil.numAlphaTabDescritpionQuery("NT", 1102, "TPend.NUMBER_FORMAT",
	 * "NUMBER_FORMAT_DESC");
	 */

	String MqColumnStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.MQ_COLUMN_STATUS",
			"MQ_COLUMN_STATUS_DESC");
	String MqColumnStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.MQ_COLUMN_STATUS",
			"MQ_COLUMN_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlMqColumnsVb vObject = new EtlMqColumnsVb();
				if (rs.getString("CONNECTOR_ID") != null) {
					vObject.setConnectorId(rs.getString("CONNECTOR_ID"));
				} else {
					vObject.setConnectorId("");
				}
				if (rs.getString("QUERY_ID") != null) {
					vObject.setQueryId(rs.getString("QUERY_ID"));
				} else {
					vObject.setQueryId("");
				}
				vObject.setColumnId(rs.getInt("COLUMN_ID"));
				if (rs.getString("COLUMN_NAME") != null) {
					vObject.setColumnName(rs.getString("COLUMN_NAME"));
				} else {
					vObject.setColumnName("");
				}
				vObject.setColumnSortOrder(rs.getInt("COLUMN_SORT_ORDER"));
				vObject.setColumnDatatypeAt(rs.getInt("COLUMN_DATATYPE_AT"));
				if (rs.getString("COLUMN_DATATYPE") != null) {
					vObject.setColumnDatatype(rs.getString("COLUMN_DATATYPE"));
				} else {
					vObject.setColumnDatatype("");
				}
				vObject.setColumnLength(rs.getInt("COLUMN_LENGTH"));
				vObject.setDateFormatNt(rs.getInt("DATE_FORMAT_NT"));
				vObject.setDateFormat(rs.getInt("DATE_FORMAT"));
				vObject.setDateFormatNtDesc(rs.getString("DATE_FORMAT_DESC"));
				vObject.setNumberFormatNt(rs.getInt("NUMBER_FORMAT_NT"));
				vObject.setNumberFormat(rs.getInt("NUMBER_FORMAT"));
				vObject.setNumberFormatNtDesc(rs.getString("NUMBER_FORMAT_DESC"));
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
				vObject.setMqColumnStatusNt(rs.getInt("MQ_COLUMN_STATUS_NT"));
				vObject.setMqColumnStatus(rs.getInt("MQ_COLUMN_STATUS"));
				vObject.setStatusDesc(rs.getString("MQ_COLUMN_STATUS_DESC"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
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
	public List<EtlMqColumnsVb> getQueryPopupResults(EtlMqColumnsVb dObj) {

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer(
				"Select TAppr.CONNECTOR_ID ,TAppr.QUERY_ID ,TAppr.COLUMN_ID ,TAppr.COLUMN_NAME"
						+ ",TAppr.COLUMN_SORT_ORDER,TAppr.COLUMN_DATATYPE_AT,TAppr.COLUMN_DATATYPE"
						+ ",TAppr.COLUMN_LENGTH,TAppr.DATE_FORMAT_NT,TAppr.DATE_FORMAT,"
						+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE  TAppr.COLUMN_DATATYPE = 'D'  AND NUM_TAB = 1103  AND NUM_SUB_TAB = "
						+ "   (CASE  WHEN ( TAppr.DATE_FORMAT = 0 OR TAppr.DATE_FORMAT IS NULL)  THEN 2   ELSE TAppr.DATE_FORMAT END))  DATE_FORMAT_DESC "
						+ ",TAppr.NUMBER_FORMAT_NT,TAppr.NUMBER_FORMAT,"
						+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM NUM_SUB_TAB WHERE  NUM_TAB = 1102 AND NUM_SUB_TAB = (CASE WHEN (   TAppr.NUMBER_FORMAT = 0 OR TAppr.NUMBER_FORMAT IS NULL)"
						+ " THEN   (CASE WHEN (TAppr.COLUMN_DATATYPE = 'C') THEN 3  WHEN (TAppr.COLUMN_DATATYPE = 'N') THEN 2 ELSE 0	 END)  ELSE	 TAppr.NUMBER_FORMAT   END)) NUMBER_FORMAT_DESC"
						+ ",TAppr.PRIMARY_KEY_FLAG"
						+ ",TAppr.PARTITION_COLUMN_FLAG,TAppr.MQ_COLUMN_STATUS_NT,TAppr.MQ_COLUMN_STATUS,"
						+ MqColumnStatusNtApprDesc + ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,"
						+ RecordIndicatorNtApprDesc + ",TAppr.MAKER," + makerApprDesc + ",TAppr.VERIFIER,"
						+ verifierApprDesc + ",TAppr.INTERNAL_STATUS" + ", " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TAppr.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
						+ "') DATE_CREATION" + " from ETL_MQ_COLUMNS TAppr ");
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_MQ_COLUMNS_PEND TPend WHERE TAppr.CONNECTOR_ID = TPend.CONNECTOR_ID AND TAppr.QUERY_ID = TPend.QUERY_ID AND TAppr.COLUMN_ID = TPend.COLUMN_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.CONNECTOR_ID,TPend.QUERY_ID"
				+ ",TPend.COLUMN_ID,TPend.COLUMN_NAME,TPend.COLUMN_SORT_ORDER,TPend.COLUMN_DATATYPE_AT"
				+ ",TPend.COLUMN_DATATYPE,TPend.COLUMN_LENGTH,TPend.DATE_FORMAT_NT,TPend.DATE_FORMAT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE  TPend.COLUMN_DATATYPE = 'D'  AND NUM_TAB = 1103  AND NUM_SUB_TAB = "
				+ "   (CASE  WHEN (   TPend.DATE_FORMAT = 0    OR TPend.DATE_FORMAT IS NULL)  THEN 2   ELSE TPend.DATE_FORMAT END))  DATE_FORMAT_DESC "
				+ ",TPend.NUMBER_FORMAT_NT,TPend.NUMBER_FORMAT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM NUM_SUB_TAB WHERE  NUM_TAB = 1102 AND NUM_SUB_TAB = (CASE WHEN (   TPend.NUMBER_FORMAT = 0 OR TPend.NUMBER_FORMAT IS NULL)"
				+ " THEN   (CASE WHEN (TPend.COLUMN_DATATYPE = 'C') THEN 3  WHEN (TPend.COLUMN_DATATYPE = 'N') THEN 2 ELSE 0	 END)  ELSE	 TPend.NUMBER_FORMAT   END)) NUMBER_FORMAT_DESC"
				+ ",TPend.PRIMARY_KEY_FLAG"
				+ ",TPend.PARTITION_COLUMN_FLAG,TPend.MQ_COLUMN_STATUS_NT,TPend.MQ_COLUMN_STATUS,"
				+ MqColumnStatusNtPendDesc + ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc + ",TPend.MAKER," + makerPendDesc + ",TPend.VERIFIER," + verifierPendDesc
				+ ",TPend.INTERNAL_STATUS" + ", " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TPend.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION" + " from ETL_MQ_COLUMNS_PEND TPend ");
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

					case "queryId":
						CommonUtils.addToQuerySearch(" upper(TAppr.QUERY_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.QUERY_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "columnId":
						CommonUtils.addToQuerySearch(" upper(TAppr.COLUMN_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.COLUMN_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "columnName":
						CommonUtils.addToQuerySearch(" upper(TAppr.COLUMN_NAME) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.COLUMN_NAME) " + val, strBufPending,
								data.getJoinType());
						break;

					case "columnSortOrder":
						CommonUtils.addToQuerySearch(" upper(TAppr.COLUMN_SORT_ORDER) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.COLUMN_SORT_ORDER) " + val, strBufPending,
								data.getJoinType());
						break;

					case "columnDatatype":
						CommonUtils.addToQuerySearch(" upper(TAppr.COLUMN_DATATYPE) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.COLUMN_DATATYPE) " + val, strBufPending,
								data.getJoinType());
						break;

					case "columnLength":
						CommonUtils.addToQuerySearch(" upper(TAppr.COLUMN_LENGTH) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.COLUMN_LENGTH) " + val, strBufPending,
								data.getJoinType());
						break;

					case "dateFormat":
						CommonUtils.addToQuerySearch(" upper(TAppr.DATE_FORMAT) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.DATE_FORMAT) " + val, strBufPending,
								data.getJoinType());
						break;

					case "numberFormat":
						CommonUtils.addToQuerySearch(" upper(TAppr.NUMBER_FORMAT) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.NUMBER_FORMAT) " + val, strBufPending,
								data.getJoinType());
						break;

					case "primaryKeyFlag":
						CommonUtils.addToQuerySearch(" upper(TAppr.PRIMARY_KEY_FLAG) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.PRIMARY_KEY_FLAG) " + val, strBufPending,
								data.getJoinType());
						break;

					case "partitionColumnFlag":
						CommonUtils.addToQuerySearch(" upper(TAppr.PARTITION_COLUMN_FLAG) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.PARTITION_COLUMN_FLAG) " + val, strBufPending,
								data.getJoinType());
						break;
					case "mqColumnStatus":
						CommonUtils.addToQuerySearch(" upper(TAppr.MQ_COLUMN_STATUS) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.MQ_COLUMN_STATUS) " + val, strBufPending,
								data.getJoinType());
						break;
					case "statusDesc":
						List<NumSubTabVb> numSTData = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 1);
						String actData = "";
						for (int k = 0; k < numSTData.size(); k++) {
							int numsubtab = numSTData.get(k).getNumSubTab();
							if (!ValidationUtil.isValid(actData)) {
								actData = "'" + Integer.toString(numsubtab) + "'";
							} else {
								actData = actData + ",'" + Integer.toString(numsubtab) + "'";
							}
						}
						CommonUtils.addToQuerySearch(" (TAPPR.MQ_COLUMN_STATUS) IN (" + actData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.MQ_COLUMN_STATUS) IN (" + actData + ") ", strBufPending,
								data.getJoinType());
						break;
					case "recordIndicator":
						CommonUtils.addToQuerySearch(" upper(TAppr.RECORD_INDICATOR) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.RECORD_INDICATOR) " + val, strBufPending,
								data.getJoinType());
						break;
					case "recordIndicatorDesc":
						numSTData = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 7);
						actData = "";
						for (int k = 0; k < numSTData.size(); k++) {
							int numsubtab = numSTData.get(k).getNumSubTab();
							if (!ValidationUtil.isValid(actData)) {
								actData = "'" + Integer.toString(numsubtab) + "'";
							} else {
								actData = actData + ",'" + Integer.toString(numsubtab) + "'";
							}
						}
						CommonUtils.addToQuerySearch(" (TAPPR.RECORD_INDICATOR) IN (" + actData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.RECORD_INDICATOR) IN (" + actData + ") ", strBufPending,
								data.getJoinType());
						break;
					case "makerName":
						List<VisionUsersVb> muData = visionUsersDao.findUserIdByUserName(val);
						String actMData = "";
						for (int k = 0; k < muData.size(); k++) {
							int visId = muData.get(k).getVisionId();
							if (!ValidationUtil.isValid(actMData)) {
								actMData = "'" + visId + "'";
							} else {
								actMData = actMData + ",'" + visId + "'";
							}
						}
						CommonUtils.addToQuerySearch(" (TAPPR.MAKER) IN (" + actMData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.MAKER) IN (" + actMData + ") ", strBufPending,
								data.getJoinType());
						break;

					case "verifierName":
						muData = visionUsersDao.findUserIdByUserName(val);
						actMData = "";
						for (int k = 0; k < muData.size(); k++) {
							int visId = muData.get(k).getVisionId();
							if (!ValidationUtil.isValid(actMData)) {
								actMData = "'" + Integer.toString(visId) + "'";
							} else {
								actMData = actMData + ",'" + Integer.toString(visId) + "'";
							}
						}
						CommonUtils.addToQuerySearch(" (TAPPR.VERIFIER) IN (" + actMData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.VERIFIER) IN (" + actMData + ") ", strBufPending,
								data.getJoinType());
						break;

					default:
					}
					count++;
				}
			}
			String orderBy = " Order By CONNECTOR_ID, QUERY_ID, COLUMN_ID  ";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlMqColumnsVb> getQueryResults(EtlMqColumnsVb dObj, int intStatus) {
		setServiceDefaults();

		List<EtlMqColumnsVb> collTemp = null;

		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID,TAppr.QUERY_ID,TAppr.COLUMN_ID"
				+ ",TAppr.COLUMN_NAME,TAppr.COLUMN_SORT_ORDER,TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE,TAppr.COLUMN_LENGTH,TAppr.DATE_FORMAT_NT,TAppr.DATE_FORMAT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE  TAppr.COLUMN_DATATYPE = 'D'  AND NUM_TAB = 1103  AND NUM_SUB_TAB = "
				+ "   (CASE  WHEN (   TAppr.DATE_FORMAT = 0    OR TAppr.DATE_FORMAT IS NULL)  THEN 2   ELSE TAppr.DATE_FORMAT END))  DATE_FORMAT_DESC "
				+ ",TAppr.NUMBER_FORMAT_NT,TAppr.NUMBER_FORMAT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM NUM_SUB_TAB WHERE  NUM_TAB = 1102 AND NUM_SUB_TAB = (CASE WHEN (   TAppr.NUMBER_FORMAT = 0 OR TAppr.NUMBER_FORMAT IS NULL)"
				+ " THEN   (CASE WHEN (TAppr.COLUMN_DATATYPE = 'C') THEN 3  WHEN (TAppr.COLUMN_DATATYPE = 'N') THEN 2 ELSE 0	 END)  ELSE	 TAppr.NUMBER_FORMAT   END)) NUMBER_FORMAT_DESC"
				+ ",TAppr.PRIMARY_KEY_FLAG"
				+ ",TAppr.PARTITION_COLUMN_FLAG,TAppr.MQ_COLUMN_STATUS_NT,TAppr.MQ_COLUMN_STATUS,"
				+ MqColumnStatusNtApprDesc + ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc + ",TAppr.MAKER," + makerApprDesc + ",TAppr.VERIFIER," + verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS" + ", " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TAppr.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_MQ_COLUMNS TAppr WHERE  UPPER(TAPPR.CONNECTOR_ID) = ?  AND UPPER(TAPPR.QUERY_ID) = ?  AND COLUMN_ID = ?  ");
		String strQueryPend = new String("SELECT TPEND.CONNECTOR_ID ,TPEND.QUERY_ID ,TPEND.COLUMN_ID ,TPEND.COLUMN_NAME"
				+ ",TPEND.COLUMN_SORT_ORDER ,TPEND.COLUMN_DATATYPE_AT ,TPEND.COLUMN_DATATYPE"
				+ ",TPEND.COLUMN_LENGTH ,TPEND.DATE_FORMAT_NT ,TPEND.DATE_FORMAT ,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE  TPend.COLUMN_DATATYPE = 'D'  AND NUM_TAB = 1103  AND NUM_SUB_TAB = "
				+ "   (CASE  WHEN (   TPend.DATE_FORMAT = 0    OR TPend.DATE_FORMAT IS NULL)  THEN 2   ELSE TPend.DATE_FORMAT END))  DATE_FORMAT_DESC, "
				+ ",TPEND.NUMBER_FORMAT_NT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM NUM_SUB_TAB WHERE  NUM_TAB = 1102 AND NUM_SUB_TAB = (CASE WHEN (   TPend.NUMBER_FORMAT = 0 OR TPend.NUMBER_FORMAT IS NULL)"
				+ " THEN   (CASE WHEN (TPend.COLUMN_DATATYPE = 'C') THEN 3  WHEN (TPend.COLUMN_DATATYPE = 'N') THEN 2 ELSE 0	 END)  ELSE	 TPend.NUMBER_FORMAT   END)) NUMBER_FORMAT_DESC,"
				+ ",TPend.NUMBER_FORMAT,TPend.PRIMARY_KEY_FLAG,TPend.PARTITION_COLUMN_FLAG"
				+ ",TPend.MQ_COLUMN_STATUS_NT,TPend.MQ_COLUMN_STATUS," + MqColumnStatusNtPendDesc
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR," + RecordIndicatorNtPendDesc + ",TPend.MAKER,"
				+ makerPendDesc + ",TPend.VERIFIER," + verifierPendDesc + ",TPend.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " FROM ETL_MQ_COLUMNS_PEND TPEND WHERE UPPER(TPEND.CONNECTOR_ID) = ?  AND UPPER(TPEND.QUERY_ID) = ?  AND COLUMN_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getConnectorId().toUpperCase();
		objParams[1] = dObj.getQueryId().toUpperCase();
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
	protected List<EtlMqColumnsVb> selectApprovedRecord(EtlMqColumnsVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlMqColumnsVb> doSelectPendingRecord(EtlMqColumnsVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlMqColumnsVb records) {
		return records.getMqColumnStatus();
	}

	@Override
	protected void setStatus(EtlMqColumnsVb vObject, int status) {
		vObject.setMqColumnStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlMqColumnsVb vObject) {
		String query = "Insert Into ETL_MQ_COLUMNS ( CONNECTOR_ID, QUERY_ID, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, "
				+ "COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, "
				+ "PARTITION_COLUMN_FLAG, MQ_COLUMN_STATUS_NT, MQ_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, "
				+ "INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getConnectorId(), vObject.getQueryId(), vObject.getColumnId(),
				vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getMqColumnStatusNt(),
				vObject.getMqColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlMqColumnsVb vObject) {
		String query = "Insert Into ETL_MQ_COLUMNS_PEND (CONNECTOR_ID, QUERY_ID, COLUMN_ID, COLUMN_NAME, "
				+ "COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT, COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, "
				+ "NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG, MQ_COLUMN_STATUS_NT,"
				+ "MQ_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getConnectorId(), vObject.getQueryId(), vObject.getColumnId(),
				vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getMqColumnStatusNt(),
				vObject.getMqColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlMqColumnsVb vObject) {
		String query = "Insert Into ETL_MQ_COLUMNS_PEND ( CONNECTOR_ID, QUERY_ID, COLUMN_ID, COLUMN_NAME, COLUMN_SORT_ORDER, COLUMN_DATATYPE_AT,"
				+ " COLUMN_DATATYPE, COLUMN_LENGTH, DATE_FORMAT_NT, DATE_FORMAT, NUMBER_FORMAT_NT, NUMBER_FORMAT, PRIMARY_KEY_FLAG, PARTITION_COLUMN_FLAG,"
				+ " MQ_COLUMN_STATUS_NT, MQ_COLUMN_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getConnectorId(), vObject.getQueryId(), vObject.getColumnId(),
				vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getMqColumnStatusNt(),
				vObject.getMqColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);

	}

	@Override
	protected int doUpdateAppr(EtlMqColumnsVb vObject) {
		String query = "Update ETL_MQ_COLUMNS Set COLUMN_NAME = ?, COLUMN_SORT_ORDER = ?, COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, COLUMN_LENGTH = ?, "
				+ "DATE_FORMAT_NT = ?, DATE_FORMAT = ?, NUMBER_FORMAT_NT = ?, NUMBER_FORMAT = ?, PRIMARY_KEY_FLAG = ?, PARTITION_COLUMN_FLAG = ?,"
				+ "MQ_COLUMN_STATUS_NT = ?, MQ_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?,"
				+ " INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ "  WHERE  CONNECTOR_ID = ?  AND QUERY_ID = ?  AND COLUMN_ID = ? ";
		Object[] args = { vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getMqColumnStatusNt(),
				vObject.getMqColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getConnectorId(),
				vObject.getQueryId(), vObject.getColumnId(), };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlMqColumnsVb vObject) {
		String query = "Update ETL_MQ_COLUMNS_PEND Set COLUMN_NAME = ?, COLUMN_SORT_ORDER = ?, COLUMN_DATATYPE_AT = ?, COLUMN_DATATYPE = ?, COLUMN_LENGTH = ?,"
				+ " DATE_FORMAT_NT = ?, DATE_FORMAT = ?, NUMBER_FORMAT_NT = ?, NUMBER_FORMAT = ?, PRIMARY_KEY_FLAG = ?, PARTITION_COLUMN_FLAG = ?, "
				+ " MQ_COLUMN_STATUS_NT = ?, MQ_COLUMN_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, "
				+ "INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ "  WHERE CONNECTOR_ID = ?  AND QUERY_ID = ?  AND COLUMN_ID = ? ";
		Object[] args = { vObject.getColumnName(), vObject.getColumnSortOrder(), vObject.getColumnDatatypeAt(),
				vObject.getColumnDatatype(), vObject.getColumnLength(), vObject.getDateFormatNt(),
				vObject.getDateFormat(), vObject.getNumberFormatNt(), vObject.getNumberFormat(),
				vObject.getPrimaryKeyFlag(), vObject.getPartitionColumnFlag(), vObject.getMqColumnStatusNt(),
				vObject.getMqColumnStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getConnectorId(),
				vObject.getQueryId(), vObject.getColumnId(), };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlMqColumnsVb vObject) {
		String query = "Delete From ETL_MQ_COLUMNS Where CONNECTOR_ID = ?  AND QUERY_ID = ?  AND COLUMN_ID = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getQueryId(), vObject.getColumnId() };
		return getJdbcTemplate().update(query, args);
	}

	public int doDeleteParentAppr(EtlMqTableVb vObject) {
		String query = "Delete From ETL_MQ_COLUMNS Where CONNECTOR_ID = ?  AND QUERY_ID = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getQueryId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deletePendingParentRecord(EtlMqTableVb vObject) {
		String query = "Delete From ETL_MQ_COLUMNS_PEND Where CONNECTOR_ID = ?  AND QUERY_ID = ?   ";
		Object[] args = { vObject.getConnectorId(), vObject.getQueryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlMqColumnsVb vObject) {
		String query = "Delete From ETL_MQ_COLUMNS_PEND Where CONNECTOR_ID = ?  AND QUERY_ID = ?  AND COLUMN_ID = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getQueryId(), vObject.getColumnId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlMqColumnsVb vObject) {
		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");

		if (ValidationUtil.isValid(vObject.getConnectorId()))
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + vObject.getConnectorId().trim());
		else
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getQueryId()))
			strAudit.append("QUERY_ID" + auditDelimiterColVal + vObject.getQueryId().trim());
		else
			strAudit.append("QUERY_ID" + auditDelimiterColVal + "NULL");
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

		strAudit.append("DATE_FORMAT" + auditDelimiterColVal + vObject.getDateFormat());
		strAudit.append(auditDelimiter);

		strAudit.append("NUMBER_FORMAT_NT" + auditDelimiterColVal + vObject.getNumberFormatNt());
		strAudit.append(auditDelimiter);

		strAudit.append("NUMBER_FORMAT" + auditDelimiterColVal + vObject.getNumberFormat());
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

		strAudit.append("MQ_COLUMN_STATUS_NT" + auditDelimiterColVal + vObject.getMqColumnStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("MQ_COLUMN_STATUS" + auditDelimiterColVal + vObject.getMqColumnStatus());
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
		serviceName = "EtlMqColumns";
		serviceDesc = "ETL Mq Columns";
		tableName = "ETL_MQ_COLUMNS";
		childTableName = "ETL_MQ_COLUMNS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}

	public List<EtlMqColumnsVb> getQueryResultsAllColumns(String connectorId, String queryID, int intStatus) {
		setServiceDefaults();

		List<EtlMqColumnsVb> collTemp = null;

		final int intKeyFieldsCount = 2;
		String strQueryAppr = new String("Select TAppr.CONNECTOR_ID,TAppr.QUERY_ID,TAppr.COLUMN_ID"
				+ ",TAppr.COLUMN_NAME,TAppr.COLUMN_SORT_ORDER,TAppr.COLUMN_DATATYPE_AT"
				+ ",TAppr.COLUMN_DATATYPE,TAppr.COLUMN_LENGTH,TAppr.DATE_FORMAT_NT,TAppr.DATE_FORMAT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE  TAppr.COLUMN_DATATYPE = 'D'  AND NUM_TAB = 1103  AND NUM_SUB_TAB = "
				+ "   (CASE  WHEN ( TAppr.DATE_FORMAT = 0    OR TAppr.DATE_FORMAT IS NULL)  THEN 2   ELSE TAppr.DATE_FORMAT END))  DATE_FORMAT_DESC, "
				+ "TAppr.NUMBER_FORMAT_NT,TAppr.NUMBER_FORMAT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM NUM_SUB_TAB WHERE  NUM_TAB = 1102 AND NUM_SUB_TAB = (CASE WHEN (   TAppr.NUMBER_FORMAT = 0 OR TAppr.NUMBER_FORMAT IS NULL)"
				+ " THEN   (CASE WHEN (TAppr.COLUMN_DATATYPE = 'C') THEN 3  WHEN (TAppr.COLUMN_DATATYPE = 'N') THEN 2 ELSE 0	 END)  ELSE	 TAppr.NUMBER_FORMAT   END)) NUMBER_FORMAT_DESC"
				+ ",TAppr.PRIMARY_KEY_FLAG"
				+ ",TAppr.PARTITION_COLUMN_FLAG,TAppr.MQ_COLUMN_STATUS_NT,TAppr.MQ_COLUMN_STATUS,"
				+ MqColumnStatusNtApprDesc + ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc + ",TAppr.MAKER," + makerApprDesc + ",TAppr.VERIFIER," + verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS" + ", " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TAppr.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_MQ_COLUMNS TAppr WHERE  UPPER(TAPPR.CONNECTOR_ID) = ?  AND UPPER(TAPPR.QUERY_ID) = ?  ORDER BY COLUMN_ID ");
		String strQueryPend = new String("SELECT TPEND.CONNECTOR_ID ,TPEND.QUERY_ID ,TPEND.COLUMN_ID ,TPEND.COLUMN_NAME"
				+ ",TPEND.COLUMN_SORT_ORDER ,TPEND.COLUMN_DATATYPE_AT ,TPEND.COLUMN_DATATYPE"
				+ ",TPEND.COLUMN_LENGTH ,TPEND.DATE_FORMAT_NT ,TPEND.DATE_FORMAT ,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION FROM NUM_SUB_TAB WHERE  TPend.COLUMN_DATATYPE = 'D'  AND NUM_TAB = 1103  AND NUM_SUB_TAB = "
				+ "   (CASE  WHEN (   TPend.DATE_FORMAT = 0    OR TPend.DATE_FORMAT IS NULL)  THEN 2   ELSE TPend.DATE_FORMAT END))  DATE_FORMAT_DESC "
				+ ",TPEND.NUMBER_FORMAT_NT,"
				+ " (SELECT NUM_SUBTAB_DESCRIPTION  FROM NUM_SUB_TAB WHERE  NUM_TAB = 1102 AND NUM_SUB_TAB = (CASE WHEN (   TPend.NUMBER_FORMAT = 0 OR TPend.NUMBER_FORMAT IS NULL)"
				+ " THEN   (CASE WHEN (TPend.COLUMN_DATATYPE = 'C') THEN 3  WHEN (TPend.COLUMN_DATATYPE = 'N') THEN 2 ELSE 0	 END)  ELSE	 TPend.NUMBER_FORMAT   END)) NUMBER_FORMAT_DESC"
				+ ",TPend.NUMBER_FORMAT,TPend.PRIMARY_KEY_FLAG,TPend.PARTITION_COLUMN_FLAG"
				+ ",TPend.MQ_COLUMN_STATUS_NT,TPend.MQ_COLUMN_STATUS," + MqColumnStatusNtPendDesc
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR," + RecordIndicatorNtPendDesc + ",TPend.MAKER,"
				+ makerPendDesc + ",TPend.VERIFIER," + verifierPendDesc + ",TPend.INTERNAL_STATUS" + ", "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " FROM ETL_MQ_COLUMNS_PEND TPEND WHERE UPPER(TPEND.CONNECTOR_ID) = ?  AND UPPER(TPEND.QUERY_ID) = ? ORDER BY COLUMN_ID ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = connectorId.toUpperCase();
		objParams[1] = queryID.toUpperCase();

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

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_MQ_COLUMNS_PRS";
		String query = "Delete From " + table + " Where FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String table = "ETL_MQ_COLUMNS_PRS";
		String query = "Delete From " + table + " Where FEED_CATEGORY = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
}
