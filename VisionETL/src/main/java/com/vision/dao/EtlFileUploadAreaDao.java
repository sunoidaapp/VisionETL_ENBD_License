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
import com.vision.vb.EtlFileTableVb;
import com.vision.vb.EtlFileUploadAreaVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlFileUploadAreaDao extends AbstractDao<EtlFileUploadAreaVb> {

	@Autowired
	private NumSubTabDao numSubTabDao;
	@Autowired
	private VisionUsersDao visionUsersDao;
	@Autowired
	private EtlFileTableDao etlFileTableDao;

	/******* Mapper Start **********/

	String FileFormatAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2116, "TAppr.FILE_FORMAT",
			"FILE_FORMAT_DESC");
	String FileFormatAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2116, "TPend.FILE_FORMAT",
			"FILE_FORMAT_DESC");

	String FileTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2117, "TAppr.FILE_TYPE",
			"FILE_TYPE_DESC");
	String FileTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2117, "TPend.FILE_TYPE",
			"FILE_TYPE_DESC");

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
				EtlFileUploadAreaVb vObject = new EtlFileUploadAreaVb();
				vObject.setConnectorId(
						ValidationUtil.isValid(rs.getString("CONNECTOR_ID")) ? rs.getString("CONNECTOR_ID") : "");
				vObject.setFileId(ValidationUtil.isValid(rs.getString("FILE_ID")) ? rs.getString("FILE_ID") : "");
				vObject.setFileName(ValidationUtil.isValid(rs.getString("FILE_NAME")) ? rs.getString("FILE_NAME") : "");
				vObject.setFileFormatAt(rs.getInt("FILE_FORMAT_AT"));
				vObject.setFileFormat(
						ValidationUtil.isValid(rs.getString("FILE_FORMAT")) ? rs.getString("FILE_FORMAT") : "");
				vObject.setFileFormatDesc(rs.getString("FILE_FORMAT_DESC"));
				vObject.setFileTypeAt(rs.getInt("FILE_TYPE_AT"));
				vObject.setFileType(ValidationUtil.isValid(rs.getString("FILE_TYPE")) ? rs.getString("FILE_TYPE") : "");
				vObject.setFileTypeDesc(rs.getString("FILE_TYPE_DESC"));
				vObject.setFileContext(
						ValidationUtil.isValid(rs.getString("FILE_CONTEXT")) ? rs.getString("FILE_CONTEXT") : "");
				vObject.setFtpDetailScript(
						ValidationUtil.isValid(rs.getString("FTP_DETAIL_SCRIPT")) ? rs.getString("FTP_DETAIL_SCRIPT")
								: "");
				vObject.setFileStatusNt(rs.getInt("FILE_STATUS_NT"));
				vObject.setFileStatus(rs.getInt("FILE_STATUS"));
				vObject.setStatusDesc(rs.getString("FILE_STATUS_DESC"));
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
	public List<EtlFileUploadAreaVb> getQueryPopupResults(EtlFileUploadAreaVb dObj) {

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer(
				"SELECT TAPPR.CONNECTOR_ID, TAPPR.FILE_ID,TAPPR.FILE_NAME,TAPPR.FILE_FORMAT_AT,TAPPR.FILE_FORMAT,"
						+ FileFormatAtApprDesc + ",TAPPR.FILE_TYPE_AT,TAPPR.FILE_TYPE," + FileTypeAtApprDesc + ","
						+ getDbFunction(Constants.TO_CHAR, "TAppr.FILE_CONTEXT") + " FILE_CONTEXT,"
						+ getDbFunction(Constants.TO_CHAR, "TAppr.FTP_DETAIL_SCRIPT")
						+ " FTP_DETAIL_SCRIPT, TAPPR.FILE_STATUS_NT,TAPPR.FILE_STATUS," + FileStatusNtApprDesc
						+ ",TAPPR.RECORD_INDICATOR_NT,TAPPR.RECORD_INDICATOR," + RecordIndicatorNtApprDesc
						+ ",TAPPR.MAKER," + makerApprDesc + ",TAPPR.VERIFIER," + verifierApprDesc
						+ ",TAPPR.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TAPPR.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
						+ " FROM ETL_FILE_UPLOAD_AREA TAPPR ");

		StringBuffer strBufPending = new StringBuffer(
				"SELECT TPEND.CONNECTOR_ID, TPEND.FILE_ID,TPEND.FILE_NAME,TPEND.FILE_FORMAT_AT,TPEND.FILE_FORMAT,"
						+ FileFormatAtPendDesc + ",TPEND.FILE_TYPE_AT,TPEND.FILE_TYPE," + FileTypeAtPendDesc + ","
						+ getDbFunction(Constants.TO_CHAR, "TPEND.FILE_CONTEXT") + " FILE_CONTEXT,"
						+ getDbFunction(Constants.TO_CHAR, "TPEND.FTP_DETAIL_SCRIPT")
						+ " FTP_DETAIL_SCRIPT,TPEND.FILE_STATUS_NT,TPEND.FILE_STATUS," + FileStatusNtPendDesc
						+ ",TPEND.RECORD_INDICATOR_NT,TPEND.RECORD_INDICATOR," + RecordIndicatorNtPendDesc
						+ ",TPEND.MAKER," + makerPendDesc + ",TPEND.VERIFIER," + verifierPendDesc
						+ ",TPEND.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TPEND.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
						+ " FROM ETL_FILE_UPLOAD_AREA_PEND TPEND ");

		if (ValidationUtil.isValid(dObj.getConnectorId())) {
			params.addElement(dObj.getConnectorId().toUpperCase());
			CommonUtils.addToQuery("UPPER(TAPPR.CONNECTOR_ID) = ?", strBufApprove);
			CommonUtils.addToQuery("UPPER(TPEND.CONNECTOR_ID) = ?", strBufPending);
		}
		String strWhereNotExists = new String(
				" NOT EXISTS (SELECT 'X' FROM ETL_FILE_UPLOAD_AREA_PEND TPEND WHERE TAPPR.CONNECTOR_ID = TPEND.CONNECTOR_ID AND TAPPR.FILE_ID = TPEND.FILE_ID )");
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

					case "fileId":
						CommonUtils.addToQuerySearch(" upper(TAppr.FILE_ID) " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.FILE_ID) " + val, strBufPending, data.getJoinType());
						break;

					case "fileName":
						CommonUtils.addToQuerySearch(" upper(TAppr.FILE_NAME) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.FILE_NAME) " + val, strBufPending,
								data.getJoinType());
						break;

					case "fileFormat":
						CommonUtils.addToQuerySearch(" upper(TAppr.FILE_FORMAT) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.FILE_FORMAT) " + val, strBufPending,
								data.getJoinType());
						break;

					case "fileType":
						CommonUtils.addToQuerySearch(" upper(TAppr.FILE_TYPE) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.FILE_TYPE) " + val, strBufPending,
								data.getJoinType());
						break;

					/*
					 * case "ftpDetailScript":
					 * CommonUtils.addToQuerySearch(" upper(TAppr.FTP_DETAIL_SCRIPT) " + val,
					 * strBufApprove, data.getJoinType());
					 * CommonUtils.addToQuerySearch(" upper(TPend.FTP_DETAIL_SCRIPT) " + val,
					 * strBufPending, data.getJoinType()); break;
					 */

					case "fileStatus":
						CommonUtils.addToQuerySearch(" upper(TAppr.FILE_STATUS) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.FILE_STATUS) " + val, strBufPending,
								data.getJoinType());
						break;
					case "statusDesc":
						List<NumSubTabVb> numSTData = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 2000);
						String actData = "";
						for (int k = 0; k < numSTData.size(); k++) {
							int numsubtab = numSTData.get(k).getNumSubTab();
							if (!ValidationUtil.isValid(actData)) {
								actData = "'" + Integer.toString(numsubtab) + "'";
							} else {
								actData = actData + ",'" + Integer.toString(numsubtab) + "'";
							}
						}
						CommonUtils.addToQuerySearch(" (TAPPR.CONNECTION_STAUTS) IN (" + actData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.CONNECTION_STAUTS) IN (" + actData + ") ", strBufPending,
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
			String orderBy = " Order By CONNECTOR_ID, FILE_ID  ";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);

		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;

		}
	}

	public List<EtlFileUploadAreaVb> getQueryResults(EtlFileUploadAreaVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFileUploadAreaVb> collTemp = null;
		final int intKeyFieldsCount = 2;
		String strQueryAppr = new String("SELECT TAPPR.CONNECTOR_ID,TAPPR.FILE_ID,TAPPR.FILE_NAME"
				+ ",TAPPR.FILE_FORMAT_AT,TAPPR.FILE_FORMAT," + FileFormatAtApprDesc + ",TAPPR.FILE_TYPE_AT"
				+ ",TAPPR.FILE_TYPE," + FileTypeAtApprDesc + ","
				+ getDbFunction(Constants.TO_CHAR, "TAppr.FILE_CONTEXT") + " FILE_CONTEXT,"
				+ getDbFunction(Constants.TO_CHAR, "TAppr.FTP_DETAIL_SCRIPT") + " FTP_DETAIL_SCRIPT,"
				+ "TAPPR.FILE_STATUS_NT,TAPPR.FILE_STATUS," + FileStatusNtApprDesc
				+ ",TAPPR.RECORD_INDICATOR_NT,TAPPR.RECORD_INDICATOR," + RecordIndicatorNtApprDesc + ",TAPPR.MAKER,"
				+ makerApprDesc + ",TAPPR.VERIFIER," + verifierApprDesc + ",TAPPR.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_FILE_UPLOAD_AREA TAPPR WHERE UPPER(TAPPR.CONNECTOR_ID) = ?  AND UPPER(TAPPR.FILE_ID) = ?  ");
		String strQueryPend = new String("SELECT TPEND.CONNECTOR_ID,TPEND.FILE_ID,TPEND.FILE_NAME"
				+ ",TPEND.FILE_FORMAT_AT,TPEND.FILE_FORMAT," + FileFormatAtPendDesc + ",TPEND.FILE_TYPE_AT"
				+ ",TPEND.FILE_TYPE," + FileTypeAtPendDesc + ","
				+ getDbFunction(Constants.TO_CHAR, "TPEND.FILE_CONTEXT") + " FILE_CONTEXT,"
				+ getDbFunction(Constants.TO_CHAR, "TPEND.FTP_DETAIL_SCRIPT") + " FTP_DETAIL_SCRIPT,"
				+ "TPEND.FILE_STATUS_NT,TPEND.FILE_STATUS," + FileStatusNtPendDesc
				+ ",TPEND.RECORD_INDICATOR_NT,TPEND.RECORD_INDICATOR," + RecordIndicatorNtPendDesc + ",TPEND.MAKER,"
				+ makerPendDesc + ",TPEND.VERIFIER," + verifierPendDesc + ",TPEND.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
				+ " FROM ETL_FILE_UPLOAD_AREA_PEND TPEND WHERE UPPER(TPEND.CONNECTOR_ID) = ?  AND UPPER(TPEND.FILE_ID) = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getConnectorId().toUpperCase();
		objParams[1] = dObj.getFileId().toUpperCase();

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
	protected List<EtlFileUploadAreaVb> selectApprovedRecord(EtlFileUploadAreaVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFileUploadAreaVb> doSelectPendingRecord(EtlFileUploadAreaVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlFileUploadAreaVb records) {
		return records.getFileStatus();
	}

	@Override
	protected void setStatus(EtlFileUploadAreaVb vObject, int status) {
		vObject.setFileStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFileUploadAreaVb vObject) {
		if (vObject.getFileFormat().startsWith("A")) {
			vObject.setFileFormat("A");
		} else if (vObject.getFileFormat().startsWith("E")) {
			vObject.setFileFormat("E");
		} else if (vObject.getFileFormat().startsWith("C")) {
			vObject.setFileFormat("C");
		} else if (vObject.getFileFormat().startsWith("J")) {
			vObject.setFileFormat("J");
		} else if (vObject.getFileFormat().startsWith("P")) {
			vObject.setFileFormat("P");
		} else if (vObject.getFileFormat().startsWith("X")) {
			vObject.setFileFormat("X");
		}
		vObject.setFileFormatAt(2116);
		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		vObject.setFileTypeAt(2117);

		String query = "INSERT INTO ETL_FILE_UPLOAD_AREA (CONNECTOR_ID, FILE_ID, FILE_NAME, FILE_FORMAT_AT, FILE_FORMAT, FILE_TYPE_AT, FILE_TYPE, FILE_CONTEXT,"
				+ " FTP_DETAIL_SCRIPT, FILE_STATUS_NT, FILE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ") ";
		Object[] args = { vObject.getConnectorId(), vObject.getFileId(), vObject.getFileName(),
				vObject.getFileFormatAt(), vObject.getFileFormat(), vObject.getFileTypeAt(), vObject.getFileType(),
				vObject.getFileContext(), vObject.getFtpDetailScript(), vObject.getFileStatusNt(),
				vObject.getFileStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlFileUploadAreaVb vObject) {
		if (vObject.getFileFormat().startsWith("A")) {
			vObject.setFileFormat("A");
		} else if (vObject.getFileFormat().startsWith("E")) {
			vObject.setFileFormat("E");
		} else if (vObject.getFileFormat().startsWith("C")) {
			vObject.setFileFormat("C");
		} else if (vObject.getFileFormat().startsWith("J")) {
			vObject.setFileFormat("J");
		} else if (vObject.getFileFormat().startsWith("P")) {
			vObject.setFileFormat("P");
		} else if (vObject.getFileFormat().startsWith("X")) {
			vObject.setFileFormat("X");
		}
		vObject.setFileFormatAt(2116);
		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		vObject.setFileTypeAt(2117);

		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		String query = "INSERT INTO ETL_FILE_UPLOAD_AREA_PEND (CONNECTOR_ID, FILE_ID, FILE_NAME, FILE_FORMAT_AT, FILE_FORMAT, FILE_TYPE_AT, FILE_TYPE, FILE_CONTEXT,"
				+ " FTP_DETAIL_SCRIPT, FILE_STATUS_NT, FILE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ") ";
		Object[] args = { vObject.getConnectorId(), vObject.getFileId(), vObject.getFileName(),
				vObject.getFileFormatAt(), vObject.getFileFormat(), vObject.getFileTypeAt(), vObject.getFileType(),
				vObject.getFileContext(), vObject.getFtpDetailScript(), vObject.getFileStatusNt(),
				vObject.getFileStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFileUploadAreaVb vObject) {
		if (vObject.getFileFormat().startsWith("A")) {
			vObject.setFileFormat("A");
		} else if (vObject.getFileFormat().startsWith("E")) {
			vObject.setFileFormat("E");
		} else if (vObject.getFileFormat().startsWith("C")) {
			vObject.setFileFormat("C");
		} else if (vObject.getFileFormat().startsWith("J")) {
			vObject.setFileFormat("J");
		} else if (vObject.getFileFormat().startsWith("P")) {
			vObject.setFileFormat("P");
		} else if (vObject.getFileFormat().startsWith("X")) {
			vObject.setFileFormat("X");
		}
		vObject.setFileFormatAt(2116);
		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		vObject.setFileTypeAt(2117);

		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		String query = "INSERT INTO ETL_FILE_UPLOAD_AREA_PEND (CONNECTOR_ID, FILE_ID, FILE_NAME, FILE_FORMAT_AT, FILE_FORMAT, FILE_TYPE_AT, FILE_TYPE, FILE_CONTEXT,"
				+ " FTP_DETAIL_SCRIPT, FILE_STATUS_NT, FILE_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, "
				+ " DATE_LAST_MODIFIED, DATE_CREATION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " ) ";
		Object[] args = { vObject.getConnectorId(), vObject.getFileId(), vObject.getFileName(),
				vObject.getFileFormatAt(), vObject.getFileFormat(), vObject.getFileTypeAt(), vObject.getFileType(),
				vObject.getFileContext(), vObject.getFtpDetailScript(), vObject.getFileStatusNt(),
				vObject.getFileStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFileUploadAreaVb vObject) {
		if (vObject.getFileFormat().startsWith("A")) {
			vObject.setFileFormat("A");
		} else if (vObject.getFileFormat().startsWith("E")) {
			vObject.setFileFormat("E");
		} else if (vObject.getFileFormat().startsWith("C")) {
			vObject.setFileFormat("C");
		} else if (vObject.getFileFormat().startsWith("J")) {
			vObject.setFileFormat("J");
		} else if (vObject.getFileFormat().startsWith("P")) {
			vObject.setFileFormat("P");
		} else if (vObject.getFileFormat().startsWith("X")) {
			vObject.setFileFormat("X");
		}
		vObject.setFileFormatAt(2116);
		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		vObject.setFileTypeAt(2117);

		String query = "UPDATE ETL_FILE_UPLOAD_AREA SET FILE_NAME = ?, FILE_FORMAT_AT = ?, FILE_FORMAT = ?, "
				+ " FILE_TYPE_AT = ?, FILE_TYPE = ?, FILE_CONTEXT = ?, FTP_DETAIL_SCRIPT = ?, FILE_STATUS_NT = ?, FILE_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " WHERE CONNECTOR_ID = ?  AND FILE_ID = ? ";
		Object[] args = { vObject.getFileName(), vObject.getFileFormatAt(), vObject.getFileFormat(),
				vObject.getFileTypeAt(), vObject.getFileType(), vObject.getFileContext(), vObject.getFtpDetailScript(),
				vObject.getFileStatusNt(), vObject.getFileStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getConnectorId(), vObject.getFileId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFileUploadAreaVb vObject) {
		if (vObject.getFileFormat().startsWith("A")) {
			vObject.setFileFormat("A");
		} else if (vObject.getFileFormat().startsWith("E")) {
			vObject.setFileFormat("E");
		} else if (vObject.getFileFormat().startsWith("C")) {
			vObject.setFileFormat("C");
		} else if (vObject.getFileFormat().startsWith("J")) {
			vObject.setFileFormat("J");
		} else if (vObject.getFileFormat().startsWith("P")) {
			vObject.setFileFormat("P");
		} else if (vObject.getFileFormat().startsWith("X")) {
			vObject.setFileFormat("X");
		}
		vObject.setFileFormatAt(2116);
		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		vObject.setFileTypeAt(2117);
		String query = "UPDATE ETL_FILE_UPLOAD_AREA_PEND SET FILE_NAME = ?, FILE_FORMAT_AT = ?, FILE_FORMAT = ?, "
				+ " FILE_TYPE_AT = ?, FILE_TYPE = ?, FILE_CONTEXT = ?, FTP_DETAIL_SCRIPT = ?, FILE_STATUS_NT = ?, FILE_STATUS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, "
				+ " DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ "  WHERE CONNECTOR_ID = ?  AND FILE_ID = ? ";
		Object[] args = { vObject.getFileName(), vObject.getFileFormatAt(), vObject.getFileFormat(),
				vObject.getFileTypeAt(), vObject.getFileType(), vObject.getFileContext(), vObject.getFtpDetailScript(),
				vObject.getFileStatusNt(), vObject.getFileStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getConnectorId(), vObject.getFileId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFileUploadAreaVb vObject) {
		String query = "DELETE FROM ETL_FILE_UPLOAD_AREA WHERE CONNECTOR_ID = ?  AND FILE_ID = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getFileId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFileUploadAreaVb vObject) {
		String query = "DELETE FROM ETL_FILE_UPLOAD_AREA_PEND WHERE CONNECTOR_ID = ?  AND FILE_ID = ?  ";
		Object[] args = { vObject.getConnectorId(), vObject.getFileId() };
		return getJdbcTemplate().update(query, args);
	}

	protected int doDeleteApprAll(String connectorId) {
		String query = "DELETE FROM ETL_FILE_UPLOAD_AREA WHERE CONNECTOR_ID = ?  ";
		Object[] args = { connectorId };
		return getJdbcTemplate().update(query, args);
	}

	protected int deletePendingRecordAll(String connectorId) {
		String query = "DELETE FROM ETL_FILE_UPLOAD_AREA_PEND WHERE CONNECTOR_ID = ?    ";
		Object[] args = { connectorId };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FILE_UPLOAD_AREA_PRS";
		String query = "Delete From " + table + " Where FEED_CATEGORY =?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getFeedCategory(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFileUploadAreaVb vObject) {
		if (vObject.getFileFormat().startsWith("A")) {
			vObject.setFileFormat("A");
		} else if (vObject.getFileFormat().startsWith("E")) {
			vObject.setFileFormat("E");
		} else if (vObject.getFileFormat().startsWith("C")) {
			vObject.setFileFormat("C");
		} else if (vObject.getFileFormat().startsWith("J")) {
			vObject.setFileFormat("J");
		} else if (vObject.getFileFormat().startsWith("P")) {
			vObject.setFileFormat("P");
		} else if (vObject.getFileFormat().startsWith("X")) {
			vObject.setFileFormat("X");
		}
		vObject.setFileFormatAt(2116);
		if ("STATIC".equalsIgnoreCase(vObject.getFileType()) || "Static".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("S");
		}
		if ("DYNAMIC".equalsIgnoreCase(vObject.getFileType()) || "Dynamic".equalsIgnoreCase(vObject.getFileType())) {
			vObject.setFileType("D");
		}
		vObject.setFileTypeAt(2117);

		final String auditDelimiter = vObject.getAuditDelimiter();
		final String auditDelimiterColVal = vObject.getAuditDelimiterColVal();
		StringBuffer strAudit = new StringBuffer("");
		if (ValidationUtil.isValid(vObject.getConnectorId()))
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + vObject.getConnectorId().trim());
		else
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFileId()))
			strAudit.append("FILE_ID" + auditDelimiterColVal + vObject.getFileId().trim());
		else
			strAudit.append("FILE_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFileName()))
			strAudit.append("FILE_NAME" + auditDelimiterColVal + vObject.getFileName().trim());
		else
			strAudit.append("FILE_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FILE_FORMAT_AT" + auditDelimiterColVal + vObject.getFileFormatAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFileFormat()))
			strAudit.append("FILE_FORMAT" + auditDelimiterColVal + vObject.getFileFormat().trim());
		else
			strAudit.append("FILE_FORMAT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("FILE_TYPE_AT" + auditDelimiterColVal + vObject.getFileTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFileType()))
			strAudit.append("FILE_TYPE" + auditDelimiterColVal + vObject.getFileType().trim());
		else
			strAudit.append("FILE_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFtpDetailScript()))
			strAudit.append("FTP_DETAIL_SCRIPT" + auditDelimiterColVal + vObject.getFtpDetailScript().trim());
		else
			strAudit.append("FTP_DETAIL_SCRIPT" + auditDelimiterColVal + "NULL");
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
		serviceName = "EtlFileUploadArea";
		serviceDesc = "ETL File Upload Area";
		tableName = "ETL_FILE_UPLOAD_AREA";
		childTableName = "ETL_FILE_UPLOAD_AREA";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}

	@Override
	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doInsertApprRecordForNonTrans(EtlFileUploadAreaVb vObject) throws RuntimeCustomException {
		List<EtlFileUploadAreaVb> collTemp = null;
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
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0));
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
		// Insert Data for ETL_File_Table
		if (vObject.getFileTableList() != null && vObject.getFileTableList().size() > 0) {
			for (EtlFileTableVb lObject : vObject.getFileTableList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				exceptionCode = etlFileTableDao.doInsertApprRecordForNonTrans(lObject);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doInsertRecordForNonTrans(EtlFileUploadAreaVb vObject) throws RuntimeCustomException {
		List<EtlFileUploadAreaVb> collTemp = null;
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
		 * staticDeletionFlag = getStatus(((ArrayList<EtlFileUploadAreaVb>)
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
			EtlFileUploadAreaVb vObjectLocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
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
		// Insert Data for ETL_File_Table
		if (vObject.getFileTableList() != null && vObject.getFileTableList().size() > 0) {
			for (EtlFileTableVb lObject : vObject.getFileTableList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setVerifier(vObject.getVerifier());
				lObject.setDateCreation(vObject.getDateCreation());
				lObject.setDateLastModified(vObject.getDateLastModified());
				lObject.setRecordIndicator(vObject.getRecordIndicator());
				exceptionCode = etlFileTableDao.doInsertRecordForNonTrans(lObject);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		exceptionCode = getResultObject(retVal);
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doDeleteApprRecordForNonTrans(EtlFileUploadAreaVb vObject) throws RuntimeCustomException {
		List<EtlFileUploadAreaVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		EtlFileUploadAreaVb vObjectlocal = null;
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
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
		EtlFileTableVb childObj = new EtlFileTableVb();
		childObj.setConnectorId(vObject.getConnectorId());
		childObj.setFileId(vObject.getFileId());
		List<EtlFileTableVb> mainTableList = etlFileTableDao.getQueryResultsByFileID(childObj, Constants.STATUS_ZERO);
		for (EtlFileTableVb lObject : mainTableList) {
			lObject.setStaticDelete(false);
			exceptionCode = etlFileTableDao.doDeleteApprRecordForNonTrans(lObject);
			retVal = exceptionCode.getErrorCode();
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		List<EtlFileTableVb> pendTbleList = etlFileTableDao.getQueryResultsByFileID(childObj, Constants.STATUS_PENDING);
		for (EtlFileTableVb lObject : pendTbleList) {
			lObject.setStaticDelete(false);
			exceptionCode = etlFileTableDao.doRejectRecord(lObject);
			retVal = exceptionCode.getErrorCode();
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doRejectRecord(EtlFileUploadAreaVb vObject) throws RuntimeCustomException {
		EtlFileUploadAreaVb vObjectlocal = null;
		List<EtlFileUploadAreaVb> collTemp = null;
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
			vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
			// Delete the record from the Pending table
			if (deletePendingRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			String ConnectorID = vObjectlocal.getConnectorId();
			if (!ValidationUtil.isValid(ConnectorID)) {
				ConnectorID = vObject.getConnectorId();
			}
			EtlFileTableVb childObj = new EtlFileTableVb();
			childObj.setConnectorId(vObject.getConnectorId());
			childObj.setFileId(vObject.getFileId());
			List<EtlFileTableVb> fileTableList = etlFileTableDao.getQueryResultsByFileID(childObj,
					Constants.STATUS_PENDING);
			if (fileTableList != null && fileTableList.size() > 0) {
				for (EtlFileTableVb lObject : fileTableList) {
					lObject.setStaticDelete(false);
					exceptionCode = etlFileTableDao.doRejectRecord(lObject);
					retVal = exceptionCode.getErrorCode();
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(retVal);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
			/*
			 * fileTableList = etlFileTableDao.getQueryResultsByFileID(childObj,
			 * Constants.STATUS_ZERO); if (fileTableList != null && fileTableList.size() >
			 * 0) { for (EtlFileTableVb lObject : fileTableList) {
			 * lObject.setStaticDelete(false); exceptionCode =
			 * etlFileTableDao.doDeleteApprRecordForNonTrans(lObject); retVal =
			 * exceptionCode.getErrorCode(); if (retVal != Constants.SUCCESSFUL_OPERATION) {
			 * exceptionCode = getResultObject(retVal); throw
			 * buildRuntimeCustomException(exceptionCode); } } }
			 */
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
	public ExceptionCode doApproveRecord(EtlFileUploadAreaVb vObject, boolean staticDelete)
			throws RuntimeCustomException {
		EtlFileUploadAreaVb oldContents = null;
		EtlFileUploadAreaVb vObjectlocal = null;
		List<EtlFileUploadAreaVb> collTemp = null;
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

			vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);

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
				oldContents = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
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

	public List<EtlFileUploadAreaVb> getQueryResultsByParent(String ConnectorID, int intStatus) {

		setServiceDefaults();

		List<EtlFileUploadAreaVb> collTemp = null;

		final int intKeyFieldsCount = 1;
		String strQueryAppr = new String(
				"SELECT TAPPR.CONNECTOR_ID,TAPPR.FILE_ID,TAPPR.FILE_NAME,TAPPR.FILE_FORMAT_AT,TAPPR.FILE_FORMAT,"
						+ FileFormatAtApprDesc + ",TAPPR.FILE_TYPE_AT,TAPPR.FILE_TYPE," + FileTypeAtApprDesc + ","
						+ getDbFunction(Constants.TO_CHAR, "TAPPR.FILE_CONTEXT") + " FILE_CONTEXT,"
						+ getDbFunction(Constants.TO_CHAR, "TAPPR.FTP_DETAIL_SCRIPT")
						+ " FTP_DETAIL_SCRIPT,TAPPR.FILE_STATUS_NT,TAPPR.FILE_STATUS," + FileStatusNtApprDesc
						+ ",TAPPR.RECORD_INDICATOR_NT,TAPPR.RECORD_INDICATOR," + RecordIndicatorNtApprDesc
						+ ",TAPPR.MAKER," + makerApprDesc + ",TAPPR.VERIFIER," + verifierApprDesc
						+ ",TAPPR.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TAPPR.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
						+ " FROM ETL_FILE_UPLOAD_AREA TAPPR WHERE UPPER(TAPPR.CONNECTOR_ID) = ? ");
		String strQueryPend = new String(
				"SELECT TPEND.CONNECTOR_ID,TPEND.FILE_ID,TPEND.FILE_NAME,TPEND.FILE_FORMAT_AT,TPEND.FILE_FORMAT,"
						+ FileFormatAtPendDesc + ",TPEND.FILE_TYPE_AT,TPEND.FILE_TYPE," + FileTypeAtPendDesc + ","
						+ getDbFunction(Constants.TO_CHAR, "TPEND.FILE_CONTEXT") + " FILE_CONTEXT,"
						+ getDbFunction(Constants.TO_CHAR, "TPEND.FTP_DETAIL_SCRIPT")
						+ " FTP_DETAIL_SCRIPT,TPEND.FILE_STATUS_NT,TPEND.FILE_STATUS," + FileStatusNtPendDesc
						+ ",TPEND.RECORD_INDICATOR_NT,TPEND.RECORD_INDICATOR," + RecordIndicatorNtPendDesc
						+ ",TPEND.MAKER," + makerPendDesc + ",TPEND.VERIFIER," + verifierPendDesc
						+ ",TPEND.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TPEND.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
						+ " FROM ETL_FILE_UPLOAD_AREA_PEND TPEND WHERE UPPER(TPEND.CONNECTOR_ID) = ? ");

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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doDeleteRecordForNonTrans(EtlFileUploadAreaVb vObject) throws RuntimeCustomException {
		EtlFileUploadAreaVb vObjectlocal = null;
		List<EtlFileUploadAreaVb> collTemp = null;
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
			vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
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
			vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
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
		EtlFileTableVb childObj = new EtlFileTableVb();
		childObj.setConnectorId(vObject.getConnectorId());
		childObj.setFileId(vObject.getFileId());
		List<EtlFileTableVb> mainTableList = etlFileTableDao.getQueryResultsByFileID(childObj, Constants.STATUS_ZERO);
		for (EtlFileTableVb lObject : mainTableList) {
			lObject.setStaticDelete(vObject.isStaticDelete());
			exceptionCode = etlFileTableDao.doDeleteRecordForNonTrans(lObject);
			retVal = exceptionCode.getErrorCode();
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(retVal);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doUpdateApprRecordForNonTrans(EtlFileUploadAreaVb vObject) throws RuntimeCustomException {
		List<EtlFileUploadAreaVb> collTemp = null;
		EtlFileUploadAreaVb vObjectlocal = null;
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
		vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
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
		if (vObject.getFileTableList() != null && vObject.getFileTableList().size() > 0) {
			for (EtlFileTableVb lObject : vObject.getFileTableList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				if (lObject.isNewRecord()) {
					strCurrentOperation = Constants.ADD;
					exceptionCode = etlFileTableDao.doInsertApprRecordForNonTrans(lObject);
				} else {
					strCurrentOperation = Constants.MODIFY;
					exceptionCode = etlFileTableDao.doUpdateApprRecordForNonTrans(lObject);
				}
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doUpdateRecordForNonTrans(EtlFileUploadAreaVb vObject) throws RuntimeCustomException {
		List<EtlFileUploadAreaVb> collTemp = null;
		EtlFileUploadAreaVb vObjectlocal = null;
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
			vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);

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
				vObjectlocal = ((ArrayList<EtlFileUploadAreaVb>) collTemp).get(0);
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
		if (vObject.getFileTableList() != null && vObject.getFileTableList().size() > 0) {
			for (EtlFileTableVb lObject : vObject.getFileTableList()) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				if (lObject.isNewRecord()) {
					strCurrentOperation = Constants.ADD;
					exceptionCode = etlFileTableDao.doInsertRecordForNonTrans(lObject);
				} else {
					strCurrentOperation = Constants.MODIFY;
					exceptionCode = etlFileTableDao.doUpdateRecordForNonTrans(lObject);
				}
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(retVal);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String table = "ETL_FILE_UPLOAD_AREA_PRS";
		String query = "Delete From " + table + " Where FEED_CATEGORY =?   AND SESSION_ID=? ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
}
