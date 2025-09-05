package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlFeedGenericAlertVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlFeedGenericAlertDao extends AbstractDao<EtlFeedGenericAlertVb> {

	@Autowired
	private VisionUsersDao visionUsersDao;

	@Autowired
	private NumSubTabDao numSubTabDao;

	/******* Mapper Start **********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String CategoryStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.ETL_AUDIT_STATUS",
			"CATEGORY_STATUS_DESC");
	String CategoryStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.ETL_AUDIT_STATUS",
			"CATEGORY_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedGenericAlertVb vObject = new EtlFeedGenericAlertVb();
				if (rs.getString("COUNTRY") != null) {
					vObject.setCountry(rs.getString("COUNTRY"));
				} else {
					vObject.setCountry("");
				}
				if (rs.getString("COUNTRY_DESC") != null) {
					vObject.setCountryDesc(rs.getString("COUNTRY_DESC"));
				} else {
					vObject.setCountryDesc("");
				}
				if (rs.getString("LE_BOOK") != null) {
					vObject.setLeBook(rs.getString("LE_BOOK"));
				} else {
					vObject.setLeBook("");
				}

				if (rs.getString("LE_BOOK_DESC") != null) {
					vObject.setLeBookDesc(rs.getString("LE_BOOK_DESC"));
				} else {
					vObject.setLeBookDesc("");
				}

				if (rs.getString("CATEGORY_ID") != null) {
					vObject.setCategoryId(rs.getString("CATEGORY_ID"));
				} else {
					vObject.setCategoryId("");
				}
				vObject.setLvl1AlertMailId(
						ValidationUtil.isValid(rs.getString("LVL1_ALERT_MAIL_ID")) ? rs.getString("LVL1_ALERT_MAIL_ID")
								: "");
				vObject.setLvl1AlertDescription(ValidationUtil.isValid(rs.getString("LVL1_ALERT_DESCRIPTION"))
						? rs.getString("LVL1_ALERT_DESCRIPTION")
						: "");

				vObject.setLvl2AlertMailId(
						ValidationUtil.isValid(rs.getString("LVL2_ALERT_MAIL_ID")) ? rs.getString("LVL2_ALERT_MAIL_ID")
								: "");
				vObject.setLvl2AlertDescription(ValidationUtil.isValid(rs.getString("LVL2_ALERT_DESCRIPTION"))
						? rs.getString("LVL2_ALERT_DESCRIPTION")
						: "");

				vObject.setLvl3AlertMailId(
						ValidationUtil.isValid(rs.getString("LVL3_ALERT_MAIL_ID")) ? rs.getString("LVL3_ALERT_MAIL_ID")
								: "");
				vObject.setLvl3AlertDescription(ValidationUtil.isValid(rs.getString("LVL3_ALERT_DESCRIPTION"))
						? rs.getString("LVL3_ALERT_DESCRIPTION")
						: "");

				vObject.setReinitiatedByFlag(rs.getString("REINITIATED_BY_FLAG"));
				vObject.setTerminatedByFlag(rs.getString("TERMINATED_BY_FLAG"));
				vObject.setCompletionStatusFlag(rs.getString("COMPLETION_STATUS_FLAG"));
				vObject.setStartTimeFlag(rs.getString("START_TIME_FLAG"));
				vObject.setEndTimeFlag(rs.getString("END_TIME_FLAG"));

				vObject.setEtlAuditStatusNt(rs.getInt("ETL_AUDIT_STATUS_NT"));
				vObject.setEtlAuditStatus(rs.getInt("ETL_AUDIT_STATUS"));
				vObject.setStatusDesc(rs.getString("CATEGORY_STATUS_DESC"));
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
	public List<EtlFeedGenericAlertVb> getQueryPopupResults(EtlFeedGenericAlertVb dObj) {
		setServiceDefaults();
		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("SELECT TAPPR.COUNTRY," + CountryApprDesc + ",TAPPR.LE_BOOK,"
				+ LeBookApprDesc + ",TAPPR.CATEGORY_ID," + getDbFunction(Constants.TO_CHAR, "TAppr.LVL1_ALERT_MAIL_ID")
				+ " LVL1_ALERT_MAIL_ID," + getDbFunction(Constants.TO_CHAR, "TAppr.LVL1_ALERT_DESCRIPTION")
				+ " LVL1_ALERT_DESCRIPTION," + getDbFunction(Constants.TO_CHAR, "TAppr.LVL2_ALERT_MAIL_ID")
				+ " LVL2_ALERT_MAIL_ID," + getDbFunction(Constants.TO_CHAR, "TAppr.LVL2_ALERT_DESCRIPTION")
				+ " LVL2_ALERT_DESCRIPTION," + getDbFunction(Constants.TO_CHAR, "TAppr.LVL3_ALERT_MAIL_ID")
				+ " LVL3_ALERT_MAIL_ID," + getDbFunction(Constants.TO_CHAR, "TAppr.LVL3_ALERT_DESCRIPTION")
				+ " LVL3_ALERT_DESCRIPTION," + "TAPPR.TERMINATED_BY_FLAG, TAPPR.REINITIATED_BY_FLAG "
				+ ",TAPPR.COMPLETION_STATUS_FLAG, TAPPR.START_TIME_FLAG "
				+ ",TAPPR.END_TIME_FLAG,TAPPR.ETL_AUDIT_STATUS_NT ,TAPPR.ETL_AUDIT_STATUS," + CategoryStatusNtApprDesc
				+ ",TAPPR.RECORD_INDICATOR_NT,TAPPR.RECORD_INDICATOR," + RecordIndicatorNtApprDesc + ",TAPPR.MAKER, "
				+ makerApprDesc + ",TAPPR.VERIFIER, " + verifierApprDesc + ",TAPPR.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION FROM ETL_FEED_GENERIC_ALERT TAPPR ");

		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_FEED_GENERIC_ALERT_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.CATEGORY_ID = TPend.CATEGORY_ID )");
		StringBuffer strBufPending = new StringBuffer("SELECT TPEND.COUNTRY," + CountryPendDesc + ",TPEND.LE_BOOK,"
				+ LeBookTPendDesc + ",TPEND.CATEGORY_ID ,"
				+ getDbFunction(Constants.TO_CHAR, "TPEND.LVL1_ALERT_MAIL_ID") + " LVL1_ALERT_MAIL_ID,"
				+ getDbFunction(Constants.TO_CHAR, "TPEND.LVL1_ALERT_DESCRIPTION") + " LVL1_ALERT_DESCRIPTION,"
				+ getDbFunction(Constants.TO_CHAR, "TPEND.LVL2_ALERT_MAIL_ID") + " LVL2_ALERT_MAIL_ID,"
				+ getDbFunction(Constants.TO_CHAR, "TPEND.LVL2_ALERT_DESCRIPTION") + " LVL2_ALERT_DESCRIPTION,"
				+ getDbFunction(Constants.TO_CHAR, "TPEND.LVL3_ALERT_MAIL_ID") + " LVL3_ALERT_MAIL_ID,"
				+ getDbFunction(Constants.TO_CHAR, "TPEND.LVL3_ALERT_DESCRIPTION") + " LVL3_ALERT_DESCRIPTION,"
				+ "TPEND.TERMINATED_BY_FLAG, TPEND.REINITIATED_BY_FLAG "
				+ ",TPEND.COMPLETION_STATUS_FLAG, TPEND.START_TIME_FLAG "
				+ ",TPEND.END_TIME_FLAG,TPEND.ETL_AUDIT_STATUS_NT ,TPEND.ETL_AUDIT_STATUS," + CategoryStatusNtPendDesc
				+ ",TPEND.RECORD_INDICATOR_NT,TPEND.RECORD_INDICATOR, " + RecordIndicatorNtPendDesc + ",TPEND.MAKER, "
				+ makerPendDesc + ",TPEND.VERIFIER, " + verifierPendDesc + ",TPEND.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION " + "FROM ETL_FEED_GENERIC_ALERT_PEND TPEND ");

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
					case "country":
						CommonUtils.addToQuerySearch(" upper(TAppr.COUNTRY) " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.COUNTRY) " + val, strBufPending, data.getJoinType());
						break;

					case "countryDesc":
						List<EtlFeedGenericAlertVb> countryData = findCountryByCustomFilter(val);
						String counData = "";
						for (int k = 0; k < countryData.size(); k++) {
							String country = countryData.get(k).getCountry();
							if (!ValidationUtil.isValid(counData)) {
								counData = "'" + country + "'";
							} else {
								counData = counData + ",'" + country + "'";
							}
						}
						CommonUtils.addToQuerySearch(" (TAPPR.COUNTRY) IN (" + counData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.COUNTRY) IN (" + counData + ") ", strBufPending,
								data.getJoinType());
						break;
					case "leBook":
						CommonUtils.addToQuerySearch(" upper(TAppr.LE_BOOK) " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LE_BOOK) " + val, strBufPending, data.getJoinType());
						break;
					case "leBookDesc":
						List<EtlFeedGenericAlertVb> leBookData = findLeBookByCustomFilter(val);
						String lebData = "";
						for (int k = 0; k < leBookData.size(); k++) {
							String Lebook = leBookData.get(k).getLeBook();
							if (!ValidationUtil.isValid(lebData)) {
								lebData = "'" + Lebook + "'";
							} else {
								lebData = lebData + ",'" + Lebook + "'";
							}
						}
						CommonUtils.addToQuerySearch(" (TAPPR.LE_BOOK) IN (" + lebData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.LE_BOOK) IN (" + lebData + ") ", strBufPending,
								data.getJoinType());
						break;

					case "categoryId":
						CommonUtils.addToQuerySearch(" upper(TAppr.CATEGORY_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.CATEGORY_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "lvl1AlertMailId":
						CommonUtils.addToQuerySearch(" upper(TAppr.LVL1_ALERT_MAIL_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LVL1_ALERT_MAIL_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "lvl1AlertDescription":
						CommonUtils.addToQuerySearch(" upper(TAppr.LVL1_ALERT_DESCRIPTION) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LVL1_ALERT_DESCRIPTION) " + val, strBufPending,
								data.getJoinType());
						break;

					case "lvl2AlertMailId":
						CommonUtils.addToQuerySearch(" upper(TAppr.LVL2_ALERT_MAIL_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LVL2_ALERT_MAIL_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "lvl2AlertDescription":
						CommonUtils.addToQuerySearch(" upper(TAppr.LVL2_ALERT_DESCRIPTION) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LVL2_ALERT_DESCRIPTION) " + val, strBufPending,
								data.getJoinType());
						break;

					case "lvl3AlertMailId":
						CommonUtils.addToQuerySearch(" upper(TAppr.LVL3_ALERT_MAIL_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LVL3_ALERT_MAIL_ID) " + val, strBufPending,
								data.getJoinType());
						break;

					case "lvl3AlertDescription":
						CommonUtils.addToQuerySearch(" upper(TAppr.LVL3_ALERT_DESCRIPTION) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LVL3_ALERT_DESCRIPTION) " + val, strBufPending,
								data.getJoinType());
						break;

					case "etlAuditStatus":
						CommonUtils.addToQuerySearch(" upper(TAppr.ETL_AUDIT_STATUS) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.ETL_AUDIT_STATUS) " + val, strBufPending,
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
						CommonUtils.addToQuerySearch(" (TAPPR.ETL_AUDIT_STATUS) IN (" + actData + ") ", strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" (TPEND.ETL_AUDIT_STATUS) IN (" + actData + ") ", strBufPending,
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
			String orderBy = " Order By COUNTRY, LE_BOOK, CATEGORY_ID";
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlFeedGenericAlertVb> getQueryResults(EtlFeedGenericAlertVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedGenericAlertVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY," + CountryApprDesc + ",TAppr.LE_BOOK, " + LeBookApprDesc
				+ ",TAppr.CATEGORY_ID,TAppr.LVL1_ALERT_MAIL_ID, TAppr.LVL1_ALERT_DESCRIPTION "
				+ ",TAppr.LVL2_ALERT_MAIL_ID, TAppr.LVL2_ALERT_DESCRIPTION "
				+ ",TAppr.LVL3_ALERT_MAIL_ID, TAppr.LVL3_ALERT_DESCRIPTION "
				+ ",TAppr.TERMINATED_BY_FLAG, TAppr.REINITIATED_BY_FLAG "
				+ ",TAppr.COMPLETION_STATUS_FLAG, TAppr.START_TIME_FLAG "
				+ ",TAppr.END_TIME_FLAG,TAppr.ETL_AUDIT_STATUS_NT ,TAppr.ETL_AUDIT_STATUS," + CategoryStatusNtApprDesc
				+ ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR, " + RecordIndicatorNtApprDesc + ",TAppr.MAKER, "
				+ makerApprDesc + ",TAppr.VERIFIER, " + verifierApprDesc + ",TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION "
				+ "From ETL_FEED_GENERIC_ALERT TAppr WHERE UPPER(TAppr.COUNTRY) = ?  AND UPPER(TAppr.LE_BOOK) = ?  AND UPPER(TAppr.CATEGORY_ID) = ?   ");
		String strQueryPend = new String("Select TPend.COUNTRY," + CountryPendDesc + ",TPend.LE_BOOK," + LeBookTPendDesc
				+ ",TPend.CATEGORY_ID,TPend.LVL1_ALERT_MAIL_ID, TPend.LVL1_ALERT_DESCRIPTION "
				+ ",TPend.LVL2_ALERT_MAIL_ID, TPend.LVL2_ALERT_DESCRIPTION "
				+ ",TPend.LVL3_ALERT_MAIL_ID, TPend.LVL3_ALERT_DESCRIPTION "
				+ ",TPend.TERMINATED_BY_FLAG, TPend.REINITIATED_BY_FLAG "
				+ ",TPend.COMPLETION_STATUS_FLAG, TPend.START_TIME_FLAG "
				+ ",TPend.END_TIME_FLAG,TPend.ETL_AUDIT_STATUS_NT ,TPend.ETL_AUDIT_STATUS," + CategoryStatusNtPendDesc
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR," + RecordIndicatorNtPendDesc + ",TPend.MAKER, "
				+ makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc + ",TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION "
				+ "From ETL_FEED_GENERIC_ALERT_PEND TPend WHERE UPPER(TPend.COUNTRY) = ?  AND UPPER(TPend.LE_BOOK) = ?  AND UPPER(TPend.CATEGORY_ID) = ?   ");

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
	protected List<EtlFeedGenericAlertVb> selectApprovedRecord(EtlFeedGenericAlertVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedGenericAlertVb> doSelectPendingRecord(EtlFeedGenericAlertVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlFeedGenericAlertVb records) {
		return records.getEtlAuditStatus();
	}

	@Override
	protected void setStatus(EtlFeedGenericAlertVb vObject, int status) {
		vObject.setEtlAuditStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFeedGenericAlertVb vObject) {
		String query = "Insert Into ETL_FEED_GENERIC_ALERT (COUNTRY, LE_BOOK, CATEGORY_ID "
				+ ",LVL1_ALERT_MAIL_ID, LVL1_ALERT_DESCRIPTION ,LVL2_ALERT_MAIL_ID, LVL2_ALERT_DESCRIPTION "
				+ ",LVL3_ALERT_MAIL_ID, LVL3_ALERT_DESCRIPTION ,TERMINATED_BY_FLAG, REINITIATED_BY_FLAG "
				+ ",COMPLETION_STATUS_FLAG, START_TIME_FLAG ,END_TIME_FLAG, ETL_AUDIT_STATUS_NT "
				+ ",ETL_AUDIT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(),
				vObject.getLvl1AlertMailId(), vObject.getLvl1AlertDescription(), vObject.getLvl2AlertMailId(),
				vObject.getLvl2AlertDescription(), vObject.getLvl3AlertMailId(), vObject.getLvl3AlertDescription(),
				vObject.getTerminatedByFlag(), vObject.getReinitiatedByFlag(), vObject.getCompletionStatusFlag(),
				vObject.getStartTimeFlag(), vObject.getEndTimeFlag(), vObject.getEtlAuditStatusNt(),
				vObject.getEtlAuditStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlFeedGenericAlertVb vObject) {
		String query = "Insert Into ETL_FEED_GENERIC_ALERT_PEND (COUNTRY, LE_BOOK, CATEGORY_ID"
				+ ",LVL1_ALERT_MAIL_ID, LVL1_ALERT_DESCRIPTION ,LVL2_ALERT_MAIL_ID, LVL2_ALERT_DESCRIPTION "
				+ ",LVL3_ALERT_MAIL_ID, LVL3_ALERT_DESCRIPTION ,TERMINATED_BY_FLAG, REINITIATED_BY_FLAG "
				+ ",COMPLETION_STATUS_FLAG, START_TIME_FLAG ,END_TIME_FLAG, ETL_AUDIT_STATUS_NT "
				+ ",ETL_AUDIT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(),
				vObject.getLvl1AlertMailId(), vObject.getLvl1AlertDescription(), vObject.getLvl2AlertMailId(),
				vObject.getLvl2AlertDescription(), vObject.getLvl3AlertMailId(), vObject.getLvl3AlertDescription(),
				vObject.getTerminatedByFlag(), vObject.getReinitiatedByFlag(), vObject.getCompletionStatusFlag(),
				vObject.getStartTimeFlag(), vObject.getEndTimeFlag(), vObject.getEtlAuditStatusNt(),
				vObject.getEtlAuditStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFeedGenericAlertVb vObject) {
		String query = "Insert Into ETL_FEED_GENERIC_ALERT_PEND (COUNTRY, LE_BOOK, CATEGORY_ID"
				+ ",LVL1_ALERT_MAIL_ID, LVL1_ALERT_DESCRIPTION ,LVL2_ALERT_MAIL_ID, LVL2_ALERT_DESCRIPTION "
				+ ",LVL3_ALERT_MAIL_ID, LVL3_ALERT_DESCRIPTION ,TERMINATED_BY_FLAG, REINITIATED_BY_FLAG "
				+ ",COMPLETION_STATUS_FLAG, START_TIME_FLAG ,END_TIME_FLAG, ETL_AUDIT_STATUS_NT "
				+ ",ETL_AUDIT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(),
				vObject.getLvl1AlertMailId(), vObject.getLvl1AlertDescription(), vObject.getLvl2AlertMailId(),
				vObject.getLvl2AlertDescription(), vObject.getLvl3AlertMailId(), vObject.getLvl3AlertDescription(),
				vObject.getTerminatedByFlag(), vObject.getReinitiatedByFlag(), vObject.getCompletionStatusFlag(),
				vObject.getStartTimeFlag(), vObject.getEndTimeFlag(), vObject.getEtlAuditStatusNt(),
				vObject.getEtlAuditStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFeedGenericAlertVb vObject) {
		String query = "Update ETL_FEED_GENERIC_ALERT Set LVL1_ALERT_MAIL_ID = ?, LVL1_ALERT_DESCRIPTION = ?,"
				+ "LVL2_ALERT_MAIL_ID = ?, LVL2_ALERT_DESCRIPTION = ?,"
				+ "LVL3_ALERT_MAIL_ID = ?, LVL3_ALERT_DESCRIPTION = ?,"
				+ "TERMINATED_BY_FLAG = ?, REINITIATED_BY_FLAG = ?,"
				+ "COMPLETION_STATUS_FLAG = ?, START_TIME_FLAG = ?, "
				+ "END_TIME_FLAG = ?, ETL_AUDIT_STATUS_NT = ?,  ETL_AUDIT_STATUS =?,"
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ? ";
		Object[] args = { vObject.getLvl1AlertMailId(), vObject.getLvl1AlertDescription(), vObject.getLvl2AlertMailId(),
				vObject.getLvl2AlertDescription(), vObject.getLvl3AlertMailId(), vObject.getLvl3AlertDescription(),
				vObject.getTerminatedByFlag(), vObject.getReinitiatedByFlag(), vObject.getCompletionStatusFlag(),
				vObject.getStartTimeFlag(), vObject.getEndTimeFlag(), vObject.getEtlAuditStatusNt(),
				vObject.getEtlAuditStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getCategoryId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFeedGenericAlertVb vObject) {
		String query = "Update ETL_FEED_GENERIC_ALERT_PEND Set LVL1_ALERT_MAIL_ID = ?, LVL1_ALERT_DESCRIPTION = ?,"
				+ "LVL2_ALERT_MAIL_ID = ?, LVL2_ALERT_DESCRIPTION = ?,"
				+ "LVL3_ALERT_MAIL_ID = ?, LVL3_ALERT_DESCRIPTION = ?,"
				+ "TERMINATED_BY_FLAG = ?, REINITIATED_BY_FLAG = ?,"
				+ "COMPLETION_STATUS_FLAG = ?, START_TIME_FLAG = ?, "
				+ "END_TIME_FLAG = ?, ETL_AUDIT_STATUS_NT = ?,  ETL_AUDIT_STATUS =?,"
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ? ";
		Object[] args = { vObject.getLvl1AlertMailId(), vObject.getLvl1AlertDescription(), vObject.getLvl2AlertMailId(),
				vObject.getLvl2AlertDescription(), vObject.getLvl3AlertMailId(), vObject.getLvl3AlertDescription(),
				vObject.getTerminatedByFlag(), vObject.getReinitiatedByFlag(), vObject.getCompletionStatusFlag(),
				vObject.getStartTimeFlag(), vObject.getEndTimeFlag(), vObject.getEtlAuditStatusNt(),
				vObject.getEtlAuditStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getCountry(),
				vObject.getLeBook(), vObject.getCategoryId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFeedGenericAlertVb dObj) {
		String query = "Delete From ETL_FEED_GENERIC_ALERT Where COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedGenericAlertVb dObj) {
		String query = "Delete From ETL_FEED_GENERIC_ALERT_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedGenericAlertVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getLvl1AlertMailId()))
			strAudit.append("LVL1_ALERT_MAIL_ID" + auditDelimiterColVal + vObject.getLvl1AlertMailId().trim());
		else
			strAudit.append("LVL1_ALERT_MAIL_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLvl1AlertDescription()))
			strAudit.append("LVL1_ALERT_DESCRIPTION" + auditDelimiterColVal + vObject.getLvl1AlertDescription().trim());
		else
			strAudit.append("LVL1_ALERT_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLvl2AlertMailId()))
			strAudit.append("LVL2_ALERT_MAIL_ID" + auditDelimiterColVal + vObject.getLvl2AlertMailId().trim());
		else
			strAudit.append("LVL2_ALERT_MAIL_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLvl2AlertMailId()))
			strAudit.append("LVL2_ALERT_DESCRIPTION" + auditDelimiterColVal + vObject.getLvl2AlertDescription().trim());
		else
			strAudit.append("LVL2_ALERT_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLvl3AlertMailId()))
			strAudit.append("LVL3_ALERT_MAIL_ID" + auditDelimiterColVal + vObject.getLvl3AlertMailId().trim());
		else
			strAudit.append("LVL3_ALERT_MAIL_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getLvl3AlertDescription()))
			strAudit.append("LVL3_ALERT_DESCRIPTION" + auditDelimiterColVal + vObject.getLvl3AlertDescription().trim());
		else
			strAudit.append("LVL3_ALERT_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getTerminatedByFlag()))
			strAudit.append("TERMINATED_BY_FLAG" + auditDelimiterColVal + vObject.getTerminatedByFlag());
		else
			strAudit.append("TERMINATED_BY_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getReinitiatedByFlag()))
			strAudit.append("REINITIATED_BY_FLAG" + auditDelimiterColVal + vObject.getReinitiatedByFlag());
		else
			strAudit.append("REINITIATED_BY_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getCompletionStatusFlag()))
			strAudit.append("COMPLETION_STATUS_FLAG" + auditDelimiterColVal + vObject.getCompletionStatusFlag());
		else
			strAudit.append("COMPLETION_STATUS_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getStartTimeFlag()))
			strAudit.append("START_TIME_FLAG" + auditDelimiterColVal + vObject.getStartTimeFlag());
		else
			strAudit.append("START_TIME_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getEndTimeFlag()))
			strAudit.append("END_TIME_FLAG" + auditDelimiterColVal + vObject.getEndTimeFlag());
		else
			strAudit.append("END_TIME_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_AUDIT_STATUS_NT" + auditDelimiterColVal + vObject.getEtlAuditStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_AUDIT_STATUS" + auditDelimiterColVal + vObject.getEtlAuditStatus());
		strAudit.append(auditDelimiter);

		strAudit.append("RECORD_INDICATOR_NT" + auditDelimiterColVal + vObject.getRecordIndicatorNt());
		strAudit.append(auditDelimiter);

		strAudit.append("RECORD_INDICATOR" + auditDelimiterColVal + vObject.getRecordIndicator());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getMaker()))
			strAudit.append("MAKER" + auditDelimiterColVal + vObject.getMaker());
		else
			strAudit.append("MAKER" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getVerifier()))
			strAudit.append("VERIFIER" + auditDelimiterColVal + vObject.getVerifier());
		else
			strAudit.append("VERIFIER" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getInternalStatus()))
			strAudit.append("INTERNAL_STATUS" + auditDelimiterColVal + vObject.getInternalStatus());
		else
			strAudit.append("INTERNAL_STATUS" + auditDelimiterColVal + "NULL");
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
	protected ExceptionCode doInsertApprRecordForNonTrans(EtlFeedGenericAlertVb vObject) throws RuntimeCustomException {
		List<EtlFeedGenericAlertVb> collTemp = null;
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
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFeedGenericAlertVb>) collTemp).get(0));
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

	@Override
	public void setServiceDefaults() {
		serviceName = "EtlFeedGenericAlert";
		serviceDesc = "ETL Feed Generic Alert";
		tableName = "ETL_FEED_GENERIC_ALERT";
		childTableName = "ETL_FEED_GENERIC_ALERT";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	public List<EtlFeedGenericAlertVb> findCountryByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY FROM COUNTRIES CT WHERE UPPER(CT.COUNTRY_DESCRIPTION) " + val
				+ " ORDER BY COUNTRY";
		return getJdbcTemplate().query(sql, getMapperForCountry());
	}

	protected RowMapper getMapperForCountry() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedGenericAlertVb etlFeedGenericAlertVb = new EtlFeedGenericAlertVb();
				etlFeedGenericAlertVb.setCountry(rs.getString("COUNTRY"));
				return etlFeedGenericAlertVb;
			}
		};
		return mapper;
	}

	public List<EtlFeedGenericAlertVb> findLeBookByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT LE_BOOK FROM LE_BOOK LEB WHERE LEB_STATUS=0 AND UPPER(LEB.LEB_DESCRIPTION) " + val
				+ " ORDER BY LE_BOOK";
		return getJdbcTemplate().query(sql, getMapperForLeBook());
	}

	protected RowMapper getMapperForLeBook() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedGenericAlertVb etlFeedGenericAlertVb = new EtlFeedGenericAlertVb();
				etlFeedGenericAlertVb.setLeBook(rs.getString("LE_BOOK"));
				return etlFeedGenericAlertVb;
			}
		};
		return mapper;
	}
}
