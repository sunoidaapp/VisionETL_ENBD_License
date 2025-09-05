package com.vision.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
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
import com.vision.vb.EtlFeedAlertMatrixVb;
import com.vision.vb.EtlFeedCategoryAccessVb;
import com.vision.vb.EtlFeedCategoryAlertConfigVb;
import com.vision.vb.EtlFeedCategoryDepedencyVb;
import com.vision.vb.EtlFeedCategoryVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlFeedCategoryDao extends AbstractDao<EtlFeedCategoryVb> {

	@Autowired
	private EtlFeedCategoryAccessDao etlFeedCategoryAccessDao;

	@Autowired
	private EtlCategoryAlertconfigDao etlCategoryAlertConfigDao;

	@Autowired
	private EtlCategoryDependencyDao etlCategoryDependencyDao;

	@Autowired
	private VisionUsersDao visionUsersDao;

	@Autowired
	private NumSubTabDao numSubTabDao;

	/******* Mapper Start **********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String CategoryStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.CATEGORY_STATUS",
			"CATEGORY_STATUS_DESC");
	String CategoryStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.CATEGORY_STATUS",
			"CATEGORY_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryVb vObject = new EtlFeedCategoryVb();
				vObject.setWriteFlag(
						ValidationUtil.isValid(rs.getString("WRITE_FLAG")) ? rs.getString("WRITE_FLAG") : "");
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
				if (ValidationUtil.isValid(vObject.getCategoryId())) {
					EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
					depVb.setCountry(vObject.getCountry());
					depVb.setLeBook(vObject.getLeBook());
					depVb.setCategoryId(vObject.getCategoryId());
					List<EtlFeedCategoryDepedencyVb> depLst = etlCategoryDependencyDao.getQueryResults(depVb,
							Constants.STATUS_ZERO);
					if (depLst != null && depLst.size() > 0) {
					} else {
						depLst = etlCategoryDependencyDao.getQueryResults(depVb, 1);
					}
					vObject.setEtlCategoryDeplst(depLst);

					EtlFeedCategoryAlertConfigVb confVb = new EtlFeedCategoryAlertConfigVb();
					confVb.setCountry(vObject.getCountry());
					confVb.setLeBook(vObject.getLeBook());
					confVb.setCategoryId(vObject.getCategoryId());
					List<EtlFeedCategoryAlertConfigVb> confLst = etlCategoryAlertConfigDao.getQueryResults(confVb,
							Constants.STATUS_ZERO);
					if (confLst != null && confLst.size() > 0) {
					} else {
						confLst = etlCategoryAlertConfigDao.getQueryResults(confVb, 1);
					}
					vObject.setEtlCategoryAlertConfiglst(confLst);
				}
				if (rs.getString("CATEGORY_DESCRIPTION") != null) {
					vObject.setCategoryDescription(rs.getString("CATEGORY_DESCRIPTION"));
				} else {
					vObject.setCategoryDescription("");
				}
				vObject.setChannelTypeSMS(rs.getString("CHANNEL_TYPE_SMS"));
				vObject.setChannelTypeEmail(rs.getString("CHANNEL_TYPE_EMAIL"));
				vObject.setAlertFlag(rs.getString("ALERT_FLAG"));
				vObject.setCategoryStatusNt(rs.getInt("CATEGORY_STATUS_NT"));
				vObject.setCategoryStatus(rs.getInt("CATEGORY_STATUS"));
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

	protected RowMapper getLocalMapper(int intStatus) {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryVb vObject = new EtlFeedCategoryVb();
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
				if (ValidationUtil.isValid(vObject.getCategoryId())) {
					EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
					depVb.setCountry(vObject.getCountry());
					depVb.setLeBook(vObject.getLeBook());
					depVb.setCategoryId(vObject.getCategoryId());
					List<EtlFeedCategoryDepedencyVb> depLst = etlCategoryDependencyDao.getQueryResults(depVb,
							intStatus);
					if (depLst != null && depLst.size() > 0) {
					} else {
						depLst = etlCategoryDependencyDao.getQueryResults(depVb, 1);
					}
					vObject.setEtlCategoryDeplst(depLst);

					EtlFeedCategoryAlertConfigVb confVb = new EtlFeedCategoryAlertConfigVb();
					confVb.setCountry(vObject.getCountry());
					confVb.setLeBook(vObject.getLeBook());
					confVb.setCategoryId(vObject.getCategoryId());
					List<EtlFeedCategoryAlertConfigVb> confLst = etlCategoryAlertConfigDao.getQueryResults(confVb,
							intStatus);
					if (confLst != null && confLst.size() > 0) {
					} else {
						confLst = etlCategoryAlertConfigDao.getQueryResults(confVb, 1);
					}
					vObject.setEtlCategoryAlertConfiglst(confLst);
				}

				if (rs.getString("CATEGORY_DESCRIPTION") != null) {
					vObject.setCategoryDescription(rs.getString("CATEGORY_DESCRIPTION"));
				} else {
					vObject.setCategoryDescription("");
				}
				vObject.setChannelTypeSMS(rs.getString("CHANNEL_TYPE_SMS"));
				vObject.setChannelTypeEmail(rs.getString("CHANNEL_TYPE_EMAIL"));
				vObject.setAlertFlag(rs.getString("ALERT_FLAG"));

				vObject.setCategoryStatusNt(rs.getInt("CATEGORY_STATUS_NT"));
				vObject.setCategoryStatus(rs.getInt("CATEGORY_STATUS"));
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
	public List<EtlFeedCategoryVb> getQueryPopupResults(EtlFeedCategoryVb vObj) {
		setServiceDefaults();
		Vector<Object> params = new Vector<Object>();
		String accessQueyr = ", ETL_FEED_CATEGORY_ACCESS FCA, VISION_USERS VU "
				+ " WHERE (TAPPR.CATEGORY_ID =  FCA.CATEGORY_ID " + " AND TAPPR.COUNTRY = FCA.COUNTRY "
				+ " AND TAPPR.LE_BOOK = FCA.LE_BOOK " + " AND FCA.USER_GROUP = VU.USER_GROUP "
				+ " AND FCA.USER_PROFILE = VU.USER_PROFILE " + " AND VU.VISION_ID = " + intCurrentUserId + " ) ";

		StringBuffer strBufApprove = new StringBuffer("SELECT FCA.write_flag, TAPPR.COUNTRY," + CountryApprDesc
				+ ",TAPPR.LE_BOOK," + LeBookApprDesc
				+ ",TAPPR.CATEGORY_ID,TAPPR.CATEGORY_DESCRIPTION,TAPPR.CATEGORY_STATUS_NT,TAPPR.CATEGORY_STATUS,"
				+ CategoryStatusNtApprDesc + ",TAPPR.RECORD_INDICATOR_NT"
				+ ",TAPPR.CHANNEL_TYPE_SMS,TAPPR.CHANNEL_TYPE_EMAIL,TAPPR.ALERT_FLAG ,TAPPR.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc + ",TAPPR.MAKER, " + makerApprDesc + "," + "TAPPR.VERIFIER, "
				+ verifierApprDesc + ",TAPPR.DATE_LAST_MODIFIED date_modified" + ",TAPPR.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION FROM ETL_FEED_CATEGORY TAPPR " + accessQueyr);

		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_FEED_CATEGORY_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.CATEGORY_ID = TPend.CATEGORY_ID )");

		StringBuffer strBufPending = new StringBuffer("SELECT FCA.write_flag, TPEND.COUNTRY," + CountryPendDesc
				+ ",TPEND.LE_BOOK," + LeBookTPendDesc
				+ ",TPEND.CATEGORY_ID,TPEND.CATEGORY_DESCRIPTION,TPEND.CATEGORY_STATUS_NT,TPEND.CATEGORY_STATUS,"
				+ CategoryStatusNtPendDesc + ",TPEND.RECORD_INDICATOR_NT"
				+ ",TPEND.CHANNEL_TYPE_SMS,TPEND.CHANNEL_TYPE_EMAIL,TPEND.ALERT_FLAG ,TPEND.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc + ",TPEND.MAKER, " + makerPendDesc + "," + "TPEND.VERIFIER, "
				+ verifierPendDesc + ",TPEND.DATE_LAST_MODIFIED date_modified" + ",TPEND.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION FROM ETL_FEED_CATEGORY_PEND TPEND " + accessQueyr.replaceAll("TAPPR.", "TPEND."));

		try {
			if (vObj.getSmartSearchOpt() != null && vObj.getSmartSearchOpt().size() > 0) {
				int count = 1;
				for (SmartSearchVb data : vObj.getSmartSearchOpt()) {
					if (count == vObj.getSmartSearchOpt().size()) {
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
						CommonUtils.addToQuery(" upper(TAppr.COUNTRY) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.COUNTRY) " + val, strBufPending);
						break;

					case "countryDesc":
						List<EtlFeedCategoryVb> countryData = findCountryByCustomFilter(val);
						String counData = "";
						if (countryData != null && countryData.size() > 0) {
							for (int k = 0; k < countryData.size(); k++) {
								String country = countryData.get(k).getCountry();
								if (!ValidationUtil.isValid(counData)) {
									counData = "'" + country + "'";
								} else {
									counData = counData + ",'" + country + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.COUNTRY) IN (" + counData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.COUNTRY) IN (" + counData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.COUNTRY) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.COUNTRY) IN ('') ", strBufPending);
						}
						break;

					case "leBook":
						CommonUtils.addToQuery(" upper(TAppr.LE_BOOK) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.LE_BOOK) " + val, strBufPending);
						break;

					case "leBookDesc":
						List<EtlFeedCategoryVb> leBookData = findLeBookByCustomFilter(val);
						String lebData = "";
						if (leBookData != null && leBookData.size() > 0) {
							for (int k = 0; k < leBookData.size(); k++) {
								String Lebook = leBookData.get(k).getLeBook();
								if (!ValidationUtil.isValid(lebData)) {
									lebData = "'" + Lebook + "'";
								} else {
									lebData = lebData + ",'" + Lebook + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TAPPR.LE_BOOK) IN (" + lebData + ") ",
									strBufApprove);
							CommonUtils.addToQuery(" (TPend.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TPend.LE_BOOK) IN (" + lebData + ") ",
									strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TAPPR.LE_BOOK) IN ('') ",
									strBufApprove);
							CommonUtils.addToQuery(" (TPend.COUNTRY"+getDbFunction(Constants.PIPELINE, null)+"'-'"+getDbFunction(Constants.PIPELINE, null)+"TPend.LE_BOOK) IN ('') ",
									strBufPending);
						}
						break;

					case "categoryId":
						CommonUtils.addToQuery(" upper(TAppr.CATEGORY_ID) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CATEGORY_ID) " + val, strBufPending);
						break;

					case "categoryDescription":
						CommonUtils.addToQuery(" upper(TAppr.CATEGORY_DESCRIPTION) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CATEGORY_DESCRIPTION) " + val, strBufPending);
						break;

					case "categoryStatus":
						CommonUtils.addToQuery(" upper(TAppr.CATEGORY_STATUS) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CATEGORY_STATUS) " + val, strBufPending);
						break;

					case "statusDesc":
						List<NumSubTabVb> numSTData = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 1);
						String actData = "";
						if (numSTData != null && numSTData.size() > 0) {
							for (int k = 0; k < numSTData.size(); k++) {
								int numsubtab = numSTData.get(k).getNumSubTab();
								if (!ValidationUtil.isValid(actData)) {
									actData = "'" + Integer.toString(numsubtab) + "'";
								} else {
									actData = actData + ",'" + Integer.toString(numsubtab) + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.CATEGORY_STATUS) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.CATEGORY_STATUS) IN (" + actData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.CATEGORY_STATUS) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.CATEGORY_STATUS) IN ('') ", strBufPending);
						}
						break;
						
					case "recordIndicator":
						CommonUtils.addToQuery(" upper(TAppr.RECORD_INDICATOR) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.RECORD_INDICATOR) " + val, strBufPending);
						break;

					case "recordIndicatorDesc":
						numSTData = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 7);
						actData = "";
						if (numSTData != null && numSTData.size() > 0) {
							for (int k = 0; k < numSTData.size(); k++) {
								int numsubtab = numSTData.get(k).getNumSubTab();
								if (!ValidationUtil.isValid(actData)) {
									actData = "'" + Integer.toString(numsubtab) + "'";
								} else {
									actData = actData + ",'" + Integer.toString(numsubtab) + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.RECORD_INDICATOR) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.RECORD_INDICATOR) IN (" + actData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.RECORD_INDICATOR) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.RECORD_INDICATOR) IN ('') ", strBufPending);
						}
						break;
						
					case "makerName":
						List<VisionUsersVb> muData = visionUsersDao.findUserIdByUserName(val);
						String actMData = "";
						if (muData != null && muData.size() > 0) {
							for (int k = 0; k < muData.size(); k++) {
								int visId = muData.get(k).getVisionId();
								if (!ValidationUtil.isValid(actMData)) {
									actMData = "'" + visId + "'";
								} else {
									actMData = actMData + ",'" + visId + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.MAKER) IN (" + actMData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.MAKER) IN (" + actMData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.MAKER) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.MAKER) IN ('') ", strBufPending);
						}
						break;

					case "verifierName":
						muData = visionUsersDao.findUserIdByUserName(val);
						actMData = "";
						if (muData != null && muData.size() > 0) {
							for (int k = 0; k < muData.size(); k++) {
								int visId = muData.get(k).getVisionId();
								if (!ValidationUtil.isValid(actMData)) {
									actMData = "'" + Integer.toString(visId) + "'";
								} else {
									actMData = actMData + ",'" + Integer.toString(visId) + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.VERIFIER) IN (" + actMData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.VERIFIER) IN (" + actMData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.VERIFIER) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.VERIFIER) IN ('') ", strBufPending);
						}
						break;
						
					default:
						break;
					}
					count++;
				}
			}
			String orderBy = " Order By date_modified desc";
			return getQueryPopupResults(vObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlFeedCategoryVb> getQueryResults(EtlFeedCategoryVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedCategoryVb> collTemp = null;

		final int intKeyFieldsCount = 3;

		String strQueryAppr = new String("Select TAppr.COUNTRY," + CountryApprDesc + ",TAppr.LE_BOOK, " + LeBookApprDesc
				+ ",TAppr.CATEGORY_ID,TAppr.CHANNEL_TYPE_SMS,TAppr.CHANNEL_TYPE_EMAIL,TAppr.ALERT_FLAG,TAppr.CATEGORY_DESCRIPTION,TAppr.CATEGORY_STATUS_NT"
				+ ",TAppr.CATEGORY_STATUS, " + CategoryStatusNtApprDesc + ",TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR, " + RecordIndicatorNtApprDesc + ",TAppr.MAKER, " + makerApprDesc
				+ ",TAppr.VERIFIER, " + verifierApprDesc + ",TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION "
				+ "From ETL_FEED_CATEGORY TAppr WHERE UPPER(TAppr.COUNTRY) = ?  AND UPPER(TAppr.LE_BOOK) = ?  AND UPPER(TAppr.CATEGORY_ID) = ?   ");

		String strQueryPend = new String("Select TPend.COUNTRY," + CountryPendDesc + ",TPend.LE_BOOK," + LeBookTPendDesc
				+ ",TPend.CATEGORY_ID,TPend.CHANNEL_TYPE_SMS,TPend.CHANNEL_TYPE_EMAIL,TPend.ALERT_FLAG,TPend.CATEGORY_DESCRIPTION,TPend.CATEGORY_STATUS_NT"
				+ ",TPend.CATEGORY_STATUS," + CategoryStatusNtPendDesc
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR," + RecordIndicatorNtPendDesc + ""
				+ ",TPend.MAKER, " + makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc
				+ ",TPend.INTERNAL_STATUS,  " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION "
				+ "From ETL_FEED_CATEGORY_PEND TPend WHERE UPPER(TPend.COUNTRY) = ?  AND UPPER(TPend.LE_BOOK) = ?  AND UPPER(TPend.CATEGORY_ID) = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry().toUpperCase();
		objParams[1] = dObj.getLeBook().toUpperCase();
		objParams[2] = dObj.getCategoryId().toUpperCase();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getLocalMapper(intStatus));
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getLocalMapper(intStatus));
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
	protected List<EtlFeedCategoryVb> selectApprovedRecord(EtlFeedCategoryVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedCategoryVb> doSelectPendingRecord(EtlFeedCategoryVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlFeedCategoryVb records) {
		return records.getCategoryStatus();
	}

	@Override
	protected void setStatus(EtlFeedCategoryVb vObject, int status) {
		vObject.setCategoryStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFeedCategoryVb vObject) {
		if ("Y".equalsIgnoreCase(vObject.getChannelTypeEmail()) || "Y".equalsIgnoreCase(vObject.getChannelTypeSMS())) {
			vObject.setAlertFlag("Y");
		}
		String query = "Insert Into ETL_FEED_CATEGORY (ALERT_FLAG,CHANNEL_TYPE_SMS,CHANNEL_TYPE_email,COUNTRY, LE_BOOK, CATEGORY_ID, CATEGORY_DESCRIPTION, "
				+ "CATEGORY_STATUS_NT, "
				+ "CATEGORY_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getAlertFlag(), vObject.getChannelTypeSMS(), vObject.getChannelTypeEmail(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getCategoryDescription(),
				vObject.getCategoryStatusNt(), vObject.getCategoryStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlFeedCategoryVb vObject) {
		if ("Y".equalsIgnoreCase(vObject.getChannelTypeEmail()) || "Y".equalsIgnoreCase(vObject.getChannelTypeSMS())) {
			vObject.setAlertFlag("Y");
		}
		String query = "Insert Into ETL_FEED_CATEGORY_PEND (ALERT_FLAG,CHANNEL_TYPE_SMS,CHANNEL_TYPE_email,COUNTRY, LE_BOOK, CATEGORY_ID, CATEGORY_DESCRIPTION, CATEGORY_STATUS_NT, "
				+ "CATEGORY_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getAlertFlag(), vObject.getChannelTypeSMS(), vObject.getChannelTypeEmail(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getCategoryDescription(),
				vObject.getCategoryStatusNt(), vObject.getCategoryStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFeedCategoryVb vObject) {
		if ("Y".equalsIgnoreCase(vObject.getChannelTypeEmail()) || "Y".equalsIgnoreCase(vObject.getChannelTypeSMS())) {
			vObject.setAlertFlag("Y");
		}
		String query = "Insert Into ETL_FEED_CATEGORY_PEND (ALERT_FLAG,CHANNEL_TYPE_SMS,CHANNEL_TYPE_email,COUNTRY, LE_BOOK, CATEGORY_ID, CATEGORY_DESCRIPTION, CATEGORY_STATUS_NT, "
				+ "CATEGORY_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + " )";
		Object[] args = { vObject.getAlertFlag(), vObject.getChannelTypeSMS(), vObject.getChannelTypeEmail(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId(), vObject.getCategoryDescription(),
				vObject.getCategoryStatusNt(), vObject.getCategoryStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFeedCategoryVb vObject) {
		if ("Y".equalsIgnoreCase(vObject.getChannelTypeEmail()) || "Y".equalsIgnoreCase(vObject.getChannelTypeSMS())) {
			vObject.setAlertFlag("Y");
		}
		String query = "Update ETL_FEED_CATEGORY Set ALERT_FLAG =?,CHANNEL_TYPE_SMS =?,CHANNEL_TYPE_email =?, CATEGORY_DESCRIPTION = ?, CATEGORY_STATUS_NT = ?, CATEGORY_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ? ";
		Object[] args = { vObject.getAlertFlag(), vObject.getChannelTypeSMS(), vObject.getChannelTypeEmail(),
				vObject.getCategoryDescription(), vObject.getCategoryStatusNt(), vObject.getCategoryStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFeedCategoryVb vObject) {
		if ("Y".equalsIgnoreCase(vObject.getChannelTypeEmail()) || "Y".equalsIgnoreCase(vObject.getChannelTypeSMS())) {
			vObject.setAlertFlag("Y");
		}
		String query = "Update ETL_FEED_CATEGORY_PEND Set ALERT_FLAG =?,CHANNEL_TYPE_SMS =?,CHANNEL_TYPE_email =?,CATEGORY_DESCRIPTION = ?, CATEGORY_STATUS_NT = ?, CATEGORY_STATUS = ?, "
				+ "RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ "WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ? ";
		Object[] args = { vObject.getAlertFlag(), vObject.getChannelTypeSMS(), vObject.getChannelTypeEmail(),
				vObject.getCategoryDescription(), vObject.getCategoryStatusNt(), vObject.getCategoryStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFeedCategoryVb dObj) {
		String query = "Delete From ETL_FEED_CATEGORY Where COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedCategoryVb dObj) {
		String query = "Delete From ETL_FEED_CATEGORY_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ?  ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getCategoryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedCategoryVb vObject) {
		if ("Y".equalsIgnoreCase(vObject.getChannelTypeEmail()) || "Y".equalsIgnoreCase(vObject.getChannelTypeSMS())) {
			vObject.setAlertFlag("Y");
		}
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

		if (ValidationUtil.isValid(vObject.getCategoryDescription()))
			strAudit.append("CATEGORY_DESCRIPTION" + auditDelimiterColVal + vObject.getCategoryDescription().trim());
		else
			strAudit.append("CATEGORY_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("CATEGORY_STATUS" + auditDelimiterColVal + vObject.getCategoryStatus());
		strAudit.append("CHANNEL_TYPE_SMS" + auditDelimiterColVal + vObject.getChannelTypeSMS());

		strAudit.append("CHANNEL_TYPE_EMAIL" + auditDelimiterColVal + vObject.getChannelTypeEmail());

		strAudit.append("ALERT_FLAG" + auditDelimiterColVal + vObject.getAlertFlag());

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
	protected ExceptionCode doInsertApprRecordForNonTrans(EtlFeedCategoryVb vObject) throws RuntimeCustomException {
		List<EtlFeedCategoryVb> collTemp = null;
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
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFeedCategoryVb>) collTemp).get(0));
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
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		if (vObject.getEtlCategoryAlertConfiglst() != null && vObject.getEtlCategoryAlertConfiglst().size() > 0) {
			for (EtlFeedCategoryAlertConfigVb alertconfigVb : vObject.getEtlCategoryAlertConfiglst()) {
				alertconfigVb.setCountry(vObject.getCountry());
				alertconfigVb.setLeBook(vObject.getLeBook());
				alertconfigVb.setCategoryId(vObject.getCategoryId());
				alertconfigVb.setVerifier(getIntCurrentUserId());
				alertconfigVb.setRecordIndicator(Constants.STATUS_ZERO);
				alertconfigVb.setCategoryAlertStatus(vObject.getCategoryStatus());
				exceptionCode = etlCategoryAlertConfigDao.doInsertApprRecordForNonTrans(alertconfigVb);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		if (vObject.getEtlCategoryDeplst() != null && vObject.getEtlCategoryDeplst().size() > 0) {
			for (EtlFeedCategoryDepedencyVb catDepVb : vObject.getEtlCategoryDeplst()) {
				catDepVb.setCountry(vObject.getCountry());
				catDepVb.setLeBook(vObject.getLeBook());
				catDepVb.setDependentCategory(catDepVb.getCategoryId());
				catDepVb.setCategoryId(vObject.getCategoryId());
				catDepVb.setVerifier(getIntCurrentUserId());
				catDepVb.setRecordIndicator(Constants.STATUS_ZERO);
				catDepVb.setCategoryDepStatus(vObject.getCategoryStatus());
				exceptionCode = etlCategoryDependencyDao.doInsertApprRecordForNonTrans(catDepVb);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		vObject.setDateCreation(systemDate);
		exceptionCode = writeAuditLog(vObject, null);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}

		VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
		EtlFeedCategoryAccessVb etlFeedCategoryAccessVb = new EtlFeedCategoryAccessVb();
		etlFeedCategoryAccessVb.setCountry(vObject.getCountry());
		etlFeedCategoryAccessVb.setLeBook(vObject.getLeBook());
		etlFeedCategoryAccessVb.setCategoryId(vObject.getCategoryId());
		etlFeedCategoryAccessVb.setUserGroup(visionUSersVb.getUserGroup());
		etlFeedCategoryAccessVb.setUserProfile(visionUSersVb.getUserProfile());
		etlFeedCategoryAccessVb.setWriteFlag("Y");
		etlFeedCategoryAccessVb.setVerificationRequired(false);
		etlFeedCategoryAccessVb.setFeedSourceStatus(0);
		etlFeedCategoryAccessDao.doInsertApprRecordForNonTrans(etlFeedCategoryAccessVb);
		return exceptionCode;
	}

	@Override
	protected ExceptionCode doInsertRecordForNonTrans(EtlFeedCategoryVb vObject) throws RuntimeCustomException {
		List<EtlFeedCategoryVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.ADD;
		strErrorDesc = "";
		strCurrentOperation = Constants.ADD;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int staticDeletionFlag = getStatus(((ArrayList<EtlFeedCategoryVb>) collTemp).get(0));
			if (staticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.RECORD_ALREADY_PRESENT_BUT_INACTIVE);
				throw buildRuntimeCustomException(exceptionCode);
			} else {
				exceptionCode = getResultObject(Constants.DUPLICATE_KEY_INSERTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}

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
			EtlFeedCategoryVb vObjectLocal = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);
			if (vObjectLocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				exceptionCode = getResultObject(Constants.PENDING_FOR_ADD_ALREADY);
				throw buildRuntimeCustomException(exceptionCode);
			}
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			// Try inserting the record
			vObject.setVerifier(0);
			vObject.setRecordIndicator(Constants.STATUS_INSERT);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
			vObject.setDateCreation(systemDate);
			retVal = doInsertionPend(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (vObject.getEtlCategoryAlertConfiglst() != null && vObject.getEtlCategoryAlertConfiglst().size() > 0) {
				for (EtlFeedCategoryAlertConfigVb alertconfigVb : vObject.getEtlCategoryAlertConfiglst()) {
					alertconfigVb.setCountry(vObject.getCountry());
					alertconfigVb.setLeBook(vObject.getLeBook());
					alertconfigVb.setCategoryId(vObject.getCategoryId());
					alertconfigVb.setVerifier(0);
					alertconfigVb.setRecordIndicator(Constants.STATUS_INSERT);
					alertconfigVb.setDateLastModified(systemDate);
					alertconfigVb.setDateCreation(systemDate);
					alertconfigVb.setCategoryAlertStatus(vObject.getCategoryStatus());
					exceptionCode = etlCategoryAlertConfigDao.doInsertRecordForNonTrans(alertconfigVb);
					retVal = exceptionCode.getErrorCode();
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
			if (vObject.getEtlCategoryDeplst() != null && vObject.getEtlCategoryDeplst().size() > 0) {
				for (EtlFeedCategoryDepedencyVb catDepVb : vObject.getEtlCategoryDeplst()) {
					catDepVb.setCountry(vObject.getCountry());
					catDepVb.setLeBook(vObject.getLeBook());
					catDepVb.setDependentCategory(catDepVb.getCategoryId());
					catDepVb.setCategoryId(vObject.getCategoryId());
					catDepVb.setVerifier(0);
					catDepVb.setRecordIndicator(Constants.STATUS_INSERT);
					catDepVb.setDateLastModified(systemDate);
					catDepVb.setDateCreation(systemDate);
					catDepVb.setCategoryDepStatus(vObject.getCategoryStatus());
					exceptionCode = etlCategoryDependencyDao.doInsertRecordForNonTrans(catDepVb);
					retVal = exceptionCode.getErrorCode();
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
				}
			}
			exceptionCode = getResultObject(retVal);
		}

		VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
		EtlFeedCategoryAccessVb etlFeedCategoryAccessVb = new EtlFeedCategoryAccessVb();
		etlFeedCategoryAccessVb.setCountry(vObject.getCountry());
		etlFeedCategoryAccessVb.setLeBook(vObject.getLeBook());
		etlFeedCategoryAccessVb.setCategoryId(vObject.getCategoryId());
		etlFeedCategoryAccessVb.setUserGroup(visionUSersVb.getUserGroup());
		etlFeedCategoryAccessVb.setUserProfile(visionUSersVb.getUserProfile());
		etlFeedCategoryAccessVb.setWriteFlag("Y");
		etlFeedCategoryAccessVb.setVerificationRequired(false);
		etlFeedCategoryAccessVb.setFeedSourceStatus(0);

		etlFeedCategoryAccessDao.doInsertApprRecordForNonTrans(etlFeedCategoryAccessVb);
		return exceptionCode;
	}

	@Override
	protected ExceptionCode doDeleteApprRecordForNonTrans(EtlFeedCategoryVb vObject) throws RuntimeCustomException {
		List<EtlFeedCategoryVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		EtlFeedCategoryVb vObjectlocal = null;
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
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlFeedCategoryVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);
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

			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);
			EtlFeedCategoryAlertConfigVb alertconfigVb = new EtlFeedCategoryAlertConfigVb();
			alertconfigVb.setCountry(vObjectlocal.getCountry());
			alertconfigVb.setLeBook(vObjectlocal.getLeBook());
			alertconfigVb.setCategoryId(vObjectlocal.getCategoryId());
			alertconfigVb.setRecordIndicator(vObject.getRecordIndicator());
			alertconfigVb.setStaticDelete(vObject.isStaticDelete());
			List<EtlFeedCategoryAlertConfigVb> collTempUpl = etlCategoryAlertConfigDao.getQueryResults(alertconfigVb,
					0);
			if (collTempUpl != null && collTempUpl.size() > 0) {
				collTempUpl.forEach(uplAreaVb -> {
					uplAreaVb.setStaticDelete(false);
					ExceptionCode exceptionCode1 = etlCategoryAlertConfigDao.doDeleteApprRecordForNonTrans(uplAreaVb);
					if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode1);
					}
				});
			}
			collTempUpl = etlCategoryAlertConfigDao.getQueryResults(alertconfigVb, 1);
			if (collTempUpl != null && collTempUpl.size() > 0) {
				collTempUpl.forEach(uplAreaVb -> {
					ExceptionCode exceptionCode1 = etlCategoryAlertConfigDao.doRejectRecord(uplAreaVb);
					if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode1);
					}
				});
			}
			EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
			depVb.setCountry(vObjectlocal.getCountry());
			depVb.setLeBook(vObjectlocal.getLeBook());
			depVb.setCategoryId(vObjectlocal.getCategoryId());
			List<EtlFeedCategoryDepedencyVb> collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb, 0);
			if (collTempUplDep != null && collTempUplDep.size() > 0) {
				collTempUplDep.forEach(uplAreaVb -> {
					uplAreaVb.setStaticDelete(false);
					ExceptionCode exceptionCode1 = etlCategoryDependencyDao.doDeleteApprRecordForNonTrans(uplAreaVb);
					if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode1);
					}
				});
			}
			collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb, 1);
			if (collTempUplDep != null && collTempUplDep.size() > 0) {
				collTempUplDep.forEach(uplAreaVb -> {
					ExceptionCode exceptionCode1 = etlCategoryDependencyDao.doRejectRecord(uplAreaVb);
					if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode1);
					}
				});
			}
		}
		VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
		EtlFeedCategoryAccessVb etlFeedCategoryAccessVb = new EtlFeedCategoryAccessVb();
		etlFeedCategoryAccessVb.setCountry(vObject.getCountry());
		etlFeedCategoryAccessVb.setLeBook(vObject.getLeBook());
		etlFeedCategoryAccessVb.setCategoryId(vObject.getCategoryId());
		etlFeedCategoryAccessVb.setUserGroup(visionUSersVb.getUserGroup());
		etlFeedCategoryAccessVb.setUserProfile(visionUSersVb.getUserProfile());
		etlFeedCategoryAccessVb.setWriteFlag("Y");
		etlFeedCategoryAccessVb.setVerificationRequired(false);
		etlFeedCategoryAccessVb.setStaticDelete(vObject.isStaticDelete());
		etlFeedCategoryAccessDao.doDeleteApprRecordForNonTrans(etlFeedCategoryAccessVb);
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);

		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
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

	@Override
	public void setServiceDefaults() {
		serviceName = "EtlFeedCategory";
		serviceDesc = "ETL Feed Category";
		tableName = "ETL_FEED_CATEGORY";
		childTableName = "ETL_FEED_CATEGORY";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	@Override
	public ExceptionCode doRejectRecord(EtlFeedCategoryVb vObject) throws RuntimeCustomException {
		EtlFeedCategoryVb vObjectlocal = null;
		List<EtlFeedCategoryVb> collTemp = null;
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
			vObjectlocal = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);
			// Delete the record from the Pending table
			if (deletePendingRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				EtlFeedCategoryAlertConfigVb alertconfigVb = new EtlFeedCategoryAlertConfigVb();
				alertconfigVb.setCountry(vObjectlocal.getCountry());
				alertconfigVb.setLeBook(vObjectlocal.getLeBook());
				alertconfigVb.setCategoryId(vObjectlocal.getCategoryId());
				List<EtlFeedCategoryAlertConfigVb> collTempUpl = etlCategoryAlertConfigDao
						.getQueryResults(alertconfigVb, 0);
				if (collTempUpl != null && collTempUpl.size() > 0) {
					collTempUpl.forEach(uplAreaVb -> {
						uplAreaVb.setStaticDelete(vObject.isStaticDelete());
						uplAreaVb.setVerificationRequired(vObject.isVerificationRequired());
						ExceptionCode exceptionCode1 = etlCategoryAlertConfigDao
								.doDeleteApprRecordForNonTrans(uplAreaVb);
						if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode1);
						}
					});
				}
				collTempUpl = etlCategoryAlertConfigDao.getQueryResults(alertconfigVb, 1);
				if (collTempUpl != null && collTempUpl.size() > 0) {
					collTempUpl.forEach(uplAreaVb -> {
						uplAreaVb.setStaticDelete(vObject.isStaticDelete());
						uplAreaVb.setVerificationRequired(vObject.isVerificationRequired());
						ExceptionCode exceptionCode1 = etlCategoryAlertConfigDao.doRejectRecord(uplAreaVb);
						if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode1);
						}
					});
				}
			}
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
				depVb.setCountry(vObjectlocal.getCountry());
				depVb.setLeBook(vObjectlocal.getLeBook());
				depVb.setCategoryId(vObjectlocal.getCategoryId());
				List<EtlFeedCategoryDepedencyVb> collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb, 0);
				if (collTempUplDep != null && collTempUplDep.size() > 0) {
					collTempUplDep.forEach(uplAreaVb -> {
						uplAreaVb.setStaticDelete(vObject.isStaticDelete());
						uplAreaVb.setVerificationRequired(vObject.isVerificationRequired());
						ExceptionCode exceptionCode1 = etlCategoryDependencyDao
								.doDeleteApprRecordForNonTrans(uplAreaVb);
						if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode1);
						}
					});
				}
				collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb, 1);
				if (collTempUplDep != null && collTempUplDep.size() > 0) {
					collTempUplDep.forEach(uplAreaVb -> {
						uplAreaVb.setStaticDelete(vObject.isStaticDelete());
						uplAreaVb.setVerificationRequired(vObject.isVerificationRequired());
						ExceptionCode exceptionCode1 = etlCategoryDependencyDao.doRejectRecord(uplAreaVb);
						if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode1);
						}
					});
				}
			}
			// dELETE THE RECORD FROM ACCESS TABLE
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
				EtlFeedCategoryAccessVb etlFeedCategoryAccessVb = new EtlFeedCategoryAccessVb();
				etlFeedCategoryAccessVb.setCountry(vObject.getCountry());
				etlFeedCategoryAccessVb.setLeBook(vObject.getLeBook());
				etlFeedCategoryAccessVb.setCategoryId(vObject.getCategoryId());
				etlFeedCategoryAccessVb.setUserGroup(visionUSersVb.getUserGroup());
				etlFeedCategoryAccessVb.setUserProfile(visionUSersVb.getUserProfile());
				etlFeedCategoryAccessVb.setWriteFlag("Y");
				etlFeedCategoryAccessVb.setVerificationRequired(false);
				if (etlFeedCategoryAccessDao.doDeleteAppr(etlFeedCategoryAccessVb) == 0) {
					exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
			return getResultObject(Constants.SUCCESSFUL_OPERATION);
		} catch (UncategorizedSQLException uSQLEcxception) {
			strErrorDesc = parseErrorMsg(uSQLEcxception);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(ex);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	@Override
	public ExceptionCode doApproveRecord(EtlFeedCategoryVb vObject, boolean staticDelete)
			throws RuntimeCustomException {
		EtlFeedCategoryVb oldContents = null;
		EtlFeedCategoryVb vObjectlocal = null;
		List<EtlFeedCategoryVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		String systemDate = getSystemDate();

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

			vObjectlocal = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);

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
				oldContents = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);
			}

			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) { // Add authorization
				// Write the contents of the Pending table record to the Approved table
				vObjectlocal.setRecordIndicator(Constants.STATUS_ZERO);
				vObjectlocal.setVerifier(getIntCurrentUserId());
				retVal = doInsertionAppr(vObjectlocal);
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

				EtlFeedCategoryAlertConfigVb alertconfigVb = new EtlFeedCategoryAlertConfigVb();
				alertconfigVb.setCountry(vObjectlocal.getCountry());
				alertconfigVb.setLeBook(vObjectlocal.getLeBook());
				alertconfigVb.setCategoryId(vObjectlocal.getCategoryId());
				List<EtlFeedCategoryAlertConfigVb> collTempUpl = etlCategoryAlertConfigDao
						.getQueryResults(alertconfigVb, Constants.STATUS_PENDING);
				if (collTempUpl != null && !collTempUpl.isEmpty()) {
					collTempUpl.forEach(uplAreaVb -> {
						ExceptionCode exceptionCodeUp = etlCategoryAlertConfigDao.doApproveRecord(uplAreaVb,
								staticDelete);
						if (exceptionCodeUp.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCodeUp = getResultObject(exceptionCodeUp.getErrorCode());
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}

				EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
				depVb.setCountry(vObjectlocal.getCountry());
				depVb.setLeBook(vObjectlocal.getLeBook());
				depVb.setCategoryId(vObjectlocal.getCategoryId());
				List<EtlFeedCategoryDepedencyVb> collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb,
						Constants.STATUS_PENDING);
				if (collTempUplDep != null && !collTempUplDep.isEmpty()) {
					collTempUplDep.forEach(uplAreaVb -> {
						ExceptionCode exceptionCodeUp = etlCategoryDependencyDao.doApproveRecord(uplAreaVb,
								staticDelete);
						if (exceptionCodeUp.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCodeUp = getResultObject(exceptionCodeUp.getErrorCode());
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}
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
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				EtlFeedCategoryAlertConfigVb alertconfigVb = new EtlFeedCategoryAlertConfigVb();
				alertconfigVb.setCountry(vObjectlocal.getCountry());
				alertconfigVb.setLeBook(vObjectlocal.getLeBook());
				alertconfigVb.setCategoryId(vObjectlocal.getCategoryId());
				List<EtlFeedCategoryAlertConfigVb> collTempUpl = etlCategoryAlertConfigDao
						.getQueryResults(alertconfigVb, Constants.STATUS_PENDING);
				etlCategoryAlertConfigDao.doDeleteApprAll(alertconfigVb);
				if (collTempUpl != null && !collTempUpl.isEmpty()) {
					collTempUpl.forEach(uplAreaVb -> {
						uplAreaVb.setDateLastModified(systemDate);
						uplAreaVb.setVerifier(getIntCurrentUserId());
						uplAreaVb.setRecordIndicator(Constants.STATUS_ZERO);
						retVal = etlCategoryAlertConfigDao.doInsertionAppr(uplAreaVb);
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}

				EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
				depVb.setCountry(vObjectlocal.getCountry());
				depVb.setLeBook(vObjectlocal.getLeBook());
				depVb.setCategoryId(vObjectlocal.getCategoryId());
				List<EtlFeedCategoryDepedencyVb> collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb,
						Constants.STATUS_PENDING);
				etlCategoryDependencyDao.doDeleteApprAll(depVb);
				if (collTempUplDep != null && !collTempUplDep.isEmpty()) {
					collTempUplDep.forEach(uplAreaVb -> {
						uplAreaVb.setDateLastModified(systemDate);
						uplAreaVb.setVerifier(getIntCurrentUserId());
						uplAreaVb.setRecordIndicator(Constants.STATUS_ZERO);
						retVal = etlCategoryDependencyDao.doInsertionAppr(uplAreaVb);
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}
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
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					EtlFeedCategoryAlertConfigVb alertconfigVb = new EtlFeedCategoryAlertConfigVb();
					alertconfigVb.setCountry(vObjectlocal.getCountry());
					alertconfigVb.setLeBook(vObjectlocal.getLeBook());
					alertconfigVb.setCategoryId(vObjectlocal.getCategoryId());
					List<EtlFeedCategoryAlertConfigVb> collTempUpl = etlCategoryAlertConfigDao
							.getQueryResults(alertconfigVb, Constants.STATUS_PENDING);
					etlCategoryAlertConfigDao.doDeleteApprAll(alertconfigVb);
					if (collTempUpl != null && !collTempUpl.isEmpty()) {
						collTempUpl.forEach(uplAreaVb -> {
							uplAreaVb.setDateLastModified(systemDate);
							uplAreaVb.setVerifier(getIntCurrentUserId());
							uplAreaVb.setRecordIndicator(Constants.STATUS_ZERO);
							uplAreaVb.setCategoryAlertStatus(Constants.PASSIVATE);
							retVal = etlCategoryAlertConfigDao.doInsertionAppr(uplAreaVb);
							if (retVal != Constants.SUCCESSFUL_OPERATION) {
								ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCodeUp);
							}
						});
					}

					EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
					depVb.setCountry(vObjectlocal.getCountry());
					depVb.setLeBook(vObjectlocal.getLeBook());
					depVb.setCategoryId(vObjectlocal.getCategoryId());
					List<EtlFeedCategoryDepedencyVb> collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb,
							Constants.STATUS_PENDING);
					etlCategoryDependencyDao.doDeleteApprAll(depVb);
					if (collTempUplDep != null && !collTempUplDep.isEmpty()) {
						collTempUplDep.forEach(uplAreaVb -> {
							uplAreaVb.setDateLastModified(systemDate);
							uplAreaVb.setVerifier(getIntCurrentUserId());
							uplAreaVb.setRecordIndicator(Constants.STATUS_ZERO);
							uplAreaVb.setCategoryDepStatus(Constants.PASSIVATE);
							retVal = etlCategoryDependencyDao.doInsertionAppr(uplAreaVb);
							if (retVal != Constants.SUCCESSFUL_OPERATION) {
								ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCodeUp);
							}
						});
					}
					setStatus(vObject, Constants.PASSIVATE);
					vObject.setDateLastModified(systemDate);

				} else {
					// Delete the existing record from the Approved table
					retVal = doDeleteAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					vObject.setDateLastModified(systemDate);
					EtlFeedCategoryAlertConfigVb alertconfigVb = new EtlFeedCategoryAlertConfigVb();
					alertconfigVb.setCountry(vObjectlocal.getCountry());
					alertconfigVb.setLeBook(vObjectlocal.getLeBook());
					alertconfigVb.setCategoryId(vObjectlocal.getCategoryId());
					List<EtlFeedCategoryAlertConfigVb> collTempUpl = etlCategoryAlertConfigDao
							.getQueryResults(alertconfigVb, 0);
					if (collTempUpl != null && collTempUpl.size() > 0) {
						collTempUpl.forEach(uplAreaVb -> {
							uplAreaVb.setStaticDelete(false);
							ExceptionCode exceptionCode1 = etlCategoryAlertConfigDao
									.doDeleteApprRecordForNonTrans(uplAreaVb);
							if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
								exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode1);
							}
						});
					}
					collTempUpl = etlCategoryAlertConfigDao.getQueryResults(alertconfigVb, 1);
					if (collTempUpl != null && collTempUpl.size() > 0) {
						collTempUpl.forEach(uplAreaVb -> {
							ExceptionCode exceptionCode1 = etlCategoryAlertConfigDao.doRejectRecord(uplAreaVb);
							if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
								exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode1);
							}
						});
					}
					EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
					depVb.setCountry(vObjectlocal.getCountry());
					depVb.setLeBook(vObjectlocal.getLeBook());
					depVb.setCategoryId(vObjectlocal.getCategoryId());
					List<EtlFeedCategoryDepedencyVb> collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb,
							0);
					if (collTempUplDep != null && collTempUplDep.size() > 0) {
						collTempUplDep.forEach(uplAreaVb -> {
							uplAreaVb.setStaticDelete(false);
							ExceptionCode exceptionCode1 = etlCategoryDependencyDao
									.doDeleteApprRecordForNonTrans(uplAreaVb);
							if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
								exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode1);
							}
						});
					}
					collTempUplDep = etlCategoryDependencyDao.getQueryResults(depVb, 1);
					if (collTempUplDep != null && collTempUplDep.size() > 0) {
						collTempUplDep.forEach(uplAreaVb -> {
							ExceptionCode exceptionCode1 = etlCategoryDependencyDao.doRejectRecord(uplAreaVb);
							if (exceptionCode1.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
								exceptionCode1 = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode1);
							}
						});
					}
					// DELETE THE RECORD FROM ACCESS TABLE
					VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
					EtlFeedCategoryAccessVb etlFeedCategoryAccessVb = new EtlFeedCategoryAccessVb();
					etlFeedCategoryAccessVb.setCountry(vObject.getCountry());
					etlFeedCategoryAccessVb.setLeBook(vObject.getLeBook());
					etlFeedCategoryAccessVb.setCategoryId(vObject.getCategoryId());
					etlFeedCategoryAccessVb.setUserGroup(visionUSersVb.getUserGroup());
					etlFeedCategoryAccessVb.setUserProfile(visionUSersVb.getUserProfile());
					etlFeedCategoryAccessVb.setWriteFlag("Y");
					etlFeedCategoryAccessVb.setVerificationRequired(false);
					etlFeedCategoryAccessDao.doApproveRecord(etlFeedCategoryAccessVb, staticDelete);

				}
				// Set the current operation to write to audit log
				strApproveOperation = Constants.DELETE;
			} else {
				exceptionCode = getResultObject(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Delete the record from the Pending table
			retVal = deletePendingRecord(vObjectlocal);
			EtlFeedCategoryAlertConfigVb alertconfigVb = new EtlFeedCategoryAlertConfigVb();
			alertconfigVb.setCountry(vObjectlocal.getCountry());
			alertconfigVb.setLeBook(vObjectlocal.getLeBook());
			alertconfigVb.setCategoryId(vObjectlocal.getCategoryId());
			etlCategoryAlertConfigDao.deletePendingRecordAll(alertconfigVb);
			EtlFeedCategoryDepedencyVb depVb = new EtlFeedCategoryDepedencyVb();
			depVb.setCountry(vObjectlocal.getCountry());
			depVb.setLeBook(vObjectlocal.getLeBook());
			depVb.setCategoryId(vObjectlocal.getCategoryId());
			etlCategoryDependencyDao.deletePendingRecordAll(depVb);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Set the internal status to Approved
			vObject.setInternalStatus(0);
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_DELETE && !staticDelete) {
				exceptionCode = writeAuditLog(null, oldContents);
				vObject.setRecordIndicator(-1);
			} else
				exceptionCode = writeAuditLog(vObjectlocal, oldContents);

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
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
		//	strErrorDesc = ex.getMessage();
			strErrorDesc = parseErrorMsg(ex);
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
	}

	public List<EtlFeedCategoryVb> findCountryByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY FROM COUNTRIES CT WHERE UPPER(CT.COUNTRY_DESCRIPTION) " + val
				+ " ORDER BY COUNTRY";
		return getJdbcTemplate().query(sql, getMapperForCountry());
	}

	protected RowMapper getMapperForCountry() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryVb etlFeedCategoryVb = new EtlFeedCategoryVb();
				etlFeedCategoryVb.setCountry(rs.getString("COUNTRY"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}

	public List<EtlFeedCategoryVb> findLeBookByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
				+ getDbFunction(Constants.PIPELINE, null)
				+ "LE_BOOK CL FROM LE_BOOK LEB WHERE LEB_STATUS=0 AND UPPER(LEB.LEB_DESCRIPTION) " + val
				+ " ORDER BY LE_BOOK";
		return getJdbcTemplate().query(sql, getMapperForLeBook());
	}

	protected RowMapper getMapperForLeBook() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryVb etlFeedCategoryVb = new EtlFeedCategoryVb();
				etlFeedCategoryVb.setLeBook(rs.getString("CL"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}

	@SuppressWarnings("unchecked")
	public List<EtlFeedCategoryVb> getCategoryListing(EtlFeedCategoryVb vObject, String tableName)
			throws DataAccessException {
		int intKeyFieldsCount = 2;
		String sql = "SELECT CATEGORY_ID, CATEGORY_ID" + getDbFunction(Constants.PIPELINE, null) + "' - '"
				+ getDbFunction(Constants.PIPELINE, null) + "CATEGORY_DESCRIPTION CATEGORY_DESCRIPTION FROM "
				+ tableName + " WHERE COUNTRY= ?  AND LE_BOOK= ? ";
		Object[] objParams = new Object[intKeyFieldsCount];
		objParams[0] = vObject.getCountry();
		objParams[1] = vObject.getLeBook();

		if (ValidationUtil.isValid(vObject.getCategoryId())) {
			sql = sql + " and CATEGORY_ID != ? ";
			intKeyFieldsCount = 3;
			objParams = new Object[intKeyFieldsCount];
			objParams[0] = vObject.getCountry();
			objParams[1] = vObject.getLeBook();
			objParams[2] = vObject.getCategoryId();
		}
		sql = sql + " ORDER BY CATEGORY_DESCRIPTION";
		return getJdbcTemplate().query(sql, objParams, getCategoryListingMapper());
	}

	protected RowMapper getCategoryListingMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryVb vObject = new EtlFeedCategoryVb();
				vObject.setCategoryId(rs.getString("CATEGORY_ID"));
				vObject.setCategoryDescription(rs.getString("CATEGORY_DESCRIPTION"));
				return vObject;
			}
		};
		return mapper;
	}

	@SuppressWarnings("unchecked")
	public List<EtlFeedCategoryVb> getCategoryDependencyListing(EtlFeedCategoryVb vObject, String tableName)
			throws DataAccessException {
		String sql = "SELECT DEPENDENT_CATEGORY CATEGORY_ID FROM " + tableName
				+ " WHERE COUNTRY= ? AND LE_BOOK= ? AND  CATEGORY_ID= ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getCategoryId() };
		return getJdbcTemplate().query(sql, args, getCategoryDependencyListingMapper());
	}

	protected RowMapper getCategoryDependencyListingMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedCategoryVb vObject = new EtlFeedCategoryVb();
				vObject.setCategoryId(rs.getString("CATEGORY_ID"));
				vObject.setCategoryDescription("");
				return vObject;
			}
		};
		return mapper;
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_CATEGORY_PRS";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSDependentParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_CATEGORY_DEP_PRS";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ? AND FEED_ID = ? AND SESSION_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedCategory(), vObject.getFeedId(),
				vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected ExceptionCode doUpdateRecordForNonTrans(EtlFeedCategoryVb vObject) throws RuntimeCustomException {
		List<EtlFeedCategoryVb> collTemp = null;
		EtlFeedCategoryVb vObjectlocal = null;
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
			vObjectlocal = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);

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
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// return getResultObject(Constants.SUCCESSFUL_OPERATION);
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
				vObjectlocal = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);
				vObject.setDateCreation(vObjectlocal.getDateCreation());
			}
			vObject.setDateCreation(vObjectlocal.getDateCreation());
			// Record is there in approved, but not in pending. So add it to pending
			vObject.setVerifier(0);
			vObject.setRecordIndicator(Constants.STATUS_UPDATE);
			retVal = doInsertionPendWithDc(vObject);
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			// return getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		EtlFeedCategoryAlertConfigVb vb = new EtlFeedCategoryAlertConfigVb();
		vb.setCountry(vObject.getCountry());
		vb.setLeBook(vObject.getLeBook());
		vb.setCategoryId(vObject.getCategoryId());
		if (vObject.getEtlCategoryAlertConfiglst() != null && vObject.getEtlCategoryAlertConfiglst().size() > 0) {
			etlCategoryAlertConfigDao.deletePendingRecordAll(vb);
			for (EtlFeedCategoryAlertConfigVb alertconfigVb : vObject.getEtlCategoryAlertConfiglst()) {
				alertconfigVb.setCountry(vObject.getCountry());
				alertconfigVb.setLeBook(vObject.getLeBook());
				alertconfigVb.setCategoryId(vObject.getCategoryId());
				alertconfigVb.setRecordIndicator(vObject.getRecordIndicator());
				alertconfigVb.setCategoryAlertStatus(vObject.getCategoryStatus());
				exceptionCode = etlCategoryAlertConfigDao.doInsertRecordForNonTrans(alertconfigVb);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		} else {
			etlCategoryAlertConfigDao.deletePendingRecordAll(vb);
		}
		EtlFeedCategoryDepedencyVb vbde = new EtlFeedCategoryDepedencyVb();
		vbde.setCountry(vObject.getCountry());
		vbde.setLeBook(vObject.getLeBook());
		vbde.setCategoryId(vObject.getCategoryId());
		if (vObject.getEtlCategoryDeplst() != null && vObject.getEtlCategoryDeplst().size() > 0) {
			etlCategoryDependencyDao.deletePendingRecordAll(vbde);
			for (EtlFeedCategoryDepedencyVb catDepVb : vObject.getEtlCategoryDeplst()) {
				catDepVb.setCountry(vObject.getCountry());
				catDepVb.setLeBook(vObject.getLeBook());
				catDepVb.setDependentCategory(catDepVb.getCategoryId());
				catDepVb.setCategoryId(vObject.getCategoryId());
				catDepVb.setRecordIndicator(vObject.getRecordIndicator());
				catDepVb.setCategoryDepStatus(vObject.getCategoryStatus());
				exceptionCode = etlCategoryDependencyDao.doInsertRecordForNonTrans(catDepVb);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		} else {
			etlCategoryDependencyDao.deletePendingRecordAll(vbde);
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);

	}

	protected ExceptionCode doUpdateApprRecordForNonTrans(EtlFeedCategoryVb vObject) throws RuntimeCustomException {
		List<EtlFeedCategoryVb> collTemp = null;
		EtlFeedCategoryVb vObjectlocal = null;
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
		vObjectlocal = ((ArrayList<EtlFeedCategoryVb>) collTemp).get(0);
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
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		EtlFeedCategoryAlertConfigVb vb = new EtlFeedCategoryAlertConfigVb();
		vb.setCountry(vObject.getCountry());
		vb.setLeBook(vObject.getLeBook());
		vb.setCategoryId(vObject.getCategoryId());
		if (vObject.getEtlCategoryAlertConfiglst() != null && vObject.getEtlCategoryAlertConfiglst().size() > 0) {
			etlCategoryAlertConfigDao.doDeleteApprAll(vb);
			for (EtlFeedCategoryAlertConfigVb alertconfigVb : vObject.getEtlCategoryAlertConfiglst()) {
				alertconfigVb.setCountry(vObject.getCountry());
				alertconfigVb.setLeBook(vObject.getLeBook());
				alertconfigVb.setCategoryId(vObject.getCategoryId());
				exceptionCode = etlCategoryAlertConfigDao.doInsertApprRecordForNonTrans(alertconfigVb);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		} else {
			etlCategoryAlertConfigDao.doDeleteApprAll(vb);
		}
		EtlFeedCategoryDepedencyVb vbd = new EtlFeedCategoryDepedencyVb();
		vbd.setCountry(vObject.getCountry());
		vbd.setLeBook(vObject.getLeBook());
		vbd.setCategoryId(vObject.getCategoryId());
		if (vObject.getEtlCategoryDeplst() != null && vObject.getEtlCategoryDeplst().size() > 0) {
			etlCategoryDependencyDao.doDeleteApprAll(vbd);
			for (EtlFeedCategoryDepedencyVb catDepVb : vObject.getEtlCategoryDeplst()) {
				catDepVb.setCountry(vObject.getCountry());
				catDepVb.setLeBook(vObject.getLeBook());
				catDepVb.setDependentCategory(catDepVb.getCategoryId());
				catDepVb.setCategoryId(vObject.getCategoryId());
				exceptionCode = etlCategoryDependencyDao.doInsertApprRecordForNonTrans(catDepVb);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		} else {
			etlCategoryDependencyDao.doDeleteApprAll(vbd);
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

	@SuppressWarnings("unchecked")
	public List<EtlFeedAlertMatrixVb> getAlertMatrixListing(EtlFeedAlertMatrixVb vObject) throws DataAccessException {
		String sql = "SELECT MATRIX_ID, MATRIX_ID" + getDbFunction(Constants.PIPELINE, null) + "' - '"
				+ getDbFunction(Constants.PIPELINE, null) + "MATRIX_DESCRIPTION"
				+ " MATRIX_DESCRIPTION FROM ETL_ALERT_MATRIX_MASTER WHERE MATRIX_ALERT_STATUS = 0 and COUNTRY= ?  AND LE_BOOK= ? ";
		sql = sql + " ORDER BY MATRIX_ID";
		Object[] args = { vObject.getCountry(), vObject.getLeBook() };
		return getJdbcTemplate().query(sql, args, getAlertMatrixListingMapper());
	}

	protected RowMapper getAlertMatrixListingMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedAlertMatrixVb vObject = new EtlFeedAlertMatrixVb();
				vObject.setMatrixID(rs.getString("MATRIX_ID"));
				vObject.setMatrixDescription(rs.getString("MATRIX_DESCRIPTION"));
				return vObject;
			}
		};
		return mapper;
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String table = "ETL_FEED_CATEGORY_PRS";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_CATEGORY = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
	

	public int deleteRecordByPRSDependentParentByCategory(EtlFeedMainVb vObject) {
		String table = "ETL_FEED_CATEGORY_DEP_PRS";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND CATEGORY_ID = ? AND SESSION_ID= ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedCategory(),
				vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

}
