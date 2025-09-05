package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.vision.authentication.CustomContextHolder;
import com.vision.exception.ExceptionCode;
import com.vision.exception.RuntimeCustomException;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlConnectorVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlMqColumnsVb;
import com.vision.vb.EtlMqTableVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlMqTableDao extends AbstractDao<EtlMqTableVb> {
	@Autowired
	private NumSubTabDao numSubTabDao;
	@Autowired
	private VisionUsersDao visionUsersDao;
	@Autowired
	private EtlMqColumnsDao etlMqColumnsDao;

	/******* Mapper Start **********/

	String QueryStautsNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.QUERY_STAUTS",
			"QUERY_STAUTS_DESC");
	String QueryStautsNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.QUERY_STAUTS",
			"QUERY_STAUTS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String QueryCategoryNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2008, "TAppr.QUERY_CATEGORY",
			"QUERY_CATEGORY_DESC");
	String QueryCategoryNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2008, "TPend.QUERY_CATEGORY",
			"QUERY_CATEGORY_DESC");

	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";
	
	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlMqTableVb vObject = new EtlMqTableVb();
				vObject.setWriteFlag(ValidationUtil.isValid(rs.getString("WRITE_FLAG"))?rs.getString("WRITE_FLAG"):"");
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
				if (rs.getString("QUERY_DESCRIPTION") != null) {
					vObject.setQueryDescription(rs.getString("QUERY_DESCRIPTION"));
				} else {
					vObject.setQueryDescription("");
				}
				if (rs.getString("QUERY") != null) {
					vObject.setQuery(rs.getString("QUERY"));
				} else {
					vObject.setQuery("");
				}
				if (rs.getString("VALIDATED_FLAG") != null) {
					vObject.setValidatedFlag(rs.getString("VALIDATED_FLAG"));
				} else {
					vObject.setValidatedFlag("");
				}
				vObject.setQueryCategoryNt(rs.getInt("QUERY_CATEGORY_NT"));
				vObject.setQueryCategory(rs.getInt("QUERY_CATEGORY"));
				vObject.setQueryCategoryNtDesc(rs.getString("QUERY_CATEGORY_DESC"));
				vObject.setQueryType(rs.getInt("QUERY_CATEGORY"));

				vObject.setQueryStatusNt(rs.getInt("QUERY_STAUTS_NT"));
				vObject.setQueryStatus(rs.getInt("QUERY_STAUTS"));
				vObject.setStatusDesc(rs.getString("QUERY_STAUTS_DESC"));

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
	protected RowMapper getLocalMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlMqTableVb vObject = new EtlMqTableVb();
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
				if (rs.getString("QUERY_DESCRIPTION") != null) {
					vObject.setQueryDescription(rs.getString("QUERY_DESCRIPTION"));
				} else {
					vObject.setQueryDescription("");
				}
				if (rs.getString("QUERY") != null) {
					vObject.setQuery(rs.getString("QUERY"));
				} else {
					vObject.setQuery("");
				}
				if (rs.getString("VALIDATED_FLAG") != null) {
					vObject.setValidatedFlag(rs.getString("VALIDATED_FLAG"));
				} else {
					vObject.setValidatedFlag("");
				}
				vObject.setQueryCategoryNt(rs.getInt("QUERY_CATEGORY_NT"));
				vObject.setQueryCategory(rs.getInt("QUERY_CATEGORY"));
				vObject.setQueryType(rs.getInt("QUERY_CATEGORY"));

				vObject.setQueryCategoryNtDesc(rs.getString("QUERY_CATEGORY_DESC"));

				vObject.setQueryStatusNt(rs.getInt("QUERY_STAUTS_NT"));
				vObject.setQueryStatus(rs.getInt("QUERY_STAUTS"));
				vObject.setStatusDesc(rs.getString("QUERY_STAUTS_DESC"));

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
	public List<EtlMqTableVb> getQueryPopupResults(EtlMqTableVb dObj) {
		setServiceDefaults();
		Vector<Object> params = new Vector<Object>();
		String accessQueyr = ", ETL_CONNECTOR_ACCESS FCA, VISION_USERS VU "
				+ " WHERE (TAPPR.Connector_ID =  FCA.Connector_ID  AND FCA.USER_GROUP = VU.USER_GROUP "
				+ " AND FCA.USER_PROFILE = VU.USER_PROFILE  AND VU.VISION_ID = " + intCurrentUserId + " ) ";
		StringBuffer strBufApprove = new StringBuffer("SELECT FCA.write_flag, TAPPR.COUNTRY," + CountryApprDesc
				+ ",TAPPR.LE_BOOK," + LeBookApprDesc + ",TAPPR.CONNECTOR_ID,TAPPR.QUERY_ID"
				+ ",TAPPR.QUERY_DESCRIPTION,TAPPR.QUERY QUERY, TAPPR.VALIDATED_FLAG, TAPPR.QUERY_STAUTS_NT"
				+ ",TAPPR.QUERY_STAUTS," + QueryStautsNtApprDesc + ",TAPPR.RECORD_INDICATOR_NT"
				+ ",TAPPR.RECORD_INDICATOR," + RecordIndicatorNtApprDesc + ",TAPPR.MAKER," + makerApprDesc
				+ ",TAPPR.VERIFIER," + verifierApprDesc + ",TAPPR.INTERNAL_STATUS,"
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
				+ ",TAPPR.DATE_LAST_MODIFIED date_modified," + getDbFunction(Constants.DATEFUNC, null)
				+ "(TAPPR.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION,TAPPR.QUERY_CATEGORY_NT"
				+ ",TAPPR.QUERY_CATEGORY, " + QueryCategoryNtApprDesc + " FROM ETL_MQ_TABLE TAPPR " + accessQueyr);

		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_MQ_TABLE_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK "
				+ " AND TAppr.CONNECTOR_ID = TPend.CONNECTOR_ID AND TAppr.QUERY_ID = TPend.QUERY_ID )");

		StringBuffer strBufPending = new StringBuffer("SELECT FCA.write_flag, TPEND.COUNTRY," + CountryPendDesc
				+ ",TPEND.LE_BOOK," + LeBookTPendDesc + ",TPEND.CONNECTOR_ID, TPEND.QUERY_ID"
				+ ",TPEND.QUERY_DESCRIPTION, TPEND.QUERY QUERY,TPEND.VALIDATED_FLAG, TPEND.QUERY_STAUTS_NT"
				+ ",TPEND.QUERY_STAUTS," + QueryStautsNtPendDesc + ",TPEND.RECORD_INDICATOR_NT"
				+ ",TPEND.RECORD_INDICATOR, " + RecordIndicatorNtPendDesc + ",TPEND.MAKER, " + makerPendDesc
				+ ",TPEND.VERIFIER," + verifierPendDesc + ",TPEND.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED"
				+ ",TPEND.DATE_LAST_MODIFIED date_modified, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TPEND.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION,TPEND.QUERY_CATEGORY_NT"
				+ ",TPEND.QUERY_CATEGORY, " + QueryCategoryNtPendDesc + " FROM ETL_MQ_TABLE_PEND TPEND  "
				+ accessQueyr.replaceAll("TAPPR", "TPEND"));
		/*
		 * if (dObj.getConnectorId() != null &&
		 * ValidationUtil.isValid(dObj.getConnectorId())) {
		 * strBufApprove.append(" WHERE  UPPER(TAPPR.CONNECTOR_ID) = '" +
		 * dObj.getConnectorId().toUpperCase() + "'");
		 * strBufPending.append(" WHERE  UPPER(TPEND.CONNECTOR_ID) = '" +
		 * dObj.getConnectorId().toUpperCase() + "'"); }
		 */
		if (ValidationUtil.isValid(dObj.getConnectorId())) {
			params.addElement(dObj.getConnectorId().toUpperCase());
			CommonUtils.addToQuery("UPPER(TAPPR.CONNECTOR_ID) = ?", strBufApprove);
			CommonUtils.addToQuery("UPPER(TPEND.CONNECTOR_ID) = ?", strBufPending);
		}
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
						CommonUtils.addToQuery(" upper(TAppr.COUNTRY) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.COUNTRY) " + val, strBufPending);
						break;

					case "countryDesc":
						List<EtlMqTableVb> countryData = findCountryByCustomFilter(val);
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
						List<EtlMqTableVb> leBookData = findLeBookByCustomFilter(val);
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
						

					case "connectorId":
						CommonUtils.addToQuery(" upper(TAppr.CONNECTOR_ID) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CONNECTOR_ID) " + val, strBufPending);
						break;

					case "queryId":
						CommonUtils.addToQuery(" upper(TAppr.QUERY_ID) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.QUERY_ID) " + val, strBufPending);
						break;

					case "queryDescription":
						addToQuerySearch(" upper(TAppr.QUERY_DESCRIPTION) " + val, strBufApprove, data.getJoinType());
						addToQuerySearch(" upper(TPend.QUERY_DESCRIPTION) " + val, strBufPending, data.getJoinType());
						break;

					case "query":
						addToQuerySearch(" upper(TAppr.QUERY) " + val, strBufApprove, data.getJoinType());
						addToQuerySearch(" upper(TPend.QUERY) " + val, strBufPending, data.getJoinType());
						break;

					case "validatedFlag":
						addToQuerySearch(" upper(TAppr.VALIDATED_FLAG) " + val, strBufApprove, data.getJoinType());
						addToQuerySearch(" upper(TPend.VALIDATED_FLAG) " + val, strBufPending, data.getJoinType());
						break;
					case "queryCategory":
						addToQuerySearch(" upper(TAppr.QUERY_CATEGORY) " + val, strBufApprove, data.getJoinType());
						addToQuerySearch(" upper(TPend.QUERY_CATEGORY) " + val, strBufPending, data.getJoinType());
						break;

					case "queryCategoryNtDesc":
						List<NumSubTabVb> numSTData1 = numSubTabDao.findNumSubTabByCustomFilterAndNumTab(val, 2008);
						String actData1 = "";
						if (numSTData1 != null && numSTData1.size() > 0) {
							for (int k = 0; k < numSTData1.size(); k++) {
								int numsubtab = numSTData1.get(k).getNumSubTab();
								if (!ValidationUtil.isValid(actData1)) {
									actData1 = "'" + Integer.toString(numsubtab) + "'";
								} else {
									actData1 = actData1 + ",'" + Integer.toString(numsubtab) + "'";
								}
							}
							addToQuerySearch(" (TAPPR.QUERY_CATEGORY) IN (" + actData1 + ") ", strBufApprove,
									data.getJoinType());
							addToQuerySearch(" (TPEND.QUERY_CATEGORY) IN (" + actData1 + ") ", strBufPending,
									data.getJoinType());
						} else {
							CommonUtils.addToQuery(" (TAPPR.QUERY_CATEGORY) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.QUERY_CATEGORY) IN ('') ", strBufPending);
						}
						break;

					case "queryStatus":
						addToQuerySearch(" upper(TAppr.QUERY_STAUTS) " + val, strBufApprove, data.getJoinType());
						addToQuerySearch(" upper(TPend.QUERY_STAUTS) " + val, strBufPending, data.getJoinType());
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
							CommonUtils.addToQuery(" (TAPPR.QUERY_STAUTS) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.QUERY_STAUTS) IN (" + actData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.QUERY_STAUTS) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.QUERY_STAUTS) IN ('') ", strBufPending);
						}
						break;
						
					case "recordIndicator":
						addToQuerySearch(" upper(TAppr.RECORD_INDICATOR) " + val, strBufApprove, data.getJoinType());
						addToQuerySearch(" upper(TPend.RECORD_INDICATOR) " + val, strBufPending, data.getJoinType());
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
					}
					count++;
				}
			}
			String orderBy = " Order By date_modified desc";
			return getQueryPopupResultsForCLOB(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlMqTableVb> getQueryResults(EtlMqTableVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlMqTableVb> collTemp = null;
		final int intKeyFieldsCount = 4;
		String strQueryAppr = new String("SELECT  TAppr.COUNTRY," + CountryApprDesc + ",TAppr.LE_BOOK, "
				+ LeBookApprDesc + ", TAPPR.CONNECTOR_ID ,TAPPR.QUERY_ID ,TAPPR.QUERY_DESCRIPTION"
				+ ",TAPPR.QUERY QUERY ,TAPPR.VALIDATED_FLAG ,TAPPR.QUERY_STAUTS_NT,TAPPR.QUERY_STAUTS,"
				+ QueryStautsNtApprDesc + ",TAPPR.RECORD_INDICATOR_NT,TAPPR.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc + ",TAPPR.MAKER," + makerApprDesc + ",TAPPR.VERIFIER,"
				+ verifierApprDesc + ",TAPPR.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION ,TAppr.QUERY_CATEGORY_NT"
				+ ",TAPPR.QUERY_CATEGORY, " + QueryCategoryNtApprDesc + " FROM ETL_MQ_TABLE TAPPR "
				+ " WHERE  UPPER(TAPPR.COUNTRY) = ?  AND UPPER(TAPPR.LE_BOOK) = ? and  UPPER(TAPPR.CONNECTOR_ID) = ? "
				+ " AND UPPER(TAPPR.QUERY_ID) = ?  ");
		String strQueryPend = new String("SELECT TPEND.COUNTRY," + CountryPendDesc + ",TPEND.LE_BOOK, "
				+ LeBookTPendDesc + ", TPEND.CONNECTOR_ID ,TPEND.QUERY_ID ,TPEND.QUERY_DESCRIPTION"
				+ ",TPEND.QUERY QUERY ,TPEND.VALIDATED_FLAG ,TPEND.QUERY_STAUTS_NT,TPEND.QUERY_STAUTS,"
				+ QueryStautsNtPendDesc + ",TPEND.RECORD_INDICATOR_NT,TPEND.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc + ",TPEND.MAKER," + makerPendDesc + ",TPEND.VERIFIER,"
				+ verifierPendDesc + ",TPEND.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION ,TPEND.QUERY_CATEGORY_NT"
				+ ",TPEND.QUERY_CATEGORY, " + QueryCategoryNtPendDesc
				+ " FROM ETL_MQ_TABLE_PEND TPEND WHERE UPPER(TPend.COUNTRY) = ?  AND UPPER(TPend.LE_BOOK) = ? "
				+ " and UPPER(TPEND.CONNECTOR_ID) = ?  AND UPPER(TPEND.QUERY_ID) = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry().toUpperCase();
		objParams[1] = dObj.getLeBook().toUpperCase();
		objParams[2] = dObj.getConnectorId().toUpperCase();
		objParams[3] = dObj.getQueryId().toUpperCase();
		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getLocalMapper());
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getLocalMapper());
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
	protected List<EtlMqTableVb> selectApprovedRecord(EtlMqTableVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlMqTableVb> doSelectPendingRecord(EtlMqTableVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlMqTableVb records) {
		return records.getQueryStatus();
	}

	@Override
	protected void setStatus(EtlMqTableVb vObject, int status) {
		vObject.setQueryStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlMqTableVb vObject) {
		vObject.setQueryCategory(vObject.getQueryType());
		String query = "Insert Into ETL_MQ_TABLE ( COUNTRY, LE_BOOK,CONNECTOR_ID, QUERY_ID, QUERY_DESCRIPTION, VALIDATED_FLAG, "
				+ "QUERY_STAUTS_NT, QUERY_STAUTS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,QUERY_CATEGORY_NT, QUERY_CATEGORY,QUERY )"
				+ "Values (?,?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ",?,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId(),
				vObject.getQueryDescription(), vObject.getValidatedFlag(), vObject.getQueryStatusNt(),
				vObject.getQueryStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getQueryCategoryNt(),
				vObject.getQueryCategory() };

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
					String clobData = ValidationUtil.isValid(vObject.getQuery()) ? vObject.getQuery() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
			result = Constants.WE_HAVE_ERROR_DESCRIPTION;
		}
		return result;
	}
	@Override
	protected int doInsertionPend(EtlMqTableVb vObject) {
		vObject.setQueryCategory(vObject.getQueryType());
		String query = "Insert Into ETL_MQ_TABLE_PEND ( COUNTRY, LE_BOOK,CONNECTOR_ID, QUERY_ID, QUERY_DESCRIPTION, VALIDATED_FLAG, QUERY_STAUTS_NT,"
				+ " QUERY_STAUTS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,QUERY_CATEGORY_NT, QUERY_CATEGORY,QUERY )"
				+ "Values ( ?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ",?,?,?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId(),
				vObject.getQueryDescription(), vObject.getValidatedFlag(), vObject.getQueryStatusNt(),
				vObject.getQueryStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getQueryCategoryNt(),
				vObject.getQueryCategory() };

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
					String clobData = ValidationUtil.isValid(vObject.getQuery()) ? vObject.getQuery() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
			result = Constants.WE_HAVE_ERROR_DESCRIPTION;
		}
		return result;
	}
	@Override
	protected int doInsertionPendWithDc(EtlMqTableVb vObject) {
		vObject.setQueryCategory(vObject.getQueryType());
		String query = "Insert Into ETL_MQ_TABLE_PEND ( COUNTRY, LE_BOOK,CONNECTOR_ID, QUERY_ID, QUERY_DESCRIPTION, VALIDATED_FLAG, QUERY_STAUTS_NT, "
				+ "QUERY_STAUTS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION,QUERY_CATEGORY_NT, QUERY_CATEGORY,QUERY )"
				+ "Values (?,?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + ", ?, ?, ?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId(),
				vObject.getQueryDescription(), vObject.getValidatedFlag(), vObject.getQueryStatusNt(),
				vObject.getQueryStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getQueryCategoryNt(),
				vObject.getQueryCategory() };

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
					String clobData = ValidationUtil.isValid(vObject.getQuery()) ? vObject.getQuery() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
			result = Constants.WE_HAVE_ERROR_DESCRIPTION;
		}
		return result;
	}
	@Override
	protected int doUpdateAppr(EtlMqTableVb vObject) {
		vObject.setQueryCategory(vObject.getQueryType());
		String query = "Update ETL_MQ_TABLE Set QUERY = ?, QUERY_DESCRIPTION = ?,  VALIDATED_FLAG = ?, QUERY_STAUTS_NT = ?, QUERY_STAUTS = ?,"
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + ", QUERY_CATEGORY_NT = ?, QUERY_CATEGORY = ?  "
				+ " WHERE   COUNTRY =? and  LE_BOOK =? and CONNECTOR_ID = ?  AND QUERY_ID = ? ";
		Object[] args = { vObject.getQueryDescription(), vObject.getValidatedFlag(), vObject.getQueryStatusNt(),
				vObject.getQueryStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getQueryCategoryNt(),
				vObject.getQueryCategory(), vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(),
				vObject.getQueryId() };

		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getQuery()) ? vObject.getQuery() : "";
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
			result = Constants.WE_HAVE_ERROR_DESCRIPTION;
		}
		return result;
	}
	@Override
	protected int doUpdatePend(EtlMqTableVb vObject) {
		vObject.setQueryCategory(vObject.getQueryType());
		String query = "Update ETL_MQ_TABLE_PEND Set QUERY = ?, QUERY_DESCRIPTION = ?,  VALIDATED_FLAG = ?, QUERY_STAUTS_NT = ?, QUERY_STAUTS = ?, "
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + ",  QUERY_CATEGORY_NT = ?, QUERY_CATEGORY = ? "
				+ " WHERE  COUNTRY =? and  LE_BOOK =? and CONNECTOR_ID = ?  AND QUERY_ID = ? ";
		Object[] args = { vObject.getQueryDescription(), vObject.getValidatedFlag(), vObject.getQueryStatusNt(),
				vObject.getQueryStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(), vObject.getQueryCategoryNt(),
				vObject.getQueryCategory(), vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(),
				vObject.getQueryId() };

		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getQuery()) ? vObject.getQuery() : "";
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
			result = Constants.WE_HAVE_ERROR_DESCRIPTION;
		}
		return result;
	}
	@Override
	protected int doDeleteAppr(EtlMqTableVb vObject) {
		String query = "Delete From ETL_MQ_TABLE Where  COUNTRY =? and  LE_BOOK =? and CONNECTOR_ID = ?  AND QUERY_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(),vObject.getConnectorId(), vObject.getQueryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlMqTableVb vObject) {
		String query = "Delete From ETL_MQ_TABLE_PEND Where   COUNTRY =? and  LE_BOOK =? and CONNECTOR_ID = ?  AND QUERY_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(),vObject.getConnectorId(), vObject.getQueryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlMqTableVb vObject) {
		vObject.setQueryCategory(vObject.getQueryType()); 
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

		if (ValidationUtil.isValid(vObject.getQueryDescription()))
			strAudit.append("QUERY_DESCRIPTION" + auditDelimiterColVal + vObject.getQueryDescription().trim());
		else
			strAudit.append("QUERY_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getQuery()))
			strAudit.append("QUERY" + auditDelimiterColVal + vObject.getQuery().trim());
		else
			strAudit.append("QUERY" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getValidatedFlag()))
			strAudit.append("VALIDATED_FLAG" + auditDelimiterColVal + vObject.getValidatedFlag().trim());
		else
			strAudit.append("VALIDATED_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("QUERY_STAUTS_NT" + auditDelimiterColVal + vObject.getQueryStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("QUERY_STAUTS" + auditDelimiterColVal + vObject.getQueryStatus());
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

		strAudit.append("QUERY_CATEGORY_NT" + auditDelimiterColVal + vObject.getQueryCategoryNt());
		strAudit.append(auditDelimiter);

		strAudit.append("QUERY_CATEGORY" + auditDelimiterColVal + vObject.getQueryCategory());
		strAudit.append(auditDelimiter);

		return strAudit.toString();
	}

	@Override
	protected void setServiceDefaults() {
		serviceName = "EtlMqTable";
		serviceDesc = "ETL Mq Table";
		tableName = "ETL_MQ_TABLE";
		childTableName = "ETL_MQ_TABLE";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();

	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doInsertRecordForNonTrans(EtlMqTableVb vObject) throws RuntimeCustomException {
		List<EtlMqTableVb> collTemp = null;
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
			int staticDeletionFlag = getStatus(((ArrayList<EtlMqTableVb>) collTemp).get(0));
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
			EtlMqTableVb vObjectLocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
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
			if (vObject.getEtlMqColumnsList() != null && vObject.getEtlMqColumnsList().size() > 0) {
				int i=1;
				for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
					lObject.setVerificationRequired(vObject.isVerificationRequired());
					lObject.setStaticDelete(vObject.isStaticDelete());
					lObject.setColumnSortOrder(i);
					lObject.setMaker(getIntCurrentUserId());
					lObject.setVerifier(0);
					lObject.setRecordIndicator(Constants.STATUS_INSERT);
					lObject.setDateLastModified(systemDate);
					lObject.setDateCreation(systemDate);
					retVal = etlMqColumnsDao.doInsertionPend(lObject);
					i++;
				}
			}
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		exceptionCode = getResultObject(retVal);
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doInsertApprRecordForNonTrans(EtlMqTableVb vObject) throws RuntimeCustomException {
		List<EtlMqTableVb> collTemp = null;
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
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlMqTableVb>) collTemp).get(0));
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
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		vObject.setDateCreation(systemDate);
		exceptionCode = writeAuditLog(vObject, null);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (vObject.getEtlMqColumnsList() != null && vObject.getEtlMqColumnsList().size() > 0) {
			int i=1;
			for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setColumnSortOrder(i);
				lObject.setRecordIndicator(Constants.STATUS_ZERO);
				lObject.setVerifier(getIntCurrentUserId());
				retVal = etlMqColumnsDao.doInsertionAppr(lObject);
				i++;
			}
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doUpdateApprRecordForNonTrans(EtlMqTableVb vObject) throws RuntimeCustomException {
		List<EtlMqTableVb> collTemp = null;
		EtlMqTableVb vObjectlocal = null;
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
		vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
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
		if (vObject.getEtlMqColumnsList() != null && vObject.getEtlMqColumnsList().size() > 0) {
			int i=1;
			for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setColumnSortOrder(i);
				lObject.setMaker(getIntCurrentUserId());
				lObject.setRecordIndicator(Constants.STATUS_ZERO);
				lObject.setVerifier(getIntCurrentUserId());
				lObject.setDateCreation(vObjectlocal.getDateCreation());
				retVal = etlMqColumnsDao.doUpdateAppr(lObject);
				if (retVal == Constants.STATUS_ZERO) {
					retVal = etlMqColumnsDao.doInsertionAppr(lObject);
				}
				i++;
			}
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
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
	protected ExceptionCode doUpdateRecordForNonTrans(EtlMqTableVb vObject) throws RuntimeCustomException {
		List<EtlMqTableVb> collTemp = null;
		EtlMqTableVb vObjectlocal = null;
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
			vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);

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
			if (vObject.getEtlMqColumnsList() != null && vObject.getEtlMqColumnsList().size() > 0) {
				int i=1;
				EtlMqTableVb lObject1 = new EtlMqTableVb();
				lObject1.setCountry(vObject.getCountry());
				lObject1.setLeBook(vObject.getLeBook());
				lObject1.setConnectorId(vObject.getConnectorId());
				lObject1.setQueryId(vObject.getQueryId());
				etlMqColumnsDao.deletePendingParentRecord(lObject1);
				for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
					lObject.setVerificationRequired(vObject.isVerificationRequired());
					lObject.setStaticDelete(vObject.isStaticDelete());
					lObject.setColumnSortOrder(i);
					lObject.setVerifier(0);
					lObject.setMaker(getIntCurrentUserId());
					lObject.setDateCreation(vObjectlocal.getDateCreation());
					lObject.setRecordIndicator(vObject.getRecordIndicator());
					retVal = etlMqColumnsDao.doUpdatePend(lObject);
					if (retVal == Constants.STATUS_ZERO) {
						retVal = etlMqColumnsDao.doInsertionPend(lObject);
					}
					i++;
				}
			}
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
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
				vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
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
			if (vObject.getEtlMqColumnsList() != null && vObject.getEtlMqColumnsList().size() > 0) {
				int i=1;
				for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
					lObject.setVerificationRequired(vObject.isVerificationRequired());
					lObject.setStaticDelete(vObject.isStaticDelete());
					lObject.setColumnSortOrder(i);
					lObject.setDateCreation(vObjectlocal.getDateCreation());
					lObject.setVerifier(0);
					lObject.setMaker(getIntCurrentUserId());
					lObject.setRecordIndicator(Constants.STATUS_UPDATE);
					retVal = etlMqColumnsDao.doInsertionPendWithDc(lObject);
					i++;
				}
			}
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doDeleteRecordForNonTrans(EtlMqTableVb vObject) throws RuntimeCustomException {
		EtlMqTableVb vObjectlocal = null;
		List<EtlMqTableVb> collTemp = null;
		ExceptionCode exceptionCode = new ExceptionCode();
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
			vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
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
			vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
			vObjectlocal.setDateCreation(vObject.getDateCreation());
		}
		// vObjectlocal.setDateCreation(vObject.getDateCreation());
		vObjectlocal.setMaker(getIntCurrentUserId());
		vObjectlocal.setRecordIndicator(Constants.STATUS_DELETE);
		vObjectlocal.setVerifier(0);
		retVal = doInsertionPendWithDc(vObjectlocal);
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		List<EtlMqColumnsVb> etlMqColumnsList = etlMqColumnsDao.getQueryResultsAllColumns(vObject.getConnectorId(),vObject.getQueryId(),Constants.STATUS_ZERO);
		vObject.setEtlMqColumnsList(etlMqColumnsList);
		if (vObject.getEtlMqColumnsList() != null && vObject.getEtlMqColumnsList().size() > 0) {
			int i=1;
			for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
				lObject.setVerificationRequired(vObject.isVerificationRequired());
				lObject.setStaticDelete(vObject.isStaticDelete());
				lObject.setColumnSortOrder(i);
				lObject.setRecordIndicator(Constants.STATUS_DELETE);
				lObject.setVerifier(0);
				retVal = etlMqColumnsDao.doInsertionPendWithDc(lObject);
				i++;
			}
		}else {
			exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (retVal != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doDeleteApprRecordForNonTrans(EtlMqTableVb vObject) throws RuntimeCustomException {
		List<EtlMqTableVb> collTemp = null;
		ExceptionCode exceptionCode = new ExceptionCode();
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		EtlMqTableVb vObjectlocal = null;
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
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlMqTableVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
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
			
			List<EtlMqColumnsVb> etlMqColumnsList = etlMqColumnsDao.getQueryResultsAllColumns(vObject.getConnectorId(),vObject.getQueryId(), Constants.STATUS_ZERO);
			vObject.setEtlMqColumnsList(etlMqColumnsList);
			if (vObject.getEtlMqColumnsList() != null && vObject.getEtlMqColumnsList().size() > 0) {
				int i=1;
				for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
					lObject.setVerificationRequired(vObject.isVerificationRequired());
					lObject.setStaticDelete(vObject.isStaticDelete());
					lObject.setColumnSortOrder(i);
					lObject.setMaker(getIntCurrentUserId());
					lObject.setVerifier(getIntCurrentUserId());
					lObject.setRecordIndicator(Constants.STATUS_ZERO);
					lObject.setMqColumnStatus(Constants.PASSIVATE);
					lObject.setVerifier(getIntCurrentUserId());
					lObject.setRecordIndicator(Constants.STATUS_ZERO);
					retVal = etlMqColumnsDao.doUpdateAppr(lObject);
					if(retVal ==Constants.STATUS_ZERO) {
						retVal = etlMqColumnsDao.doInsertionAppr(lObject);
					}
					i++;
				}
			}else {
				exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (retVal != Constants.SUCCESSFUL_OPERATION) {
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}

		} else {
			// delete the record from the Approve Table
			retVal = doDeleteAppr(vObject);
//			vObject.setRecordIndicator(-1);
			String systemDate = getSystemDate();
			vObject.setDateLastModified(systemDate);

			retVal = etlMqColumnsDao.doDeleteParentAppr(vObject);

		}
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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doRejectRecord(EtlMqTableVb vObject) throws RuntimeCustomException {
		EtlMqTableVb vObjectlocal = null;
		List<EtlMqTableVb> collTemp = null;
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
			vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
			// Delete the record from the Pending table
			if (deletePendingRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (etlMqColumnsDao.deletePendingParentRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
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

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doApproveRecord(EtlMqTableVb vObject, boolean staticDelete) throws RuntimeCustomException {
		EtlMqTableVb oldContents = null;
		EtlMqTableVb vObjectlocal = null;
		List<EtlMqTableVb> collTemp = null;
		ExceptionCode exceptionCode = new ExceptionCode();
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

			vObjectlocal = ((ArrayList<EtlMqTableVb>) collTemp).get(0);

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
				oldContents = ((ArrayList<EtlMqTableVb>) collTemp).get(0);
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
				List<EtlMqColumnsVb> etlMqColumnsList = etlMqColumnsDao.getQueryResultsAllColumns(vObject.getConnectorId(),vObject.getQueryId(), Constants.STATUS_PENDING);
				vObject.setEtlMqColumnsList(etlMqColumnsList);
				if(vObject.getEtlMqColumnsList()!= null && vObject.getEtlMqColumnsList().size()>0) {
					int i=1;
					for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
						lObject.setVerificationRequired(vObject.isVerificationRequired());
						lObject.setStaticDelete(vObject.isStaticDelete());
						lObject.setColumnSortOrder(i);
						lObject.setRecordIndicator(Constants.STATUS_ZERO);
						lObject.setVerifier(getIntCurrentUserId());
						retVal = etlMqColumnsDao.doInsertionAppr(lObject);
					}
				}
				else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
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
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}

				List<EtlMqColumnsVb>  etlMqColumnsList = etlMqColumnsDao.getQueryResultsAllColumns(vObject.getConnectorId(),vObject.getQueryId(), Constants.STATUS_PENDING);
				vObject.setEtlMqColumnsList(etlMqColumnsList);
				if(vObject.getEtlMqColumnsList()!= null && vObject.getEtlMqColumnsList().size()>0) {
					int i=1;
					EtlMqTableVb lObject1 = new EtlMqTableVb();
					lObject1.setCountry(vObject.getCountry());
					lObject1.setLeBook(vObject.getLeBook());
					lObject1.setConnectorId(vObject.getConnectorId());
					lObject1.setQueryId(vObject.getQueryId());
					etlMqColumnsDao.doDeleteParentAppr(lObject1);
					for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
						lObject.setVerificationRequired(vObject.isVerificationRequired());
						lObject.setStaticDelete(vObject.isStaticDelete());
						lObject.setColumnSortOrder(i);
						lObject.setVerifier(getIntCurrentUserId());
						lObject.setRecordIndicator(Constants.STATUS_ZERO);
						retVal = etlMqColumnsDao.doUpdateAppr(lObject);
						if(retVal ==Constants.STATUS_ZERO) {
							retVal = etlMqColumnsDao.doInsertionAppr(lObject);
						}
					}
				} else {
					exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
					throw buildRuntimeCustomException(exceptionCode);
				}
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
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
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					
					List<EtlMqColumnsVb>  etlMqColumnsList = etlMqColumnsDao.getQueryResultsAllColumns(vObject.getConnectorId(),vObject.getQueryId(), Constants.STATUS_PENDING);
					vObject.setEtlMqColumnsList(etlMqColumnsList);
					if(vObject.getEtlMqColumnsList()!= null && vObject.getEtlMqColumnsList().size()>0) {
						int i=1;
						EtlMqTableVb lObject1 = new EtlMqTableVb();
						lObject1.setCountry(vObject.getCountry());
						lObject1.setLeBook(vObject.getLeBook());
						lObject1.setConnectorId(vObject.getConnectorId());
						lObject1.setQueryId(vObject.getQueryId());
						etlMqColumnsDao.doDeleteParentAppr(lObject1);
						for (EtlMqColumnsVb lObject : vObject.getEtlMqColumnsList()) {
							lObject.setVerificationRequired(vObject.isVerificationRequired());
							lObject.setStaticDelete(vObject.isStaticDelete());
							lObject.setColumnSortOrder(i);
							lObject.setMqColumnStatus(Constants.PASSIVATE);
							lObject.setVerifier(getIntCurrentUserId());
							lObject.setRecordIndicator(Constants.STATUS_ZERO);
							retVal = etlMqColumnsDao.doUpdateAppr(lObject);
							if(retVal ==Constants.STATUS_ZERO) {
								retVal = etlMqColumnsDao.doInsertionAppr(lObject);
							}
						}
					}
					else {
						exceptionCode.setErrorCode(Constants.ERRONEOUS_OPERATION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					setStatus(vObject, Constants.PASSIVATE);
					String systemDate = getSystemDate();
					vObject.setDateLastModified(systemDate);

				} else {
					// Delete the existing record from the Approved table
					retVal = doDeleteAppr(vObjectlocal);
					retVal = etlMqColumnsDao.doDeleteParentAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
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
				exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			retVal = etlMqColumnsDao.deletePendingParentRecord(vObjectlocal);

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

	public List<EtlMqTableVb> getQueryResultsByParent(String connectorId, int intStatus) {
		setServiceDefaults();
		List<EtlMqTableVb> collTemp = null;
		final int intKeyFieldsCount = 1;
		String strQueryAppr = new String("SELECT TAPPR.CONNECTOR_ID ,TAPPR.QUERY_ID ,TAPPR.QUERY_DESCRIPTION"
				+ ",TAPPR.QUERY QUERY ,TAPPR.VALIDATED_FLAG ,TAPPR.QUERY_STAUTS_NT,TAppr.QUERY_STAUTS,"
				+ QueryStautsNtApprDesc + ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,"
				+ RecordIndicatorNtApprDesc + ",TAppr.MAKER," + makerApprDesc + ",TAppr.VERIFIER,"
				+ verifierApprDesc + ",TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAPPR.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION ,TAPPR.QUERY_CATEGORY_NT"
				+ ",TAPPR.QUERY_CATEGORY, " + QueryCategoryNtApprDesc + " FROM ETL_MQ_TABLE TAPPR "
				+ " WHERE  UPPER(TAPPR.CONNECTOR_ID) = ? ");
		String strQueryPend = new String("SELECT TPEND.CONNECTOR_ID ,TPEND.QUERY_ID ,TPEND.QUERY_DESCRIPTION"
				+ ",TPEND.QUERY QUERY ,TPEND.VALIDATED_FLAG ,TPEND.QUERY_STAUTS_NT,TPend.QUERY_STAUTS,"
				+ QueryStautsNtPendDesc + ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,"
				+ RecordIndicatorNtPendDesc + ",TPend.MAKER," + makerPendDesc + ",TPend.VERIFIER,"
				+ verifierPendDesc + ",TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPEND.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION ,TPEND.QUERY_CATEGORY_NT"
				+ ",TPEND.QUERY_CATEGORY, " + QueryCategoryNtPendDesc
				+ " FROM ETL_MQ_TABLE_PEND TPEND WHERE UPPER(TPEND.CONNECTOR_ID) = ? ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = connectorId.toUpperCase();

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
	public static void addToQuerySearch(String strFld, StringBuffer strBuf, String condition){
		if (strBuf.toString().endsWith(") ") == true || strBuf.toString().endsWith("?") == true
				|| strBuf.toString().endsWith("IS NULL") == true || strBuf.toString().endsWith("AND ") == true
				|| strBuf.toString().endsWith("OR ") == true)
			strBuf.append(" AND "+strFld + condition + " ");
		else
			strBuf.append(" WHERE " + strFld + condition + " ");
	}
	
	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType){
		String table = "ETL_MQ_TABLE_PRS";
		String query = "Delete From "+table+" Where FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = {  vObject.getFeedId() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
	public List<EtlMqTableVb> findCountryByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY FROM COUNTRIES CT WHERE UPPER(CT.COUNTRY_DESCRIPTION) " + val + "ORDER BY COUNTRY";
		return  getJdbcTemplate().query(sql, getMapperForCountry());
	}
	
	protected RowMapper getMapperForCountry(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlMqTableVb etlFeedCategoryVb = new EtlMqTableVb();
				etlFeedCategoryVb.setCountry(rs.getString("COUNTRY"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}
	public List<EtlMqTableVb> findLeBookByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
				+ getDbFunction(Constants.PIPELINE, null)
				+ "LE_BOOK CL FROM LE_BOOK LEB WHERE LEB_STATUS=0 AND UPPER(LEB.LEB_DESCRIPTION) " + val
				+ "ORDER BY LE_BOOK";
		return  getJdbcTemplate().query(sql, getMapperForLeBook());
	}
	protected RowMapper getMapperForLeBook(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlMqTableVb etlFeedCategoryVb = new EtlMqTableVb();
				etlFeedCategoryVb.setLeBook(rs.getString("CL"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject){
		String table = "ETL_MQ_TABLE_PRS";
		String query = "Delete From "+table+" Where FEED_CATEGORY = ? AND SESSION_ID=? ";
		Object[] args = {  vObject.getFeedCategory() , vObject.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
	
}