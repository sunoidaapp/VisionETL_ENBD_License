package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
import com.vision.vb.AlphaSubTabVb;
import com.vision.vb.EtlConnectorAccessVb;
import com.vision.vb.EtlConnectorVb;
import com.vision.vb.EtlFeedMainVb;
import com.vision.vb.EtlFileColumnsVb;
import com.vision.vb.EtlFileTableVb;
import com.vision.vb.EtlFileUploadAreaVb;
import com.vision.vb.EtlMqTableVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlConnectorDao extends AbstractDao<EtlConnectorVb> {
	@Autowired
	private AlphaSubTabDao alphaSubTabDao;
	@Autowired
	private NumSubTabDao numSubTabDao;
	@Autowired
	private VisionUsersDao visionUsersDao;
	@Autowired
	private EtlMqTableDao etlMqTableDao;
	@Autowired
	private EtlFileUploadAreaDao etlFileUploadAreaDao;
	
	@Autowired
	private EtlFileTableDao etlFileTableDao;
	@Autowired
	private EtlFileColumnsDao etlFileColumnsDao;
	
	@Autowired
	private EtlConnectorAccessDao etlConnectorAccessDao;
	
	
	/******* Mapper Start **********/
	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorVb vObject = new EtlConnectorVb();
				List<EtlConnectorVb> collTemp = new ArrayList<EtlConnectorVb>();
				vObject.setWriteFlag(ValidationUtil.isValid(rs.getString("WRITE_FLAG"))?rs.getString("WRITE_FLAG"):"");
				vObject.setCountry(ValidationUtil.isValid(rs.getString("COUNTRY"))?rs.getString("COUNTRY"):"");
				vObject.setLeBook(ValidationUtil.isValid(rs.getString("LE_BOOK"))?rs.getString("LE_BOOK"):"");
				vObject.setConnectorType(ValidationUtil.isValid(rs.getString("CONNECTOR_TYPE"))?rs.getString("CONNECTOR_TYPE"):"");
				vObject.setConnectorId(ValidationUtil.isValid(rs.getString("CONNECTOR_ID"))?rs.getString("CONNECTOR_ID"):"");
				vObject.setTableIndicator(ValidationUtil.isValid(rs.getString("TABLE_INDICATOR"))?rs.getString("TABLE_INDICATOR"):"");
				
				if("APPR".equalsIgnoreCase(vObject.getTableIndicator())) {
					collTemp = getQueryResults(vObject, 0);
				} else {
					collTemp = getQueryResults(vObject, 1);
				}
				if(collTemp!=null && collTemp.size() == 1){
					vObject = collTemp.get(0);
				}
				
				return vObject;
			}
		};
		return mapper;
	}
	
	protected RowMapper getLocalMapper(EtlConnectorVb dobj) {

		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorVb vObject = new EtlConnectorVb();
				vObject.setWriteFlag(dobj.getWriteFlag());
				vObject.setCountry(ValidationUtil.isValid(rs.getString("COUNTRY"))?rs.getString("COUNTRY"):"");
				vObject.setLeBook(ValidationUtil.isValid(rs.getString("LE_BOOK"))?rs.getString("LE_BOOK"):"");
				vObject.setConnectorType(ValidationUtil.isValid(rs.getString("CONNECTOR_TYPE"))?rs.getString("CONNECTOR_TYPE"):"");
				vObject.setConnectorId(ValidationUtil.isValid(rs.getString("CONNECTOR_ID"))?rs.getString("CONNECTOR_ID"):"");
				vObject.setCountryDesc(ValidationUtil.isValid(rs.getString("COUNTRY_DESC"))?rs.getString("COUNTRY_DESC"):"");
				vObject.setLeBookDesc(ValidationUtil.isValid(rs.getString("LE_BOOK_DESC"))?rs.getString("LE_BOOK_DESC"):"");
				vObject.setConnectorTypeAt(rs.getInt("CONNECTOR_TYPE_AT"));
				vObject.setConnectorTypeDesc(ValidationUtil.isValid(rs.getString("CONNECTOR_TYPE_DESC"))?rs.getString("CONNECTOR_TYPE_DESC"):"");
				vObject.setConnectionName(ValidationUtil.isValid(rs.getString("CONNECTION_NAME"))?rs.getString("CONNECTION_NAME"):"");
				vObject.setConnectorScripts(ValidationUtil.isValid(rs.getString("CONNECTOR_SCRIPTS"))?rs.getString("CONNECTOR_SCRIPTS"):"");
				vObject.setEndpointTypeAt(rs.getInt("ENDPOINT_TYPE_AT"));
				vObject.setEndpointType(ValidationUtil.isValid(rs.getString("ENDPOINT_TYPE"))?rs.getString("ENDPOINT_TYPE"):"");
				vObject.setEndpointTypeDesc(ValidationUtil.isValid(rs.getString("ENDPOINT_TYPE_DESC"))?rs.getString("ENDPOINT_TYPE_DESC"):"");
				vObject.setConnectionStatusNt(rs.getInt("CONNECTION_STAUTS_NT"));
				vObject.setConnectionStatus(rs.getInt("CONNECTION_STAUTS"));
				vObject.setStatusDesc(rs.getString("CONNECTION_STATUS_DESC"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getLong("MAKER"));
				vObject.setVerifier(rs.getLong("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				vObject.setDateCreation(rs.getString("DATE_CREATION"));
				vObject.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				return vObject;
			}
		};
		return mapper;
	}

	String statusApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.CONNECTION_STAUTS",
			"CONNECTION_STATUS_DESC");
	String statusPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.CONNECTION_STAUTS",
			"CONNECTION_STATUS_DESC");

	String recIndApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String recIndPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	String connectionTypeApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2100, "TAppr.CONNECTOR_TYPE",
			"CONNECTOR_TYPE_DESC");
	String connectionTypePendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2100, "TPend.CONNECTOR_TYPE",
			"CONNECTOR_TYPE_DESC");

	String endPointApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2101, "TAppr.ENDPOINT_TYPE",
			"ENDPOINT_TYPE_DESC");
	String endPointPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2101, "TPend.ENDPOINT_TYPE",
			"ENDPOINT_TYPE_DESC");

	String countryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	
	String leBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String leBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	
	/*public EtlConnectorVb getClobData(EtlConnectorVb vObj) {
		
		String sql = "SELECT CONNECTOR_SCRIPTS from "+tableName+" T WHERE T.COUNTRY = ? AND T.LE_BOOK = ? AND T.CONNECTOR_ID = ?";
		Object args[] = {vObj.getCountry(), vObj.getLeBook(), vObj.getConnectorId() };
		return getJdbcTemplate().query(sql, new ResultSetExtractor<EtlConnectorVb>() {

			@Override
			public EtlConnectorVb extractData(ResultSet rs) throws SQLException, DataAccessException {
				return null;
			}
			
		});
		
	}*/
	
	/******* Mapper End **********/
	public List<EtlConnectorVb> getQueryPopupResults(EtlConnectorVb vObj) {
		setServiceDefaults();
		Vector<Object> params = new Vector<Object>();
		String accessQueyr = ", ETL_CONNECTOR_ACCESS FCA, VISION_USERS VU "
				+ " WHERE (TAPPR.Connector_ID =  FCA.Connector_ID  AND FCA.USER_GROUP = VU.USER_GROUP "
				+ " AND FCA.USER_PROFILE = VU.USER_PROFILE  AND VU.VISION_ID = " + intCurrentUserId + " ) ";

		StringBuffer strBufApprove = new StringBuffer("Select FCA.WRITE_FLAG, TAppr.COUNTRY," + countryApprDesc
				+ ",TAppr.LE_BOOK, " + leBookApprDesc + ",TAppr.CONNECTOR_TYPE_AT ,TAppr.CONNECTOR_TYPE, "
				+ connectionTypeApprDesc + ",TAppr.CONNECTOR_ID, TAppr.CONNECTION_NAME, 'APPR' as TABLE_INDICATOR "
//				+ ",TAPPR.CONNECTOR_SCRIPTS " 
				+ ",TAppr.ENDPOINT_TYPE_AT,TAppr.ENDPOINT_TYPE, " + endPointApprDesc
				+ ",TAppr.CONNECTION_STAUTS_NT,TAppr.CONNECTION_STAUTS, " + statusApprDesc
				+ ",TAppr.RECORD_INDICATOR_NT ,TAppr.RECORD_INDICATOR, " + recIndApprDesc + ",TAppr.MAKER ,"
				+ makerApprDesc + ",TAppr.VERIFIER ," + verifierApprDesc
				+ ", TAppr.INTERNAL_STATUS,TAPPR.DATE_LAST_MODIFIED date_modified, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION from ETL_CONNECTOR TAppr "
				+ accessQueyr);
		String strWhereNotExists = new String(
				" Not Exists (SELECT 'X' FROM ETL_CONNECTOR_PEND TPEND WHERE TPEND.COUNTRY = TAPPR.COUNTRY AND TPEND.LE_BOOK = TAPPR.LE_BOOK "
						+ "AND TAPPR.CONNECTOR_TYPE = TPEND.CONNECTOR_TYPE AND TAPPR.CONNECTOR_ID = TPEND.CONNECTOR_ID )");
		StringBuffer strBufPending = new StringBuffer("Select FCA.WRITE_FLAG,TPend.COUNTRY, " + CountryPendDesc
				+ ",TPend.LE_BOOK, " + leBookTPendDesc + ",TPend.CONNECTOR_TYPE_AT,TPend.CONNECTOR_TYPE, "
				+ connectionTypePendDesc + ",TPend.CONNECTOR_ID, TPend.CONNECTION_NAME, 'PEND' as TABLE_INDICATOR "
//				+ ", TPEND.CONNECTOR_SCRIPTS " 
				+ ",TPend.ENDPOINT_TYPE_AT,TPend.ENDPOINT_TYPE, " + endPointPendDesc
				+ ",TPend.CONNECTION_STAUTS_NT,TPend.CONNECTION_STAUTS, " + statusPendDesc
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR, " + recIndPendDesc + ",TPend.MAKER,"
				+ makerPendDesc + ",TPend.VERIFIER," + verifierPendDesc
				+ ",TPend.INTERNAL_STATUS,TPEND.DATE_LAST_MODIFIED date_modified, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION from ETL_CONNECTOR_PEND TPend "
				+ accessQueyr.replaceAll("TAPPR", "TPend"));
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
						List<EtlConnectorVb> countryData = findCountryByCustomFilter(val);
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
						List<EtlConnectorVb> leBookData = findLeBookByCustomFilter(val);
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

					case "connectorType":
						CommonUtils.addToQuery(" upper(TAppr.CONNECTOR_TYPE) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CONNECTOR_TYPE) " + val, strBufPending);
						break;

					case "connectorTypeDesc":
						List<AlphaSubTabVb> alphaSTData = alphaSubTabDao.findAlphaSubTabByCustomFilterAndAlphaTab(val,
								2100);
						String actualData = "";
						if (alphaSTData != null && alphaSTData.size() > 0) {
							for (int k = 0; k < alphaSTData.size(); k++) {
								String alphasubtab = alphaSTData.get(k).getAlphaSubTab();
								if (!ValidationUtil.isValid(actualData)) {
									actualData = "'" + alphasubtab + "'";
								} else {
									actualData = actualData + ",'" + alphasubtab + "'";
								}
							}
							CommonUtils.addToQuery(" (TAPPR.CONNECTOR_TYPE) IN (" + actualData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.CONNECTOR_TYPE) IN (" + actualData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.CONNECTOR_TYPE) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.CONNECTOR_TYPE) IN ('') ", strBufPending);
						}
						break;

					case "connectorId":
						CommonUtils.addToQuery(" upper(TAppr.CONNECTOR_ID) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CONNECTOR_ID) " + val, strBufPending);
						break;

					case "connectionName":
						CommonUtils.addToQuery(" upper(TAppr.CONNECTION_NAME) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CONNECTION_NAME) " + val, strBufPending);
						break;

					case "connectorScripts":
						CommonUtils.addToQuery(" upper(TAppr.CONNECTOR_SCRIPTS) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CONNECTOR_SCRIPTS) " + val, strBufPending);
						break;

					case "endpointType":
						CommonUtils.addToQuery(" upper(TAppr.ENDPOINT_TYPE) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.ENDPOINT_TYPE) " + val, strBufPending);
						break;
					case "connectionStatus":
						CommonUtils.addToQuery(" upper(TAppr.CONNECTION_STAUTS) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.CONNECTION_STAUTS) " + val, strBufPending);
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
							CommonUtils.addToQuery(" (TAPPR.CONNECTION_STAUTS) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.CONNECTION_STAUTS) IN (" + actData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.CONNECTION_STAUTS) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.CONNECTION_STAUTS) IN ('') ", strBufPending);
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

	public List<EtlConnectorVb> getQueryResults(EtlConnectorVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlConnectorVb> collTemp = null;
		final int intKeyFieldsCount = 4;
		StringBuffer strQueryAppr = new StringBuffer("Select TAppr.COUNTRY," + countryApprDesc + ",TAppr.LE_BOOK,"
				+ leBookApprDesc + ",TAppr.CONNECTOR_TYPE_AT ,TAppr.CONNECTOR_TYPE, " + connectionTypeApprDesc
				+ ",TAppr.CONNECTOR_ID,TAppr.CONNECTION_NAME ,TAppr.CONNECTOR_SCRIPTS"
				+ ",TAppr.ENDPOINT_TYPE_AT,TAppr.ENDPOINT_TYPE, " + endPointApprDesc
				+ ",TAppr.CONNECTION_STAUTS_NT,TAppr.CONNECTION_STAUTS, " + statusApprDesc
				+ ",TAppr.RECORD_INDICATOR_NT ,TAppr.RECORD_INDICATOR, " + recIndApprDesc + ",TAppr.MAKER,"
				+ makerApprDesc + ",TAppr.VERIFIER ," + verifierApprDesc
				+ ",TAppr.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TAppr.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null)
				+ " "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TAppr.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null)
				+ " "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION FROM ETL_CONNECTOR TAppr "
				+ " WHERE UPPER(TAppr.COUNTRY) = ?  AND UPPER(TAppr.LE_BOOK) = ? AND UPPER(TAppr.CONNECTOR_TYPE) = ? AND UPPER(TAppr.CONNECTOR_ID) = ? ");

		StringBuffer strQueryPend = new StringBuffer("Select TPend.COUNTRY, " + CountryPendDesc + ",TPend.LE_BOOK, "
				+ leBookTPendDesc + ",TPend.CONNECTOR_TYPE_AT,TPend.CONNECTOR_TYPE, " + connectionTypePendDesc
				+ ",TPend.CONNECTOR_ID,TPend.CONNECTION_NAME,TPend.CONNECTOR_SCRIPTS"
				+ ",TPend.ENDPOINT_TYPE_AT,TPend.ENDPOINT_TYPE, " + endPointPendDesc
				+ ",TPend.CONNECTION_STAUTS_NT,TPend.CONNECTION_STAUTS, " + statusPendDesc
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR, " + recIndPendDesc + ",TPend.MAKER,"
				+ makerPendDesc + ",TPend.VERIFIER," + verifierPendDesc
				+ ",TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TPend.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null)
				+ " "+getDbFunction(Constants.TIME, null)+"') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TPend.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null)
				+ " "+getDbFunction(Constants.TIME, null)+"') DATE_CREATION  FROM ETL_CONNECTOR_PEND TPend WHERE "
				+ " UPPER(TPend.COUNTRY) = ?  AND UPPER(TPend.LE_BOOK) = ? AND UPPER(TPend.CONNECTOR_TYPE) = ? AND UPPER(TPend.CONNECTOR_ID) = ?");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry().toUpperCase();
		objParams[1] = dObj.getLeBook().toUpperCase();
		objParams[2] = dObj.getConnectorType().toUpperCase();
		objParams[3] = dObj.getConnectorId().toUpperCase();

		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 0) {
				collTemp = getJdbcTemplate().query(strQueryAppr.toString(), objParams, getLocalMapper(dObj));
			} else {
				collTemp = getJdbcTemplate().query(strQueryPend.toString(), objParams, getLocalMapper(dObj));
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
	protected List<EtlConnectorVb> selectApprovedRecord(EtlConnectorVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlConnectorVb> doSelectPendingRecord(EtlConnectorVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected int getStatus(EtlConnectorVb records) {
		return records.getConnectionStatus();
	}

	@Override
	protected void setStatus(EtlConnectorVb vObject, int status) {
		vObject.setConnectionStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlConnectorVb vObject) {
		if(ValidationUtil.isValid(vObject.getConnectorScripts())) {
			String dbLinkName = CommonUtils.getValueWithoutEncrypt(vObject.getConnectorScripts(), "DB_LINK_NAME");
			if(ValidationUtil.isValid(dbLinkName)) {
				vObject.setDbLinkFlag("Y");
			}
		}
		String query = "Insert Into ETL_CONNECTOR (DB_LINK_FLAG,COUNTRY, LE_BOOK"
				+ ", CONNECTOR_TYPE_AT, CONNECTOR_TYPE, CONNECTOR_ID, CONNECTION_NAME"
				+ ", ENDPOINT_TYPE_AT, ENDPOINT_TYPE, CONNECTION_STAUTS_NT, CONNECTION_STAUTS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, CONNECTOR_SCRIPTS)"
				+ "Values (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?)";
		Object[] args = { vObject.getDbLinkFlag(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getConnectorTypeAt(), vObject.getConnectorType(), vObject.getConnectorId(),
				vObject.getConnectionName(), vObject.getEndpointTypeAt(), vObject.getEndpointType(),
				vObject.getConnectionStatusNt(), vObject.getConnectionStatus(), vObject.getRecordIndicatorNt(),
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
					String clobData = ValidationUtil.isValid(vObject.getConnectorScripts())	? vObject.getConnectorScripts()	: "";
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
	protected int doInsertionPend(EtlConnectorVb vObject) {
		if(ValidationUtil.isValid(vObject.getConnectorScripts())) {
			String dbLinkName = CommonUtils.getValueWithoutEncrypt(vObject.getConnectorScripts(), "DB_LINK_NAME");
			if(ValidationUtil.isValid(dbLinkName)) {
				vObject.setDbLinkFlag("Y");
			}
		}
		String query = "Insert Into ETL_CONNECTOR_PEND (DB_LINK_FLAG,COUNTRY, LE_BOOK"
				+ ", CONNECTOR_TYPE_AT, CONNECTOR_TYPE, CONNECTOR_ID, CONNECTION_NAME"
				+ ", ENDPOINT_TYPE_AT, ENDPOINT_TYPE, CONNECTION_STAUTS_NT, CONNECTION_STAUTS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, CONNECTOR_SCRIPTS)"
				+ "Values (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ", ?)";
		Object[] args = { vObject.getDbLinkFlag(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getConnectorTypeAt(), vObject.getConnectorType(), vObject.getConnectorId(),
				vObject.getConnectionName(), vObject.getEndpointTypeAt(), vObject.getEndpointType(),
				vObject.getConnectionStatusNt(), vObject.getConnectionStatus(), vObject.getRecordIndicatorNt(),
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
					String clobData = ValidationUtil.isValid(vObject.getConnectorScripts())	? vObject.getConnectorScripts()	: "";
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
	protected int doInsertionPendWithDc(EtlConnectorVb vObject) {
		if(ValidationUtil.isValid(vObject.getConnectorScripts())) {
			String dbLinkName = CommonUtils.getValueWithoutEncrypt(vObject.getConnectorScripts(), "DB_LINK_NAME");
			if(ValidationUtil.isValid(dbLinkName)) {
				vObject.setDbLinkFlag("Y");
			}
		}
		String query = "Insert Into ETL_CONNECTOR_PEND (DB_LINK_FLAG,COUNTRY, LE_BOOK"
				+ ", CONNECTOR_TYPE_AT, CONNECTOR_TYPE, CONNECTOR_ID, CONNECTION_NAME"
				+ ", ENDPOINT_TYPE_AT, ENDPOINT_TYPE, CONNECTION_STAUTS_NT, CONNECTION_STAUTS, "
				+ "RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, CONNECTOR_SCRIPTS)"
				+ "Values (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + ", ?)";
		Object[] args = { vObject.getDbLinkFlag(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getConnectorTypeAt(), vObject.getConnectorType(), vObject.getConnectorId(),
				vObject.getConnectionName(), vObject.getEndpointTypeAt(), vObject.getEndpointType(),
				vObject.getConnectionStatusNt(), vObject.getConnectionStatus(), vObject.getRecordIndicatorNt(),
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
					String clobData = ValidationUtil.isValid(vObject.getConnectorScripts())	? vObject.getConnectorScripts()	: "";
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
	protected int doUpdateAppr(EtlConnectorVb vObject) {
		if(ValidationUtil.isValid(vObject.getConnectorScripts())) {
			String dbLinkName = CommonUtils.getValueWithoutEncrypt(vObject.getConnectorScripts(), "DB_LINK_NAME");
			if(ValidationUtil.isValid(dbLinkName)) {
				vObject.setDbLinkFlag("Y");
			}
		}
		String query = "Update ETL_CONNECTOR Set  CONNECTOR_SCRIPTS = ?,DB_LINK_FLAG =?, CONNECTION_NAME = ?, ENDPOINT_TYPE_AT = ?,ENDPOINT_TYPE = ?,CONNECTION_STAUTS_NT = ?,CONNECTION_STAUTS = ?,"
				+ "RECORD_INDICATOR_NT = ?,RECORD_INDICATOR = ?,MAKER = ?,VERIFIER = ?,INTERNAL_STATUS = ?,DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "WHERE COUNTRY = ? AND LE_BOOK = ? AND CONNECTOR_TYPE = ? AND CONNECTOR_ID = ? ";
		Object[] args = { vObject.getDbLinkFlag(), vObject.getConnectionName(), vObject.getEndpointTypeAt(),
				vObject.getEndpointType(), vObject.getConnectionStatusNt(), vObject.getConnectionStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorType(),
				vObject.getConnectorId() };
		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getConnectorScripts())	? vObject.getConnectorScripts()	: "";
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
	protected int doUpdatePend(EtlConnectorVb vObject) {
		if(ValidationUtil.isValid(vObject.getConnectorScripts())) {
			String dbLinkName = CommonUtils.getValueWithoutEncrypt(vObject.getConnectorScripts(), "DB_LINK_NAME");
			if(ValidationUtil.isValid(dbLinkName)) {
				vObject.setDbLinkFlag("Y");
			}
		}
		String query = "Update ETL_CONNECTOR_PEND Set CONNECTOR_SCRIPTS = ?,DB_LINK_FLAG =?, CONNECTION_NAME = ?, ENDPOINT_TYPE_AT = ?,ENDPOINT_TYPE = ?,CONNECTION_STAUTS_NT = ?,CONNECTION_STAUTS = ?,"
				+ "RECORD_INDICATOR_NT = ?,RECORD_INDICATOR = ?,MAKER = ?,VERIFIER = ?,INTERNAL_STATUS = ?,DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + " "
				+ "WHERE COUNTRY = ? AND LE_BOOK = ? AND CONNECTOR_TYPE = ? AND CONNECTOR_ID = ?";
		Object[] args = { vObject.getDbLinkFlag(), vObject.getConnectionName(), vObject.getEndpointTypeAt(),
				vObject.getEndpointType(), vObject.getConnectionStatusNt(), vObject.getConnectionStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorType(),
				vObject.getConnectorId() };
		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getConnectorScripts())	? vObject.getConnectorScripts()	: "";
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
	protected int doDeleteAppr(EtlConnectorVb vObject) {
		String query = "Delete From ETL_CONNECTOR Where COUNTRY = ? AND LE_BOOK = ? AND CONNECTOR_TYPE = ? AND CONNECTOR_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorType(),
				vObject.getConnectorId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlConnectorVb vObject) {
		String query = "Delete From ETL_CONNECTOR_PEND Where COUNTRY = ? AND LE_BOOK = ? AND CONNECTOR_TYPE = ? AND CONNECTOR_ID = ? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorType(),
				vObject.getConnectorId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlConnectorVb vObject) {
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

		strAudit.append("CONNECTOR_TYPE_AT" + auditDelimiterColVal + vObject.getConnectorTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getConnectorType()))
			strAudit.append("CONNECTOR_TYPE" + auditDelimiterColVal + vObject.getConnectorType().trim());
		else
			strAudit.append("CONNECTOR_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getConnectorId()))
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + vObject.getConnectorId().trim());
		else
			strAudit.append("CONNECTOR_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getConnectionName()))
			strAudit.append("CONNECTION_NAME" + auditDelimiterColVal + vObject.getConnectionName().trim());
		else
			strAudit.append("CONNECTION_NAME" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getConnectorScripts()))
			strAudit.append("CONNECTOR_SCRIPTS" + auditDelimiterColVal + vObject.getConnectorScripts().trim());
		else
			strAudit.append("CONNECTOR_SCRIPTS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getEndpointTypeAt()))
			strAudit.append("ENDPOINT_TYPE_AT" + auditDelimiterColVal + vObject.getEndpointTypeAt());
		else
			strAudit.append("ENDPOINT_TYPE_AT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getEndpointType()))
			strAudit.append("ENDPOINT_TYPE" + auditDelimiterColVal + vObject.getEndpointType().trim());
		else
			strAudit.append("ENDPOINT_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getConnectionStatusNt()))
			strAudit.append("CONNECTION_STAUTS_NT" + auditDelimiterColVal + vObject.getConnectionStatusNt());
		else
			strAudit.append("CONNECTION_STAUTS_NT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getConnectionStatus()))
			strAudit.append("CONNECTION_STAUTS" + auditDelimiterColVal + vObject.getConnectionStatus());
		else
			strAudit.append("CONNECTION_STAUTS" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getRecordIndicatorNt()))
			strAudit.append("RECORD_INDICATOR_NT" + auditDelimiterColVal + vObject.getRecordIndicatorNt());
		else
			strAudit.append("RECORD_INDICATOR_NT" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getRecordIndicator()))
			strAudit.append("RECORD_INDICATOR" + auditDelimiterColVal + vObject.getRecordIndicator());
		else
			strAudit.append("RECORD_INDICATOR" + auditDelimiterColVal + "NULL");
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
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + vObject.getDateLastModified());
		else
			strAudit.append("DATE_LAST_MODIFIED" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getDateCreation()))
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + vObject.getDateCreation());
		else
			strAudit.append("DATE_CREATION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);
		
		if (ValidationUtil.isValid(vObject.getDbLinkFlag()))
			strAudit.append("DB_LINK_FLAG" + auditDelimiterColVal + vObject.getDbLinkFlag());
		else
			strAudit.append("DB_LINK_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		return strAudit.toString();
	}

	@Override
	public void setServiceDefaults() {
		serviceName = "EtlConnector";
		serviceDesc = "ETL Connector";
		tableName = "ETL_CONNECTOR";
		childTableName = "ETL_CONNECTOR";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

	public List<EtlConnectorVb> getDisplayTagList(String macroVarType) throws DataAccessException {
		String macrovarName;
		if (macroVarType.equalsIgnoreCase("VPNDIRECT") || macroVarType.equalsIgnoreCase("VPN")) {
			macrovarName = "CONNECTIVITY_DETAILS";
		} else {
			macrovarName = "DB_DETAILS";
		}
		String sql = "select TAG_NAME, DISPLAY_NAME , MASKED_FLAG ,ENCRYPTION, MANDATORY_FLAG, TAG_TYPE from"
				+ "  MACROVAR_TAGGING where MACROVAR_NAME=? AND MACROVAR_TYPE=? ORDER BY TAG_NO";
		
		Object [] args= {macrovarName,macroVarType};
		
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorVb vObject = new EtlConnectorVb();
				vObject.setTagName(rs.getString("TAG_NAME"));
				vObject.setDisplayName(rs.getString("DISPLAY_NAME"));
				vObject.setMaskedFlag(rs.getString("MASKED_FLAG"));
				vObject.setEncryption(rs.getString("ENCRYPTION"));
				vObject.setMandatoryFlag(rs.getString("MANDATORY_FLAG"));
				vObject.setTagType(rs.getInt("TAG_TYPE"));
				return vObject;
			}
		};
		return getJdbcTemplate().query(sql,args, mapper);
	}

	public String getConnectorScripts(EtlConnectorVb vObject, int intStatus) {
		String etlConnectorScript = null;
		Object[] objParams = new Object[4];
		String sqlApprQuery = " SELECT TAPPR.CONNECTOR_SCRIPTS FROM ETL_CONNECTOR TAPPR  WHERE UPPER(TAPPR.COUNTRY) = ? AND UPPER(TAPPR.LE_BOOK) = ? "
				+ " AND  UPPER(TAPPR.CONNECTOR_TYPE) = ? AND UPPER(TAPPR.CONNECTOR_ID) = ? ";
		String sqlPendQuery = " SELECT TPEND.CONNECTOR_SCRIPTS FROM ETL_CONNECTOR_PEND TPEND  WHERE UPPER(TPEND.COUNTRY) = ? "
				+ " AND UPPER(TPEND.LE_BOOK) = ? AND  UPPER(TPEND.CONNECTOR_TYPE) = ? AND UPPER(TPEND.CONNECTOR_ID) = ? ";
		objParams[0] = vObject.getCountry().toUpperCase();
		objParams[1] = vObject.getLeBook().toUpperCase();
		objParams[2] = vObject.getConnectorType().toUpperCase();
		objParams[3] = vObject.getConnectorId().toUpperCase();
		try {
			if (intStatus == 0) {
				etlConnectorScript = getJdbcTemplate().queryForObject(sqlApprQuery, objParams, String.class);
			} else {
				etlConnectorScript = getJdbcTemplate().queryForObject(sqlPendQuery, objParams, String.class);
			}
			return etlConnectorScript;
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	public ExceptionCode doRejectRecord(EtlConnectorVb vObject) throws RuntimeCustomException {
		EtlConnectorVb vObjectlocal = null;
		List<EtlConnectorVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		setServiceDefaults();
		strErrorDesc = "";
		strCurrentOperation = Constants.REJECT;
		vObject.setMaker(getIntCurrentUserId());
		List<EtlMqTableVb> listmanualQuery = null;
		try {
			if (vObject.getRecordIndicator() == 1 || vObject.getRecordIndicator() == 3)
				vObject.setRecordIndicator(0);
			else
				vObject.setRecordIndicator(-1);
			collTemp = doSelectPendingRecord(vObject);
			if (collTemp == null) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (collTemp.size() == 0) {
				exceptionCode = getResultObject(Constants.NO_SUCH_PENDING_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
			vObjectlocal = ((ArrayList<EtlConnectorVb>) collTemp).get(0);
			if (deletePendingRecord(vObjectlocal) == 0) {
				exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
				throw buildRuntimeCustomException(exceptionCode);
			}
			if (vObject.getConnectorType().equalsIgnoreCase("S")
					|| vObjectlocal.getConnectorType().equalsIgnoreCase("S")) {
				if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) {
					EtlMqTableVb etlMqTableVb = new EtlMqTableVb();
					etlMqTableVb.setConnectorId(vObject.getConnectorId());
					listmanualQuery = etlMqTableDao.getQueryResultsByParent(etlMqTableVb.getConnectorId(), 0);
					if (listmanualQuery != null && listmanualQuery.size() > 0) {
						for (EtlMqTableVb tableVb : listmanualQuery) {
							tableVb.setStaticDelete(vObject.isStaticDelete());
							tableVb.setVerificationRequired(vObject.isVerificationRequired());
							exceptionCode = etlMqTableDao.doDeleteApprRecordForNonTrans(tableVb);
							if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
								exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode);
							}
						}
					}
					listmanualQuery = etlMqTableDao.getQueryResultsByParent(etlMqTableVb.getConnectorId(), 1);
					if (listmanualQuery != null && listmanualQuery.size() > 0) {
						for (EtlMqTableVb tableVb : listmanualQuery) {
							tableVb.setStaticDelete(vObject.isStaticDelete());
							tableVb.setVerificationRequired(vObject.isVerificationRequired());
							exceptionCode = etlMqTableDao.doRejectRecord(tableVb);
							if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
								exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCode);
							}
						}
					}
				}
			} else if (vObject.getConnectorType().equalsIgnoreCase("SS")
					|| vObjectlocal.getConnectorType().equalsIgnoreCase("SS")) {
				List<EtlFileUploadAreaVb> semiStructuredPendList = etlFileUploadAreaDao
						.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
				if (semiStructuredPendList != null && semiStructuredPendList.size() > 0) {
					for (EtlFileUploadAreaVb etlFileUploadAreaVb : semiStructuredPendList) {
						exceptionCode = etlFileUploadAreaDao.doRejectRecord(etlFileUploadAreaVb);
						if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
				/*List<EtlFileUploadAreaVb> semiStructuredMainList = etlFileUploadAreaDao
						.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_ZERO);
				if (semiStructuredMainList != null && semiStructuredMainList.size() > 0) {
					for (EtlFileUploadAreaVb etlFileUploadAreaVb : semiStructuredMainList) {
						etlFileUploadAreaVb.setStaticDelete(false);
						exceptionCode = etlFileUploadAreaDao.doDeleteApprRecordForNonTrans(etlFileUploadAreaVb);
						if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(retVal);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}*/
			}
			// dELETE THE RECORD FROM ACCESS TABLE
			if (vObjectlocal.getRecordIndicator() == Constants.STATUS_INSERT) {
				VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
				EtlConnectorAccessVb etlConnectorAccessVb = new EtlConnectorAccessVb();
				etlConnectorAccessVb.setConnectorId(vObject.getConnectorId());
				etlConnectorAccessVb.setUserGroup(visionUSersVb.getUserGroup());
				etlConnectorAccessVb.setUserProfile(visionUSersVb.getUserProfile());
				etlConnectorAccessVb.setWriteFlag("Y");
				etlConnectorAccessVb.setVerificationRequired(false);
				if (etlConnectorAccessDao.doDeleteAppr(etlConnectorAccessVb) == 0) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
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
	public ExceptionCode doApproveRecord(EtlConnectorVb vObject, boolean staticDelete) throws RuntimeCustomException {
		EtlConnectorVb oldContents = null;
		EtlConnectorVb vObjectlocal = null;
		List<EtlConnectorVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strCurrentOperation = Constants.APPROVE;
		setServiceDefaults();
		List<EtlMqTableVb> listmanualQuery = null;
		String systemDate = getSystemDate();
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

			vObjectlocal = ((ArrayList<EtlConnectorVb>) collTemp).get(0);

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
				oldContents = ((ArrayList<EtlConnectorVb>) collTemp).get(0);
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

				EtlFileUploadAreaVb etlFileUploadAreaVb= new EtlFileUploadAreaVb();
				etlFileUploadAreaVb.setConnectorId(vObjectlocal.getConnectorId());
				List<EtlFileUploadAreaVb> collTempUpl = etlFileUploadAreaDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
				if(collTempUpl != null && !collTempUpl.isEmpty()) {
					collTempUpl.forEach(uplAreaVb-> {
						ExceptionCode exceptionCodeUp=etlFileUploadAreaDao.doApproveRecord(uplAreaVb, staticDelete);
						if (exceptionCodeUp.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}
				EtlFileTableVb childObj = new EtlFileTableVb();
				childObj.setConnectorId(vObject.getConnectorId());
				List<EtlFileTableVb> pendTableList = etlFileTableDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
				if(pendTableList != null && !pendTableList.isEmpty()) {
					pendTableList.forEach(fileVb-> {
						ExceptionCode exceptionCodeUp=etlFileTableDao.doApproveRecord(fileVb, staticDelete);
						if (exceptionCodeUp.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}
				EtlFileColumnsVb childColObj = new EtlFileColumnsVb();
				childColObj.setConnectorId(vObject.getConnectorId());
				List<EtlFileColumnsVb> pendColumnsList = etlFileColumnsDao.getQueryResultsByParentAppr(vObject.getConnectorId(), Constants.STATUS_PENDING);
				if(pendColumnsList != null && !pendColumnsList.isEmpty()) {
					pendColumnsList.forEach(fileColVb-> {
						ExceptionCode exceptionCodeUp=etlFileColumnsDao.doApproveRecord(fileColVb, staticDelete);
						if (exceptionCodeUp.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
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
				if (vObject.getConnectorType().equalsIgnoreCase("SS")
						|| vObjectlocal.getConnectorType().equalsIgnoreCase("SS")) {
				EtlFileUploadAreaVb etlFileUploadAreaVb= new EtlFileUploadAreaVb();
				etlFileUploadAreaVb.setConnectorId(vObjectlocal.getConnectorId());
				List<EtlFileUploadAreaVb> collTempUpl = etlFileUploadAreaDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
				etlFileUploadAreaDao.doDeleteApprAll(vObjectlocal.getConnectorId());
				if(collTempUpl != null && !collTempUpl.isEmpty()) {
					collTempUpl.forEach(uplAreaVb-> {
						uplAreaVb.setDateLastModified(systemDate);
						uplAreaVb.setVerifier(getIntCurrentUserId());
						uplAreaVb.setRecordIndicator(Constants.STATUS_ZERO);
						retVal=etlFileUploadAreaDao.doInsertionAppr(uplAreaVb);
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}
				EtlFileTableVb childObj = new EtlFileTableVb();
				childObj.setConnectorId(vObject.getConnectorId());
				List<EtlFileTableVb> pendTableList = etlFileTableDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
				etlFileTableDao.doDeleteApprAll(vObjectlocal.getConnectorId());
				if(pendTableList != null && !pendTableList.isEmpty()) {
					pendTableList.forEach(fileVb-> {
						fileVb.setDateLastModified(systemDate);
						fileVb.setVerifier(getIntCurrentUserId());
						fileVb.setRecordIndicator(Constants.STATUS_ZERO);
						retVal=etlFileTableDao.doInsertionAppr(fileVb);
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}
				EtlFileColumnsVb childColObj = new EtlFileColumnsVb();
				childColObj.setConnectorId(vObject.getConnectorId());
				List<EtlFileColumnsVb> pendColumnsList = etlFileColumnsDao.getQueryResultsByParentAppr(vObject.getConnectorId(), Constants.STATUS_PENDING);
				etlFileColumnsDao.doDeleteApprAll(vObjectlocal.getConnectorId());
				if(pendColumnsList != null && !pendColumnsList.isEmpty()) {
					pendColumnsList.forEach(fileColVb-> {
						fileColVb.setDateLastModified(systemDate);
						fileColVb.setVerifier(getIntCurrentUserId());
						fileColVb.setRecordIndicator(Constants.STATUS_ZERO);
						retVal=etlFileColumnsDao.doInsertionAppr(fileColVb);
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCodeUp);
						}
					});
				}
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
					setStatus(vObject, Constants.PASSIVATE);
					vObject.setDateLastModified(systemDate);
					
					if (vObject.getConnectorType().equalsIgnoreCase("SS")
							|| vObjectlocal.getConnectorType().equalsIgnoreCase("SS")) {
					EtlFileUploadAreaVb etlFileUploadAreaVb= new EtlFileUploadAreaVb();
					etlFileUploadAreaVb.setConnectorId(vObjectlocal.getConnectorId());
					List<EtlFileUploadAreaVb> collTempUpl = etlFileUploadAreaDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
					etlFileUploadAreaDao.doDeleteApprAll(vObjectlocal.getConnectorId());
					if(collTempUpl != null && !collTempUpl.isEmpty()) {
						collTempUpl.forEach(uplAreaVb-> {
							uplAreaVb.setDateLastModified(systemDate);
							uplAreaVb.setVerifier(getIntCurrentUserId());
							uplAreaVb.setRecordIndicator(Constants.STATUS_ZERO);
							uplAreaVb.setFileStatus(Constants.PASSIVATE);
							retVal=etlFileUploadAreaDao.doInsertionAppr(uplAreaVb);
							if (retVal != Constants.SUCCESSFUL_OPERATION) {
								ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCodeUp);
							}
						});
					}
					EtlFileTableVb childObj = new EtlFileTableVb();
					childObj.setConnectorId(vObject.getConnectorId());
					List<EtlFileTableVb> pendTableList = etlFileTableDao.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
					etlFileTableDao.doDeleteApprAll(vObjectlocal.getConnectorId());
					if(pendTableList != null && !pendTableList.isEmpty()) {
						pendTableList.forEach(fileVb-> {
							fileVb.setDateLastModified(systemDate);
							fileVb.setVerifier(getIntCurrentUserId());
							fileVb.setRecordIndicator(Constants.STATUS_ZERO);
							fileVb.setFileStatus(Constants.PASSIVATE);
							retVal=etlFileTableDao.doInsertionAppr(fileVb);
							if (retVal != Constants.SUCCESSFUL_OPERATION) {
								ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCodeUp);
							}
						});
					}
					EtlFileColumnsVb childColObj = new EtlFileColumnsVb();
					childColObj.setConnectorId(vObject.getConnectorId());
					List<EtlFileColumnsVb> pendColumnsList = etlFileColumnsDao.getQueryResultsByParentAppr(vObject.getConnectorId(), Constants.STATUS_PENDING);
					etlFileColumnsDao.doDeleteApprAll(vObjectlocal.getConnectorId());
					if(pendColumnsList != null && !pendColumnsList.isEmpty()) {
						pendColumnsList.forEach(fileColVb-> {
							fileColVb.setDateLastModified(systemDate);
							fileColVb.setVerifier(getIntCurrentUserId());
							fileColVb.setRecordIndicator(Constants.STATUS_ZERO);
							fileColVb.setFileColumnStatus(Constants.PASSIVATE);
							retVal=etlFileColumnsDao.doInsertionAppr(fileColVb);
							if (retVal != Constants.SUCCESSFUL_OPERATION) {
								ExceptionCode exceptionCodeUp = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
								throw buildRuntimeCustomException(exceptionCodeUp);
							}
						});
					}
					}
					
				} else {
					// Delete the existing record from the Approved table
					retVal = doDeleteAppr(vObjectlocal);
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
					vObject.setDateLastModified(systemDate);
					if (vObject.getConnectorType().equalsIgnoreCase("S")
							|| vObjectlocal.getConnectorType().equalsIgnoreCase("S")) {
						// DELETE THE RECORDS FROM MQ TABLE AND COLUMNS
						listmanualQuery = etlMqTableDao.getQueryResultsByParent(vObject.getConnectorId(), 0);
						if (listmanualQuery != null && listmanualQuery.size() > 0) {
							for (EtlMqTableVb tableVb : listmanualQuery) {
								tableVb.setStaticDelete(false);
								exceptionCode = etlMqTableDao.doDeleteApprRecordForNonTrans(tableVb);
								if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
									exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
									throw buildRuntimeCustomException(exceptionCode);
								}
							}
						}
						listmanualQuery = etlMqTableDao.getQueryResultsByParent(vObject.getConnectorId(), 1);
						if (listmanualQuery != null && listmanualQuery.size() > 0) {
							for (EtlMqTableVb tableVb : listmanualQuery) {
								exceptionCode = etlMqTableDao.doRejectRecord(tableVb);
								if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
									exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
									throw buildRuntimeCustomException(exceptionCode);
								}
							}
						}
					} else if (vObject.getConnectorType().equalsIgnoreCase("SS")
							|| vObjectlocal.getConnectorType().equalsIgnoreCase("SS")) {
						List<EtlFileUploadAreaVb> semiStructuredPendList = etlFileUploadAreaDao
								.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
						if (semiStructuredPendList != null && semiStructuredPendList.size() > 0) {
							for (EtlFileUploadAreaVb etlFileUploadAreaVb : semiStructuredPendList) {
								exceptionCode = etlFileUploadAreaDao.doRejectRecord(etlFileUploadAreaVb);
								if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
									exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
									throw buildRuntimeCustomException(exceptionCode);
								}
							}
						}
						List<EtlFileUploadAreaVb> semiStructuredMainList = etlFileUploadAreaDao
								.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_ZERO);
						if (semiStructuredMainList != null && semiStructuredMainList.size() > 0) {
							for (EtlFileUploadAreaVb etlFileUploadAreaVb : semiStructuredMainList) {
								etlFileUploadAreaVb.setStaticDelete(false);
								exceptionCode = etlFileUploadAreaDao.doDeleteApprRecordForNonTrans(etlFileUploadAreaVb);
								if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
									exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
									throw buildRuntimeCustomException(exceptionCode);
								}
							}
						}
					}
					// DELETE THE RECORD FROM ACCESS TABLE
					VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
					EtlConnectorAccessVb etlConnectorAccessVb = new EtlConnectorAccessVb();
					etlConnectorAccessVb.setConnectorId(vObject.getConnectorId());
					etlConnectorAccessVb.setUserGroup(visionUSersVb.getUserGroup());
					etlConnectorAccessVb.setUserProfile(visionUSersVb.getUserProfile());
					etlConnectorAccessVb.setWriteFlag("Y");
					etlConnectorAccessVb.setVerificationRequired(false);
					etlConnectorAccessVb.setStaticDelete(vObject.isStaticDelete());
					etlConnectorAccessDao.doDeleteApprRecordForNonTrans(etlConnectorAccessVb);
				}
				// Set the current operation to write to audit log
				strApproveOperation = Constants.DELETE;
			} else {
				exceptionCode = getResultObject(Constants.INVALID_STATUS_FLAG_IN_DATABASE);
				throw buildRuntimeCustomException(exceptionCode);
			}

			// Delete the record from the Pending table
			retVal = deletePendingRecord(vObjectlocal);
			if (vObject.getConnectorType().equalsIgnoreCase("SS")
					|| vObjectlocal.getConnectorType().equalsIgnoreCase("SS")) {
				etlFileUploadAreaDao.deletePendingRecordAll(vObjectlocal.getConnectorId());
				etlFileTableDao.deletePendingRecordAll(vObjectlocal.getConnectorId());
				etlFileColumnsDao.deletePendingRecordAll(vObjectlocal.getConnectorId());
				
			}

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
	@Override
	protected ExceptionCode doInsertApprRecordForNonTrans(EtlConnectorVb vObject) throws RuntimeCustomException {
		List<EtlConnectorVb> collTemp = null;
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
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlConnectorVb>) collTemp).get(0));
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
		if(vObject.getConnectorType().equalsIgnoreCase("SS")){
			if (vObject.getChildren() != null && vObject.getChildren().size() > 0) {
				List<EtlFileUploadAreaVb> etlFileUploadAreaVbs = dynamicScriptCreation(vObject.getChildren());
				for (EtlFileUploadAreaVb lObject : etlFileUploadAreaVbs) {
					lObject.setConnectorId(vObject.getConnectorId());
					vObject.setVerifier(getIntCurrentUserId());
					vObject.setRecordIndicator(Constants.STATUS_ZERO);
					lObject.setFileId(lObject.getFileId());
					exceptionCode = etlFileUploadAreaDao.doInsertApprRecordForNonTrans(lObject);
					retVal = exceptionCode.getErrorCode();
					if (retVal != Constants.SUCCESSFUL_OPERATION) {
						exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
						throw buildRuntimeCustomException(exceptionCode);
					}
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
		EtlConnectorAccessVb etlConnectorAccessVb = new EtlConnectorAccessVb();
		etlConnectorAccessVb.setconnectorId(vObject.getConnectorId());
		etlConnectorAccessVb.setUserGroup(visionUSersVb.getUserGroup());
		etlConnectorAccessVb.setUserProfile(visionUSersVb.getUserProfile());
		etlConnectorAccessVb.setWriteFlag("Y");
		etlConnectorAccessVb.setVerificationRequired(false);
		etlConnectorAccessVb.setDataConnectorStatus(0);
		exceptionCode = etlConnectorAccessDao.doInsertApprRecordForNonTrans(etlConnectorAccessVb);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		return exceptionCode;
	}

	@Override
	protected ExceptionCode doInsertRecordForNonTrans(EtlConnectorVb vObject) throws RuntimeCustomException {
		List<EtlConnectorVb> collTemp = null;
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
			int staticDeletionFlag = getStatus(((ArrayList<EtlConnectorVb>) collTemp).get(0));
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
			EtlConnectorVb vObjectLocal = ((ArrayList<EtlConnectorVb>) collTemp).get(0);
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
			if(vObject.getConnectorType().equalsIgnoreCase("SS")){
				if (vObject.getChildren() != null && vObject.getChildren().size() > 0) {
					List<EtlFileUploadAreaVb> etlFileUploadAreaVbs = dynamicScriptCreation(vObject.getChildren());
					for (EtlFileUploadAreaVb lObject : etlFileUploadAreaVbs) {
						lObject.setConnectorId(vObject.getConnectorId());
						lObject.setFileId(lObject.getFileId());
						lObject.setVerifier(vObject.getVerifier());
						lObject.setDateCreation(vObject.getDateCreation());
						lObject.setDateLastModified(vObject.getDateLastModified());
						lObject.setRecordIndicator(vObject.getRecordIndicator());
						exceptionCode = etlFileUploadAreaDao.doInsertRecordForNonTrans(lObject);
						retVal = exceptionCode.getErrorCode();
						if (retVal != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
			}
		}

		VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
		EtlConnectorAccessVb etlConnectorAccessVb = new EtlConnectorAccessVb();
		etlConnectorAccessVb.setconnectorId(vObject.getConnectorId());
		etlConnectorAccessVb.setUserGroup(visionUSersVb.getUserGroup());
		etlConnectorAccessVb.setUserProfile(visionUSersVb.getUserProfile());
		etlConnectorAccessVb.setWriteFlag("Y");
		etlConnectorAccessVb.setVerificationRequired(false);
		etlConnectorAccessVb.setDataConnectorStatus(0);
		exceptionCode = etlConnectorAccessDao.doInsertApprRecordForNonTrans(etlConnectorAccessVb);
		if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(retVal);
		}
		return exceptionCode;
	}

	@Transactional(rollbackForClassName = { "com.vision.exception.RuntimeCustomException" })
	protected ExceptionCode doDeleteApprRecordForNonTrans(EtlConnectorVb vObject) throws RuntimeCustomException {
		List<EtlConnectorVb> collTemp = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation = Constants.DELETE;
		strErrorDesc = "";
		strCurrentOperation = Constants.DELETE;
		setServiceDefaults();
		EtlConnectorVb vObjectlocal = null;
		List<EtlMqTableVb> listmanualQuery = null;

		vObject.setMaker(getIntCurrentUserId());
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null) {
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		// If record already exists in the approved table, reject the addition
		if (collTemp.size() > 0) {
			int intStaticDeletionFlag = getStatus(((ArrayList<EtlConnectorVb>) collTemp).get(0));
			if (intStaticDeletionFlag == Constants.PASSIVATE) {
				exceptionCode = getResultObject(Constants.CANNOT_DELETE_AN_INACTIVE_RECORD);
				throw buildRuntimeCustomException(exceptionCode);
			}
		} else {
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_DELETE_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlConnectorVb>) collTemp).get(0);
		vObject.setDateCreation(vObjectlocal.getDateCreation());
		if (vObject.isStaticDelete()) {
			vObjectlocal.setMaker(getIntCurrentUserId());
			vObject.setVerifier(getIntCurrentUserId());
			vObject.setRecordIndicator(Constants.STATUS_ZERO);
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
			if (vObject.getConnectorType().equalsIgnoreCase("S")
					|| vObjectlocal.getConnectorType().equalsIgnoreCase("S")) {
				EtlMqTableVb etlMqTableVb = new EtlMqTableVb();
				etlMqTableVb.setConnectorId(vObject.getConnectorId());
				etlMqTableVb.setRecordIndicator(vObject.getRecordIndicator());
				etlMqTableVb.setStaticDelete(vObject.isStaticDelete());
				listmanualQuery = etlMqTableDao.getQueryResultsByParent(etlMqTableVb.getConnectorId(), 0);
				if (listmanualQuery != null && listmanualQuery.size() > 0) {
					for (EtlMqTableVb tableVb : listmanualQuery) {
						tableVb.setStaticDelete(false);
						exceptionCode = etlMqTableDao.doDeleteApprRecordForNonTrans(tableVb);
						if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
				listmanualQuery = etlMqTableDao.getQueryResultsByParent(etlMqTableVb.getConnectorId(), 1);
				if (listmanualQuery != null && listmanualQuery.size() > 0) {
					for (EtlMqTableVb tableVb : listmanualQuery) {
						exceptionCode = etlMqTableDao.doRejectRecord(tableVb);
						if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
			} else if (vObject.getConnectorType().equalsIgnoreCase("SS")
					|| vObjectlocal.getConnectorType().equalsIgnoreCase("SS")) {
				List<EtlFileUploadAreaVb> semiStructuredPendList = etlFileUploadAreaDao
						.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_PENDING);
				if (semiStructuredPendList != null && semiStructuredPendList.size() > 0) {
					for (EtlFileUploadAreaVb etlFileUploadAreaVb : semiStructuredPendList) {
						exceptionCode = etlFileUploadAreaDao.doRejectRecord(etlFileUploadAreaVb);
						if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
				List<EtlFileUploadAreaVb> semiStructuredMainList = etlFileUploadAreaDao
						.getQueryResultsByParent(vObject.getConnectorId(), Constants.STATUS_ZERO);
				if (semiStructuredMainList != null && semiStructuredMainList.size() > 0) {
					for (EtlFileUploadAreaVb etlFileUploadAreaVb : semiStructuredMainList) {
						etlFileUploadAreaVb.setStaticDelete(false);
						exceptionCode = etlFileUploadAreaDao.doDeleteApprRecordForNonTrans(etlFileUploadAreaVb);
						if (exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION) {
							exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
							throw buildRuntimeCustomException(exceptionCode);
						}
					}
				}
			}
			VisionUsersVb visionUSersVb = CustomContextHolder.getContext();
			EtlConnectorAccessVb etlConnectorAccessVb = new EtlConnectorAccessVb();
			etlConnectorAccessVb.setConnectorId(vObject.getConnectorId());
			etlConnectorAccessVb.setUserGroup(visionUSersVb.getUserGroup());
			etlConnectorAccessVb.setUserProfile(visionUSersVb.getUserProfile());
			etlConnectorAccessVb.setWriteFlag("Y");
			etlConnectorAccessVb.setVerificationRequired(false);
			etlConnectorAccessVb.setStaticDelete(vObject.isStaticDelete());
			etlConnectorAccessDao.doDeleteApprRecordForNonTrans(etlConnectorAccessVb);
			vObject.setDateLastModified(systemDate);
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
	public List<EtlConnectorVb> findLeBookByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY||'-'||LE_BOOK CL FROM LE_BOOK LEB WHERE LEB_STATUS=0 AND UPPER(LEB.LEB_DESCRIPTION) " + val + "ORDER BY LE_BOOK";
		return  getJdbcTemplate().query(sql, getMapperForLeBook());
	}
	protected RowMapper getMapperForLeBook(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorVb etlFeedCategoryVb = new EtlConnectorVb();
				etlFeedCategoryVb.setLeBook(rs.getString("CL"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}
	public List<EtlConnectorVb> findCountryByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY FROM COUNTRIES CT WHERE UPPER(CT.COUNTRY_DESCRIPTION) " + val + "ORDER BY COUNTRY";
		return  getJdbcTemplate().query(sql, getMapperForCountry());
	}
	
	protected RowMapper getMapperForCountry(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorVb etlFeedCategoryVb = new EtlConnectorVb();
				etlFeedCategoryVb.setCountry(rs.getString("COUNTRY"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}
	
	protected int deleteRecordByPRSParent(EtlFeedMainVb dObj, String tableType){
		String query = "Delete From ETL_CONNECTOR_PRS Where COUNTRY = ?  AND LE_BOOK = ?  AND feed_id = ?  AND SESSION_ID =?";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedId() , dObj.getSessionId()};
		return getJdbcTemplate().update(query,args);
	}
	@Override
	protected ExceptionCode doUpdateRecordForNonTrans(EtlConnectorVb vObject) throws RuntimeCustomException {
		List<EtlConnectorVb> collTemp = null;
		EtlConnectorVb vObjectlocal = null;
		ExceptionCode exceptionCode =null;
		strApproveOperation =Constants.MODIFY;
		strErrorDesc  = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		// Search if record already exists in pending.  If it already exists, check for status
		collTemp = doSelectPendingRecord(vObject);
		if (collTemp == null){
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if (collTemp.size() > 0) {
			vObjectlocal = ((ArrayList<EtlConnectorVb>) collTemp).get(0);

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
				vObjectlocal = ((ArrayList<EtlConnectorVb>) collTemp).get(0);
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
		if(vObject.getConnectorType().equalsIgnoreCase("SS")){
		if (vObject.getChildren() != null && vObject.getChildren().size() > 0) {
			etlFileUploadAreaDao.deletePendingRecordAll(vObject.getConnectorId());
			etlFileTableDao.deletePendingRecordAll(vObject.getConnectorId());
			etlFileColumnsDao.deletePendingRecordAll(vObject.getConnectorId());
			List<EtlFileUploadAreaVb> etlFileUploadAreaVbs = dynamicScriptCreation(vObject.getChildren());
			for (EtlFileUploadAreaVb lObject : etlFileUploadAreaVbs) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setFileId(lObject.getFileId());
				lObject.setVerifier(vObject.getVerifier());
				lObject.setDateCreation(vObject.getDateCreation());
				lObject.setDateLastModified(vObject.getDateLastModified());
				lObject.setRecordIndicator(vObject.getRecordIndicator());
				exceptionCode = etlFileUploadAreaDao.doInsertRecordForNonTrans(lObject);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		}
		return getResultObject(Constants.SUCCESSFUL_OPERATION);
		
	}

	protected ExceptionCode doUpdateApprRecordForNonTrans(EtlConnectorVb vObject) throws RuntimeCustomException  {
		List<EtlConnectorVb> collTemp = null;
		EtlConnectorVb vObjectlocal = null;
		ExceptionCode exceptionCode = null;
		strApproveOperation =Constants.MODIFY;
		strErrorDesc  = "";
		strCurrentOperation = Constants.MODIFY;
		setServiceDefaults();
		vObject.setMaker(getIntCurrentUserId());
		if("RUNNING".equalsIgnoreCase(getBuildStatus(vObject))){
			exceptionCode = getResultObject(Constants.BUILD_IS_RUNNING);
			throw buildRuntimeCustomException(exceptionCode);
		}
		collTemp = selectApprovedRecord(vObject);
		if (collTemp == null){
			exceptionCode = getResultObject(Constants.ERRONEOUS_OPERATION);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObjectlocal = ((ArrayList<EtlConnectorVb>)collTemp).get(0);
		// Even if record is not there in Appr. table reject the record
		if (collTemp.size() == 0){
			exceptionCode = getResultObject(Constants.ATTEMPT_TO_MODIFY_UNEXISTING_RECORD);
			throw buildRuntimeCustomException(exceptionCode);
		}
		vObject.setRecordIndicator(Constants.STATUS_ZERO);
		vObject.setVerifier(getIntCurrentUserId());
		vObject.setDateCreation(vObjectlocal.getDateCreation());
		retVal = doUpdateAppr(vObject);
		if (retVal != Constants.SUCCESSFUL_OPERATION){
			exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
			throw buildRuntimeCustomException(exceptionCode);
		} else {
			exceptionCode = getResultObject(Constants.SUCCESSFUL_OPERATION);
		}
		String systemDate = getSystemDate();
		vObject.setDateLastModified(systemDate);
		exceptionCode = writeAuditLog(vObject, vObjectlocal);
		if(exceptionCode.getErrorCode() != Constants.SUCCESSFUL_OPERATION){
			exceptionCode = getResultObject(Constants.AUDIT_TRAIL_ERROR);
			throw buildRuntimeCustomException(exceptionCode);
		}
		if(vObject.getConnectorType().equalsIgnoreCase("SS")){
		if (vObject.getChildren() != null && vObject.getChildren().size() > 0) {
			etlFileUploadAreaDao.doDeleteApprAll(vObject.getConnectorId());
			etlFileTableDao.doDeleteApprAll(vObject.getConnectorId());
			etlFileColumnsDao.doDeleteApprAll(vObject.getConnectorId());
				List<EtlFileUploadAreaVb> etlFileUploadAreaVbs = dynamicScriptCreation(vObject.getChildren());
				for (EtlFileUploadAreaVb lObject : etlFileUploadAreaVbs) {
				lObject.setConnectorId(vObject.getConnectorId());
				lObject.setFileId(lObject.getFileId());
				vObject.setVerifier(0);
				vObject.setRecordIndicator(Constants.STATUS_INSERT);
				exceptionCode = etlFileUploadAreaDao.doInsertApprRecordForNonTrans(lObject);
				retVal = exceptionCode.getErrorCode();
				if (retVal != Constants.SUCCESSFUL_OPERATION) {
					exceptionCode = getResultObject(Constants.WE_HAVE_ERROR_DESCRIPTION);
					throw buildRuntimeCustomException(exceptionCode);
				}
			}
		}
		}
		return exceptionCode;
	}
	public List<EtlFileUploadAreaVb> dynamicScriptCreation(List<EtlFileUploadAreaVb> vObjects) {
		List<EtlFileUploadAreaVb> etlFileUploadAreaVbs = new ArrayList<>();
		for (EtlFileUploadAreaVb etlFileUploadAreaVb : vObjects) {
			StringBuffer variableScript = new StringBuffer("");
			JSONObject extractVbData = new JSONObject(etlFileUploadAreaVb);
			JSONArray eachColData = (JSONArray) extractVbData.getJSONArray("dynamicScript");
			for (int i = 0; i < eachColData.length(); i++) {
				JSONObject ss = eachColData.getJSONObject(i);
				String ch = fixJSONObject(ss);
				JSONObject extractData = new JSONObject(ch);
				String tag = extractData.getString("TAG");
				String encryption = extractData.getString("ENCRYPTION");
				String value = extractData.getString("VALUE");
				variableScript.append("{");
				if (ValidationUtil.isValid(encryption) && encryption.equalsIgnoreCase("Yes")) {
					variableScript.append(tag + ":#ENCRYPT$@!" + value + "#");
				} else {
					variableScript.append(tag + ":#CONSTANT$@!" + value + "#");
				}
				variableScript.append("}");
			}
			etlFileUploadAreaVb.setFtpDetailScript(String.valueOf(variableScript));
			etlFileUploadAreaVbs.add(etlFileUploadAreaVb);
		}
		return etlFileUploadAreaVbs;
	}
	public String fixJSONObject(JSONObject obj) {
		String jsonString = obj.toString();
		for (int i = 0; i < obj.names().length(); i++) {
			try {
				jsonString = jsonString.replace(obj.names().getString(i), obj.names().getString(i).toUpperCase());
			} catch (JSONException e) {
				if ("Y".equalsIgnoreCase(enableDebug)) {
					e.printStackTrace();
				}
			}
		}
		return jsonString;
	}
	
	public String getDbLinkValidationQuery(String dbType) {
		String sql = "select HTML_TAG_PROPERTY from VRD_OBJECT_PROPERTIES "
				+ " where VRD_OBJECT_ID = '"+dbType+"' AND OBJ_TAG_ID = 'DB_LINK_VALIDATION'";
		try {
			return getJdbcTemplate().queryForObject(sql, String.class);
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			return null;
		}
	}
	
	protected int deleteRecordByPRSParentByCategory(EtlFeedMainVb dObj) {
		String query = "Delete From ETL_CONNECTOR_PRS Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_CATEGORY = ?  AND SESSION_ID =? ";
		Object[] args = { dObj.getCountry(), dObj.getLeBook(), dObj.getFeedCategory(), dObj.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}
}
