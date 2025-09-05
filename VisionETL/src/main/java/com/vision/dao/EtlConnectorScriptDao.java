package com.vision.dao;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.vision.authentication.CustomContextHolder;
import com.vision.util.CommonUtils;
import com.vision.util.Constants;
import com.vision.util.ValidationUtil;
import com.vision.vb.EtlConnectorsScriptVb;
import com.vision.vb.NumSubTabVb;
import com.vision.vb.SmartSearchVb;
import com.vision.vb.VisionUsersVb;

@Component
public class EtlConnectorScriptDao extends AbstractDao<EtlConnectorsScriptVb> {

	@Autowired
	private VisionUsersDao visionUsersDao;

	@Autowired
	private NumSubTabDao numSubTabDao;
	
	
	/*******Mapper Start**********/
	String ScriptTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2122, "TAppr.SCRIPT_TYPE", "SCRIPT_TYPE_DESC");
	String ScriptTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2122, "TPend.SCRIPT_TYPE", "SCRIPT_TYPE_DESC");
	String ScriptTypeAtUplDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2122, "TUpl.SCRIPT_TYPE", "SCRIPT_TYPE_DESC");

	String ExecutionTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2123, "TAppr.EXECUTION_TYPE", "EXECUTION_TYPE_DESC");
	String ExecutionTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2123, "TPend.EXECUTION_TYPE", "EXECUTION_TYPE_DESC");
	String ExecutionTypeAtUplDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2123, "TUpl.EXECUTION_TYPE", "EXECUTION_TYPE_DESC");

	String QuerySTATUSNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.SCRIPT_STATUS", "SCRIPT_STATUS_DESC");
	String QuerySTATUSNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.SCRIPT_STATUS", "SCRIPT_STATUS_DESC");
	String QuerySTATUSNtUplDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TUpl.SCRIPT_STATUS", "SCRIPT_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");
	String RecordIndicatorNtUplDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TUpl.RECORD_INDICATOR", "RECORD_INDICATOR_DESC");

	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";


	@Override
	protected RowMapper getMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorsScriptVb vObject = new EtlConnectorsScriptVb();
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
				if(rs.getString("CONNECTOR_ID")!= null){ 
					vObject.setConnectorId(rs.getString("CONNECTOR_ID"));
				}else{
					vObject.setConnectorId("");
				}
				if(rs.getString("SCRIPT_ID")!= null){ 
					vObject.setScriptId(rs.getString("SCRIPT_ID").toUpperCase());
				}else{
					vObject.setScriptId("");
				}
//				if(rs.getString("SCRIPT")!= null){ 
//					vObject.setScript(rs.getString("SCRIPT"));
//				}else{
//					vObject.setScript("");
//				}
//				vObject.setScriptDesc(rs.getString("SCRIPT_DESCRIPTION"));
				vObject.setScriptTypeAt(rs.getInt("SCRIPT_TYPE_AT"));
				vObject.setScriptType(rs.getString("SCRIPT_TYPE"));
				vObject.setScriptTypeDesc(rs.getString("SCRIPT_TYPE_DESC"));
				vObject.setExecutionTypeAt(rs.getInt("EXECUTION_TYPE_AT"));
				vObject.setExecutionType(rs.getString("EXECUTION_TYPE"));
				vObject.setExecutionTypeDesc(rs.getString("EXECUTION_TYPE_DESC"));
				vObject.setScriptStatusNt(rs.getInt("SCRIPT_STATUS_NT"));
				vObject.setScriptStatus(rs.getInt("SCRIPT_STATUS"));
				vObject.setStatusDesc(rs.getString("SCRIPT_STATUS_DESC"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				if(rs.getString("DATE_LAST_MODIFIED")!= null){ 
					vObject.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				}else{
					vObject.setDateLastModified("");
				}
				if(rs.getString("DATE_CREATION")!= null){ 
					vObject.setDateCreation(rs.getString("DATE_CREATION"));
				}else{
					vObject.setDateCreation("");
				}
				return vObject;
			}
		};
		return mapper;
	}
	protected RowMapper getLocalMapper(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorsScriptVb vObject = new EtlConnectorsScriptVb();
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
				if(rs.getString("CONNECTOR_ID")!= null){ 
					vObject.setConnectorId(rs.getString("CONNECTOR_ID"));
				}else{
					vObject.setConnectorId("");
				}
				if(rs.getString("SCRIPT_ID")!= null){ 
					vObject.setScriptId(rs.getString("SCRIPT_ID"));
				}else{
					vObject.setScriptId("");
				}
				if(rs.getString("SCRIPT")!= null){ 
					vObject.setScript(rs.getString("SCRIPT"));
				}else{
					vObject.setScript("");
				}
				vObject.setScriptDesc(rs.getString("SCRIPT_DESCRIPTION"));
				vObject.setScriptTypeAt(rs.getInt("SCRIPT_TYPE_AT"));
				vObject.setScriptType(rs.getString("SCRIPT_TYPE"));
				vObject.setScriptTypeDesc(rs.getString("SCRIPT_TYPE_DESC"));
				vObject.setExecutionTypeAt(rs.getInt("EXECUTION_TYPE_AT"));
				vObject.setExecutionType(rs.getString("EXECUTION_TYPE"));
				vObject.setExecutionTypeDesc(rs.getString("EXECUTION_TYPE_DESC"));
				vObject.setScriptStatusNt(rs.getInt("SCRIPT_STATUS_NT"));
				vObject.setScriptStatus(rs.getInt("SCRIPT_STATUS"));
				vObject.setStatusDesc(rs.getString("SCRIPT_STATUS_DESC"));
				vObject.setRecordIndicatorNt(rs.getInt("RECORD_INDICATOR_NT"));
				vObject.setRecordIndicator(rs.getInt("RECORD_INDICATOR"));
				vObject.setRecordIndicatorDesc(rs.getString("RECORD_INDICATOR_DESC"));
				vObject.setMaker(rs.getInt("MAKER"));
				vObject.setVerifier(rs.getInt("VERIFIER"));
				vObject.setMakerName(rs.getString("MAKER_NAME"));
				vObject.setVerifierName(rs.getString("VERIFIER_NAME"));
				vObject.setInternalStatus(rs.getInt("INTERNAL_STATUS"));
				if(rs.getString("DATE_LAST_MODIFIED")!= null){ 
					vObject.setDateLastModified(rs.getString("DATE_LAST_MODIFIED"));
				}else{
					vObject.setDateLastModified("");
				}
				if(rs.getString("DATE_CREATION")!= null){ 
					vObject.setDateCreation(rs.getString("DATE_CREATION"));
				}else{
					vObject.setDateCreation("");
				}
				return vObject;
			}
		};
		return mapper;
	}

/*******Mapper End**********/
	public List<EtlConnectorsScriptVb> getQueryPopupResults(EtlConnectorsScriptVb dObj) {
		setServiceDefaults();

		Vector<Object> params = new Vector<Object>();

		String accessQueyr = ", ETL_CONNECTOR_ACCESS FCA, VISION_USERS VU "
				+ " WHERE (TAPPR.Connector_ID =  FCA.Connector_ID  AND FCA.USER_GROUP = VU.USER_GROUP "
				+ " AND FCA.USER_PROFILE = VU.USER_PROFILE  AND VU.VISION_ID = " + intCurrentUserId + " ) ";

		StringBuffer strBufApprove = new StringBuffer(
				"Select FCA.write_flag, TAPPR.COUNTRY," + CountryApprDesc + ",TAPPR.LE_BOOK," + LeBookApprDesc + ","
						+ " TAppr.CONNECTOR_ID,TAppr.SCRIPT_ID,TAppr.SCRIPT_DESCRIPTION"
						// + ",TAppr.SCRIPT"
						+ ",TAppr.SCRIPT_TYPE_AT,TAppr.SCRIPT_TYPE," + ScriptTypeAtApprDesc
						+ ",TAppr.EXECUTION_TYPE_AT,TAppr.EXECUTION_TYPE," + ExecutionTypeAtApprDesc
						+ ",TAppr.SCRIPT_STATUS_NT,TAppr.SCRIPT_STATUS," + QuerySTATUSNtApprDesc
						+ ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR," + RecordIndicatorNtApprDesc
						+ ",TAppr.MAKER, " + makerApprDesc + ",TAppr.VERIFIER, " + verifierApprDesc
						+ ",TAppr.INTERNAL_STATUS,TAPPR.DATE_LAST_MODIFIED date_modified, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
						+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TAppr.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
						+ " from ETL_CONNECTOR_SCRIPTS TAppr " + accessQueyr);
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_CONNECTOR_SCRIPTS_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.CONNECTOR_ID = TPend.CONNECTOR_ID AND TAppr.SCRIPT_ID = TPend.SCRIPT_ID )");
		StringBuffer strBufPending = new StringBuffer(
				"Select  FCA.write_flag,TPend.COUNTRY," + CountryPendDesc + ",TPend.LE_BOOK," + LeBookTPendDesc + ","
						+ "TPend.CONNECTOR_ID,TPend.SCRIPT_ID,TPend.SCRIPT_DESCRIPTION"
						// + ",TPend.SCRIPT"
						+ ",TPend.SCRIPT_TYPE_AT,TPend.SCRIPT_TYPE," + ScriptTypeAtPendDesc
						+ ",TPend.EXECUTION_TYPE_AT,TPend.EXECUTION_TYPE," + ExecutionTypeAtPendDesc
						+ ",TPend.SCRIPT_STATUS_NT,TPend.SCRIPT_STATUS," + QuerySTATUSNtPendDesc
						+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR," + RecordIndicatorNtPendDesc
						+ ",TPend.MAKER, " + makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc
						+ ",TPend.INTERNAL_STATUS,TPEND.DATE_LAST_MODIFIED date_modified, "
						+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
						+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
						+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null)
						+ "(TPend.DATE_CREATION, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
						+ getDbFunction(Constants.TIME, null) + "') DATE_CREATION"
						+ " from ETL_CONNECTOR_SCRIPTS_PEND TPend " + accessQueyr.replaceAll("TAPPR", "TPEND"));
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
						List<EtlConnectorsScriptVb> countryData = findCountryByCustomFilter(val);
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
						List<EtlConnectorsScriptVb> leBookData = findLeBookByCustomFilter(val);
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
							CommonUtils.addToQuery(" (TAPPR.COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
									+ getDbFunction(Constants.PIPELINE, null) + "TAPPR.LE_BOOK) IN (" + lebData + ") ",
									strBufApprove);
							CommonUtils.addToQuery(" (TPend.COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
									+ getDbFunction(Constants.PIPELINE, null) + "TPend.LE_BOOK) IN (" + lebData + ") ",
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

					case "scriptId":
						CommonUtils.addToQuery(" upper(TAppr.SCRIPT_ID) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.SCRIPT_ID) " + val, strBufPending);
						break;

					case "scriptDesc":
						CommonUtils.addToQuery(" upper(TAppr.SCRIPT_DESCRIPTION) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.SCRIPT_DESCRIPTION) " + val, strBufPending);
						break;

					case "script":
						CommonUtils.addToQuery(" upper(TAppr.SCRIPT) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.SCRIPT) " + val, strBufPending);
						break;

					case "scriptType":
						CommonUtils.addToQuery(" upper(TAppr.SCRIPT_TYPE) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.SCRIPT_TYPE) " + val, strBufPending);
						break;

					case "executionType":
						CommonUtils.addToQuery(" upper(TAppr.EXECUTION_TYPE) " + val, strBufApprove);
						CommonUtils.addToQuery(" upper(TPend.EXECUTION_TYPE) " + val, strBufPending);
						break;

					case "scriptStatus":
						CommonUtils.addToQuery(" upper(TAppr.SCRIPT_STATUS) " + val, strBufApprove);

						CommonUtils.addToQuery(" upper(TPend.SCRIPT_STATUS) " + val, strBufPending);

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
							CommonUtils.addToQuery(" (TAPPR.SCRIPT_STATUS) IN (" + actData + ") ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.SCRIPT_STATUS) IN (" + actData + ") ", strBufPending);
						} else {
							CommonUtils.addToQuery(" (TAPPR.SCRIPT_STATUS) IN ('') ", strBufApprove);
							CommonUtils.addToQuery(" (TPEND.SCRIPT_STATUS) IN ('') ", strBufPending);
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
			return getQueryPopupResults(dObj, strBufPending, strBufApprove, strWhereNotExists, orderBy, params);
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlConnectorsScriptVb> getQueryResults(EtlConnectorsScriptVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlConnectorsScriptVb> collTemp = null;
		final int intKeyFieldsCount = 4;
		StringBuffer strQueryAppr = new StringBuffer("Select TAppr.COUNTRY," + CountryApprDesc + ",TAppr.LE_BOOK, "
				+ LeBookApprDesc + ",TAppr.CONNECTOR_ID,TAppr.SCRIPT_ID,TAppr.SCRIPT_DESCRIPTION"
				+ ",TAppr.SCRIPT,TAppr.SCRIPT_TYPE_AT,TAppr.SCRIPT_TYPE," + ScriptTypeAtApprDesc
				+ ",TAppr.EXECUTION_TYPE_AT,TAppr.EXECUTION_TYPE," + ExecutionTypeAtApprDesc
				+ ",TAppr.SCRIPT_STATUS_NT,TAppr.SCRIPT_STATUS," + QuerySTATUSNtApprDesc
				+ ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR," + RecordIndicatorNtApprDesc
				+ ",TAppr.MAKER, " + makerApprDesc + ",TAppr.VERIFIER, " + verifierApprDesc + ",TAppr.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " from ETL_CONNECTOR_SCRIPTS TAppr WHERE  UPPER(TAppr.COUNTRY) = ?  AND UPPER(TAppr.LE_BOOK) = ? and UPPER(CONNECTOR_ID) = ?  AND UPPER(SCRIPT_ID) = ?  ");
		StringBuffer strQueryPend = new StringBuffer("Select TPend.COUNTRY," + CountryPendDesc + ",TPend.LE_BOOK, "
				+ LeBookTPendDesc + ",TPend.CONNECTOR_ID,TPend.SCRIPT_ID,TPend.SCRIPT_DESCRIPTION"
				+ ",TPend.SCRIPT,TPend.SCRIPT_TYPE_AT,TPend.SCRIPT_TYPE," + ScriptTypeAtPendDesc
				+ ",TPend.EXECUTION_TYPE_AT,TPend.EXECUTION_TYPE," + ExecutionTypeAtPendDesc
				+ ",TPend.SCRIPT_STATUS_NT,TPend.SCRIPT_STATUS," + QuerySTATUSNtPendDesc
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR," + RecordIndicatorNtPendDesc
				+ ",TPend.MAKER, " + makerPendDesc + ",TPend.VERIFIER, " + verifierPendDesc + ",TPend.INTERNAL_STATUS"
				+ ", " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " from ETL_CONNECTOR_SCRIPTS_PEND TPend WHERE UPPER(TPend.COUNTRY) = ?  AND UPPER(TPend.LE_BOOK) = ? and UPPER(TPend.CONNECTOR_ID) = ?  AND UPPER(TPend.SCRIPT_ID) = ?  ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry().toUpperCase();
		objParams[1] = dObj.getLeBook().toUpperCase();
		objParams[2] = dObj.getConnectorId().toUpperCase();
		objParams[3] = dObj.getScriptId().toUpperCase();

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
	protected List<EtlConnectorsScriptVb> selectApprovedRecord(EtlConnectorsScriptVb vObject){
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}


	@Override
	protected List<EtlConnectorsScriptVb> doSelectPendingRecord(EtlConnectorsScriptVb vObject){
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}


	@Override
	protected List<EtlConnectorsScriptVb> doSelectUplRecord(EtlConnectorsScriptVb vObject){
		return getQueryResults(vObject, 9999);
	}


	@Override
	protected int getStatus(EtlConnectorsScriptVb records){return records.getScriptStatus();}


	@Override
	protected void setStatus(EtlConnectorsScriptVb vObject,int status){vObject.setScriptStatus(status);}


	@Override
	protected int doInsertionAppr(EtlConnectorsScriptVb vObject) {
		String query = "Insert Into ETL_CONNECTOR_SCRIPTS ( COUNTRY, LE_BOOK,CONNECTOR_ID, SCRIPT_ID, SCRIPT_DESCRIPTION, SCRIPT_TYPE_AT,SCRIPT_TYPE, EXECUTION_TYPE_AT, "
				+ "EXECUTION_TYPE,  SCRIPT_STATUS_NT,SCRIPT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS,  "
				+ "DATE_LAST_MODIFIED, DATE_CREATION, SCRIPT)"
				+ "Values (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?,?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ",?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getScriptId(),
				vObject.getScriptDesc(), 2122, vObject.getScriptType(), 2123, vObject.getExecutionType(),
				vObject.getScriptStatusNt(), vObject.getScriptStatus(), vObject.getRecordIndicatorNt(),
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
					String clobData = ValidationUtil.isValid(vObject.getScript()) ? vObject.getScript() : "";
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
	protected int doInsertionPend(EtlConnectorsScriptVb vObject) {
		String query = "Insert Into ETL_CONNECTOR_SCRIPTS_PEND ( COUNTRY, LE_BOOK,CONNECTOR_ID, SCRIPT_ID, SCRIPT_DESCRIPTION, SCRIPT_TYPE_AT,SCRIPT_TYPE, EXECUTION_TYPE_AT,EXECUTION_TYPE,   SCRIPT_STATUS_NT,SCRIPT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, SCRIPT)"
				+ "Values (?,?,?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ",?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getScriptId(),
				vObject.getScriptDesc(), 2122, vObject.getScriptType(), 2123, vObject.getExecutionType(),
				vObject.getScriptStatusNt(), vObject.getScriptStatus(), vObject.getRecordIndicatorNt(),
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
					String clobData = ValidationUtil.isValid(vObject.getScript()) ? vObject.getScript() : "";
					ps.setCharacterStream(++argumentLength, new StringReader(clobData), clobData.length());
					return ps;
				}
			});
		} catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			result = Constants.WE_HAVE_ERROR_DESCRIPTION;
			strErrorDesc = e.getMessage();
		}
		return result;
	}
	
	@Override
	protected int doInsertionPendWithDc(EtlConnectorsScriptVb vObject) {
		String query = "Insert Into ETL_CONNECTOR_SCRIPTS_PEND ( COUNTRY, LE_BOOK, CONNECTOR_ID, SCRIPT_ID, SCRIPT_DESCRIPTION, SCRIPT_TYPE_AT,SCRIPT_TYPE, EXECUTION_TYPE_AT,EXECUTION_TYPE,   SCRIPT_STATUS_NT,SCRIPT_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION, SCRIPT)"
				+ "Values (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?,?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.SYSDATE, null) + ",?)";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getScriptId(),
				vObject.getScriptDesc(), 2122, vObject.getScriptType(), 2123, vObject.getExecutionType(),
				vObject.getScriptStatusNt(), vObject.getScriptStatus(), vObject.getRecordIndicatorNt(),
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
					String clobData = ValidationUtil.isValid(vObject.getScript()) ? vObject.getScript() : "";
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
	protected int doUpdateAppr(EtlConnectorsScriptVb vObject) {
		String query = "Update ETL_CONNECTOR_SCRIPTS Set SCRIPT = ?, SCRIPT_DESCRIPTION = ?, SCRIPT_TYPE_AT = ?, SCRIPT_TYPE = ?,"
				+ " EXECUTION_TYPE_AT = ?, EXECUTION_TYPE = ?,  SCRIPT_STATUS_NT = ?,"
				+ " SCRIPT_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, "
				+ " VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = " + getDbFunction(Constants.SYSDATE, null)
				+ " " + " WHERE COUNTRY =? and  LE_BOOK =? and CONNECTOR_ID = ?  AND SCRIPT_ID = ? ";
		Object[] args = { vObject.getScriptDesc(), 2122, vObject.getScriptType(), 2123, vObject.getExecutionType(),
				vObject.getScriptStatusNt(), vObject.getScriptStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getScriptId() };
		int result = 0;
		try {
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getScript()) ? vObject.getScript() : "";
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
	protected int doUpdatePend(EtlConnectorsScriptVb vObject){
	String query = "Update ETL_CONNECTOR_SCRIPTS_PEND Set SCRIPT = ?, SCRIPT_DESCRIPTION = ?, SCRIPT_TYPE_AT = ?, SCRIPT_TYPE = ?,"
			+ " EXECUTION_TYPE_AT = ?, EXECUTION_TYPE = ?,  SCRIPT_STATUS_NT = ?,"
			+ " SCRIPT_STATUS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, "
			+ "VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "+getDbFunction(Constants.SYSDATE, null)+" "
			+ " WHERE country=? and le_book =? and CONNECTOR_ID = ?  AND SCRIPT_ID = ? ";
		
		Object[] args = { vObject.getScriptDesc(), 2122, vObject.getScriptType(), 2123, vObject.getExecutionType(),
				vObject.getScriptStatusNt(), vObject.getScriptStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getScriptId() };
	
		int result=0;
		try{
			return getJdbcTemplate().update(new PreparedStatementCreator() {
				@Override
				public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
					PreparedStatement ps = connection.prepareStatement(query);
					int psIndex = 0;
					String clobData = ValidationUtil.isValid(vObject.getScript()) ? vObject.getScript() : "";
					ps.setCharacterStream(++psIndex, new StringReader(clobData), clobData.length());

					for (int i = 1; i <= args.length; i++) {
						ps.setObject(++psIndex, args[i - 1]);
					}

					return ps;
				}
			});
		}catch (Exception e) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				e.printStackTrace();
			}
			strErrorDesc = e.getMessage();
			result = Constants.WE_HAVE_ERROR_DESCRIPTION;
		}
		return result;
	}

	@Override
	protected int doDeleteAppr(EtlConnectorsScriptVb vObject){
		String query = "Delete From ETL_CONNECTOR_SCRIPTS Where country =? and le_book=? and CONNECTOR_ID = ?  AND SCRIPT_ID = ?  ";
		Object[] args = {  vObject.getCountry(), vObject.getLeBook(),vObject.getConnectorId(), vObject.getScriptId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deletePendingRecord(EtlConnectorsScriptVb vObject){
		String query = "Delete From ETL_CONNECTOR_SCRIPTS_PEND Where country =? and le_book=? and CONNECTOR_ID = ?  AND SCRIPT_ID = ?  ";
		Object[] args = {  vObject.getCountry(), vObject.getLeBook(),vObject.getConnectorId(), vObject.getScriptId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected int deleteUplRecord(EtlConnectorsScriptVb vObject){
		String query = "Delete From ETL_CONNECTOR_SCRIPTS_UPL Where country =? and le_book=? and CONNECTOR_ID = ?  AND SCRIPT_ID = ?  ";
		Object[] args = {vObject.getCountry(), vObject.getLeBook(),  vObject.getConnectorId(), vObject.getScriptId() };
		return getJdbcTemplate().update(query,args);
	}


	@Override
	protected String getAuditString(EtlConnectorsScriptVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getScriptId()))
			strAudit.append("SCRIPT_ID" + auditDelimiterColVal + vObject.getScriptId().trim());
		else
			strAudit.append("SCRIPT_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getScriptDesc()))
			strAudit.append("SCRIPT_DESCRIPTION" + auditDelimiterColVal + vObject.getScriptDesc().trim());
		else
			strAudit.append("SCRIPT_DESCRIPTION" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("SCRIPT_TYPE_AT" + auditDelimiterColVal + 2122);
		strAudit.append(auditDelimiter);

		strAudit.append("SCRIPT_TYPE" + auditDelimiterColVal + vObject.getScriptType());
		strAudit.append(auditDelimiter);

		strAudit.append("EXECUTION_TYPE_AT" + auditDelimiterColVal + 2123);
		strAudit.append(auditDelimiter);

		strAudit.append("EXECUTION_TYPE" + auditDelimiterColVal + vObject.getExecutionType());
		strAudit.append(auditDelimiter);

		strAudit.append("SCRIPT" + auditDelimiterColVal + vObject.getScript());
		strAudit.append(auditDelimiter);

		strAudit.append("SCRIPT_STATUS_NT" + auditDelimiterColVal + vObject.getScriptStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("SCRIPT_STATUS" + auditDelimiterColVal + vObject.getScriptStatus());
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

	public List<EtlConnectorsScriptVb> findCountryByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY FROM COUNTRIES CT WHERE UPPER(CT.COUNTRY_DESCRIPTION) "+val+" ORDER BY COUNTRY";
		return  getJdbcTemplate().query(sql, getMapperForCountry());
	}
	
	protected RowMapper getMapperForCountry(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorsScriptVb etlFeedCategoryVb = new EtlConnectorsScriptVb();
				etlFeedCategoryVb.setCountry(rs.getString("COUNTRY"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}

	public List<EtlConnectorsScriptVb> findLeBookByCustomFilter(String val) throws DataAccessException {
		String sql = "SELECT COUNTRY" + getDbFunction(Constants.PIPELINE, null) + "'-'"
				+ getDbFunction(Constants.PIPELINE, null)
				+ "LE_BOOK CL FROM LE_BOOK LEB WHERE LEB_STATUS=0 AND UPPER(LEB.LEB_DESCRIPTION) "+val+" ORDER BY LE_BOOK";
		return getJdbcTemplate().query(sql, getMapperForLeBook());
	}
	protected RowMapper getMapperForLeBook(){
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlConnectorsScriptVb etlFeedCategoryVb = new EtlConnectorsScriptVb();
				etlFeedCategoryVb.setLeBook(rs.getString("CL"));
				return etlFeedCategoryVb;
			}
		};
		return mapper;
	}
	
	@Override
	protected void setServiceDefaults(){
		serviceName = "EtlConnectorsScriptList";
		serviceDesc = "ETL Connectors Script List";
		tableName = "ETL_CONNECTOR_SCRIPTS";
		childTableName = "ETL_CONNECTOR_SCRIPTS";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}
}