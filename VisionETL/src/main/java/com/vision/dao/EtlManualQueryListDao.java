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
import com.vision.vb.EtlManualQueryListVb;
import com.vision.vb.SmartSearchVb;

@Component
public class EtlManualQueryListDao extends AbstractDao<EtlManualQueryListVb> {

	/******* Mapper Start **********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String QueryStautsNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TAppr.QUERY_STAUTS",
			"QUERY_STAUTS_DESC");
	String QueryStautsNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 1, "TPend.QUERY_STAUTS",
			"QUERY_STAUTS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlManualQueryListVb vObject = new EtlManualQueryListVb();
				if (rs.getString("COUNTRY") != null) {
					vObject.setCountry(rs.getString("COUNTRY"));
				} else {
					vObject.setCountry("");
				}
				if (rs.getString("LE_BOOK") != null) {
					vObject.setLeBook(rs.getString("LE_BOOK"));
				} else {
					vObject.setLeBook("");
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
				if (rs.getString("PRE_SCRIPT1") != null) {
					vObject.setPreScript1(rs.getString("PRE_SCRIPT1"));
				} else {
					vObject.setPreScript1("");
				}
				if (rs.getString("PRE_SCRIPT2") != null) {
					vObject.setPreScript2(rs.getString("PRE_SCRIPT2"));
				} else {
					vObject.setPreScript2("");
				}
				if (rs.getString("PRE_SCRIPT3") != null) {
					vObject.setPreScript3(rs.getString("PRE_SCRIPT3"));
				} else {
					vObject.setPreScript3("");
				}
				if (rs.getString("SQL_QUERY1") != null) {
					vObject.setSqlQuery1(rs.getString("SQL_QUERY1"));
				} else {
					vObject.setSqlQuery1("");
				}
				if (rs.getString("SQL_QUERY2") != null) {
					vObject.setSqlQuery2(rs.getString("SQL_QUERY2"));
				} else {
					vObject.setSqlQuery2("");
				}
				if (rs.getString("SQL_QUERY3") != null) {
					vObject.setSqlQuery3(rs.getString("SQL_QUERY3"));
				} else {
					vObject.setSqlQuery3("");
				}
				if (rs.getString("VALIDATED_FLAG") != null) {
					vObject.setValidatedFlag(rs.getString("VALIDATED_FLAG"));
				} else {
					vObject.setValidatedFlag("");
				}
				vObject.setQueryStatusNt(rs.getInt("QUERY_STAUTS_NT"));
				vObject.setQueryStatus(rs.getInt("QUERY_STAUTS"));
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
	public List<EtlManualQueryListVb> getQueryPopupResults(EtlManualQueryListVb dObj) {

		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.CONNECTOR_ID"
				+ ",TAppr.QUERY_ID,TAppr.QUERY_DESCRIPTION,TAppr.PRE_SCRIPT1,TAppr.PRE_SCRIPT2"
				+ ",TAppr.PRE_SCRIPT3,TAppr.SQL_QUERY1,TAppr.SQL_QUERY2,TAppr.SQL_QUERY3"
				+ ",TAppr.VALIDATED_FLAG,TAppr.QUERY_STAUTS_NT,TAppr.QUERY_STAUTS"
				+ ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER"
				+ ",TAppr.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TAppr.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_MANUAL_QUERY_LIST TAppr ");
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_MANUAL_QUERY_LIST_PEND TPend WHERE TAppr.COUNTRY = TPend.COUNTRY AND TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.CONNECTOR_ID = TPend.CONNECTOR_ID AND TAppr.QUERY_ID = TPend.QUERY_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.CONNECTOR_ID"
				+ ",TPend.QUERY_ID,TPend.QUERY_DESCRIPTION,TPend.PRE_SCRIPT1,TPend.PRE_SCRIPT2"
				+ ",TPend.PRE_SCRIPT3,TPend.SQL_QUERY1,TPend.SQL_QUERY2,TPend.SQL_QUERY3"
				+ ",TPend.VALIDATED_FLAG,TPend.QUERY_STAUTS_NT,TPend.QUERY_STAUTS"
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER"
				+ ",TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TPend.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_MANUAL_QUERY_LIST_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.CONNECTOR_ID"
				+ ",TUpl.QUERY_ID,TUpl.QUERY_DESCRIPTION,TUpl.PRE_SCRIPT1,TUpl.PRE_SCRIPT2"
				+ ",TUpl.PRE_SCRIPT3,TUpl.SQL_QUERY1,TUpl.SQL_QUERY2,TUpl.SQL_QUERY3"
				+ ",TUpl.VALIDATED_FLAG,TUpl.QUERY_STAUTS_NT,TUpl.QUERY_STAUTS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_MANUAL_QUERY_LIST_UPL TUpl ");

		String strWhereNotExistsApprInUpl = new String(
				" Not Exists (Select 'X' From ETL_MANUAL_QUERY_LIST_UPL TUpl WHERE TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.CONNECTOR_ID = TUpl.CONNECTOR_ID AND TAppr.QUERY_ID = TUpl.QUERY_ID) ");
		String strWhereNotExistsPendInUpl = new String(
				" Not Exists (Select 'X' From ETL_MANUAL_QUERY_LIST_UPL TUpl WHERE TUpl.COUNTRY = TPend.COUNTRY AND TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.CONNECTOR_ID = TPend.CONNECTOR_ID AND TUpl.QUERY_ID = TPend.QUERY_ID) ");
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
						CommonUtils.addToQuerySearch(" upper(TUpl.COUNTRY) " + val, strBufUpl, data.getJoinType());
						break;

					case "leBook":
						CommonUtils.addToQuerySearch(" upper(TAppr.LE_BOOK) " + val, strBufApprove, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.LE_BOOK) " + val, strBufPending, data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.LE_BOOK) " + val, strBufUpl, data.getJoinType());
						break;

					case "connectorId":
						CommonUtils.addToQuerySearch(" upper(TAppr.CONNECTOR_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.CONNECTOR_ID) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.CONNECTOR_ID) " + val, strBufUpl, data.getJoinType());
						break;

					case "queryId":
						CommonUtils.addToQuerySearch(" upper(TAppr.QUERY_ID) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.QUERY_ID) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.QUERY_ID) " + val, strBufUpl, data.getJoinType());
						break;

					case "queryDescription":
						CommonUtils.addToQuerySearch(" upper(TAppr.QUERY_DESCRIPTION) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.QUERY_DESCRIPTION) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.QUERY_DESCRIPTION) " + val, strBufUpl,
								data.getJoinType());
						break;

					case "preScript1":
						CommonUtils.addToQuerySearch(" upper(TAppr.PRE_SCRIPT1) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.PRE_SCRIPT1) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.PRE_SCRIPT1) " + val, strBufUpl, data.getJoinType());
						break;

					case "preScript2":
						CommonUtils.addToQuerySearch(" upper(TAppr.PRE_SCRIPT2) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.PRE_SCRIPT2) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.PRE_SCRIPT2) " + val, strBufUpl, data.getJoinType());
						break;

					case "preScript3":
						CommonUtils.addToQuerySearch(" upper(TAppr.PRE_SCRIPT3) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.PRE_SCRIPT3) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.PRE_SCRIPT3) " + val, strBufUpl, data.getJoinType());
						break;

					case "sqlQuery1":
						CommonUtils.addToQuerySearch(" upper(TAppr.SQL_QUERY1) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.SQL_QUERY1) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.SQL_QUERY1) " + val, strBufUpl, data.getJoinType());
						break;

					case "sqlQuery2":
						CommonUtils.addToQuerySearch(" upper(TAppr.SQL_QUERY2) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.SQL_QUERY2) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.SQL_QUERY2) " + val, strBufUpl, data.getJoinType());
						break;

					case "sqlQuery3":
						CommonUtils.addToQuerySearch(" upper(TAppr.SQL_QUERY3) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.SQL_QUERY3) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.SQL_QUERY3) " + val, strBufUpl, data.getJoinType());
						break;

					case "validatedFlag":
						CommonUtils.addToQuerySearch(" upper(TAppr.VALIDATED_FLAG) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.VALIDATED_FLAG) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.VALIDATED_FLAG) " + val, strBufUpl,
								data.getJoinType());
						break;

					case "queryStauts":
						CommonUtils.addToQuerySearch(" upper(TAppr.QUERY_STAUTS) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.QUERY_STAUTS) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.QUERY_STAUTS) " + val, strBufUpl, data.getJoinType());
						break;

					case "recordIndicator":
						CommonUtils.addToQuerySearch(" upper(TAppr.RECORD_INDICATOR) " + val, strBufApprove,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TPend.RECORD_INDICATOR) " + val, strBufPending,
								data.getJoinType());
						CommonUtils.addToQuerySearch(" upper(TUpl.RECORD_INDICATOR) " + val, strBufUpl,
								data.getJoinType());
						break;

					default:
					}
					count++;
				}
			}
			String orderBy = " Order By COUNTRY, LE_BOOK, CONNECTOR_ID, QUERY_ID ";
			CommonUtils.addToQuery(strWhereNotExistsApprInUpl, strBufApprove);
			CommonUtils.addToQuery(strWhereNotExistsPendInUpl, strBufPending);
			StringBuffer lPageQuery = new StringBuffer();
			if (dObj.isVerificationRequired())
				lPageQuery.append(strBufPending.toString() + " Union " + strBufUpl.toString());
			else
				lPageQuery.append(strBufApprove.toString() + " Union " + strBufUpl.toString());
			return getQueryPopupResultsPgn(dObj, lPageQuery, strBufApprove, strWhereNotExists, orderBy, params,
					getMapper());
		} catch (Exception ex) {
			if ("Y".equalsIgnoreCase(enableDebug)) {
				ex.printStackTrace();
			}
			return null;
		}
	}

	public List<EtlManualQueryListVb> getQueryResults(EtlManualQueryListVb dObj, int intStatus) {

		setServiceDefaults();

		List<EtlManualQueryListVb> collTemp = null;

		final int intKeyFieldsCount = 4;
		String strQueryAppr = new String("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.CONNECTOR_ID"
				+ ",TAppr.QUERY_ID,TAppr.QUERY_DESCRIPTION,TAppr.PRE_SCRIPT1,TAppr.PRE_SCRIPT2"
				+ ",TAppr.PRE_SCRIPT3,TAppr.SQL_QUERY1,TAppr.SQL_QUERY2,TAppr.SQL_QUERY3"
				+ ",TAppr.VALIDATED_FLAG,TAppr.QUERY_STAUTS_NT,TAppr.QUERY_STAUTS"
				+ ",TAppr.RECORD_INDICATOR_NT,TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER"
				+ ",TAppr.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TAppr.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_MANUAL_QUERY_LIST TAppr WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.CONNECTOR_ID"
				+ ",TPend.QUERY_ID,TPend.QUERY_DESCRIPTION,TPend.PRE_SCRIPT1,TPend.PRE_SCRIPT2"
				+ ",TPend.PRE_SCRIPT3,TPend.SQL_QUERY1,TPend.SQL_QUERY2,TPend.SQL_QUERY3"
				+ ",TPend.VALIDATED_FLAG,TPend.QUERY_STAUTS_NT,TPend.QUERY_STAUTS"
				+ ",TPend.RECORD_INDICATOR_NT,TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER"
				+ ",TPend.INTERNAL_STATUS, " + getDbFunction(Constants.DATEFUNC, null)
				+ "(TPend.DATE_LAST_MODIFIED, '" + getDbFunction(Constants.DD_Mon_RRRR, null) + " "
				+ getDbFunction(Constants.TIME, null) + "') DATE_LAST_MODIFIED, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_MANUAL_QUERY_LIST_PEND TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.CONNECTOR_ID"
				+ ",TUpl.QUERY_ID,TUpl.QUERY_DESCRIPTION,TUpl.PRE_SCRIPT1,TUpl.PRE_SCRIPT2"
				+ ",TUpl.PRE_SCRIPT3,TUpl.SQL_QUERY1,TUpl.SQL_QUERY2,TUpl.SQL_QUERY3"
				+ ",TUpl.VALIDATED_FLAG,TUpl.QUERY_STAUTS_NT,TUpl.QUERY_STAUTS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_MANUAL_QUERY_LIST_UPL TPend WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getCountry();
		objParams[1] = dObj.getLeBook();
		objParams[2] = dObj.getConnectorId();
		objParams[3] = dObj.getQueryId();

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
	protected List<EtlManualQueryListVb> selectApprovedRecord(EtlManualQueryListVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlManualQueryListVb> doSelectPendingRecord(EtlManualQueryListVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlManualQueryListVb> doSelectUplRecord(EtlManualQueryListVb vObject) {
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlManualQueryListVb records) {
		return records.getQueryStatus();
	}

	@Override
	protected void setStatus(EtlManualQueryListVb vObject, int status) {
		vObject.setQueryStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlManualQueryListVb vObject) {
		String query = "Insert Into ETL_MANUAL_QUERY_LIST (COUNTRY, LE_BOOK, CONNECTOR_ID, QUERY_ID, QUERY_DESCRIPTION, PRE_SCRIPT1, PRE_SCRIPT2, PRE_SCRIPT3, SQL_QUERY1, SQL_QUERY2, SQL_QUERY3, VALIDATED_FLAG, QUERY_STAUTS_NT, QUERY_STAUTS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId(),
				vObject.getQueryDescription(), vObject.getPreScript1(), vObject.getPreScript2(),
				vObject.getPreScript3(), vObject.getSqlQuery1(), vObject.getSqlQuery2(), vObject.getSqlQuery3(),
				vObject.getValidatedFlag(), vObject.getQueryStatusNt(), vObject.getQueryStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlManualQueryListVb vObject) {
		String query = "Insert Into ETL_MANUAL_QUERY_LIST_PEND (COUNTRY, LE_BOOK, CONNECTOR_ID, QUERY_ID, QUERY_DESCRIPTION, PRE_SCRIPT1, PRE_SCRIPT2, PRE_SCRIPT3, SQL_QUERY1, SQL_QUERY2, SQL_QUERY3, VALIDATED_FLAG, QUERY_STAUTS_NT, QUERY_STAUTS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId(),
				vObject.getQueryDescription(), vObject.getPreScript1(), vObject.getPreScript2(),
				vObject.getPreScript3(), vObject.getSqlQuery1(), vObject.getSqlQuery2(), vObject.getSqlQuery3(),
				vObject.getValidatedFlag(), vObject.getQueryStatusNt(), vObject.getQueryStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlManualQueryListVb vObject) {
		String query = "Insert Into ETL_MANUAL_QUERY_LIST_PEND (COUNTRY, LE_BOOK, CONNECTOR_ID, QUERY_ID, QUERY_DESCRIPTION, PRE_SCRIPT1, PRE_SCRIPT2,"
				+ " PRE_SCRIPT3, SQL_QUERY1, SQL_QUERY2, SQL_QUERY3, VALIDATED_FLAG, QUERY_STAUTS_NT, QUERY_STAUTS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ " Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ getDbFunction(Constants.SYSDATE, null) + ", "
				+ getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId(),
				vObject.getQueryDescription(), vObject.getPreScript1(), vObject.getPreScript2(),
				vObject.getPreScript3(), vObject.getSqlQuery1(), vObject.getSqlQuery2(), vObject.getSqlQuery3(),
				vObject.getValidatedFlag(), vObject.getQueryStatusNt(), vObject.getQueryStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlManualQueryListVb vObject) {
		String query = "Update ETL_MANUAL_QUERY_LIST Set QUERY_DESCRIPTION = ?, PRE_SCRIPT1 = ?, PRE_SCRIPT2 = ?, PRE_SCRIPT3 = ?, SQL_QUERY1 = ?, "
				+ " SQL_QUERY2 = ?, SQL_QUERY3 = ?, VALIDATED_FLAG = ?, QUERY_STAUTS_NT = ?, QUERY_STAUTS = ?, RECORD_INDICATOR_NT = ?,  "
				+ "RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null)
				+ "  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ? ";
		Object[] args = { vObject.getQueryDescription(), vObject.getPreScript1(), vObject.getPreScript2(),
				vObject.getPreScript3(), vObject.getSqlQuery1(), vObject.getSqlQuery2(), vObject.getSqlQuery3(),
				vObject.getValidatedFlag(), vObject.getQueryStatusNt(), vObject.getQueryStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus(), vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(),
				vObject.getQueryId(), };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlManualQueryListVb vObject) {
		String query = "Update ETL_MANUAL_QUERY_LIST_PEND Set QUERY_DESCRIPTION = ?, PRE_SCRIPT1 = ?, PRE_SCRIPT2 = ?, PRE_SCRIPT3 = ?, SQL_QUERY1 = ?, "
				+ " SQL_QUERY2 = ?, SQL_QUERY3 = ?, VALIDATED_FLAG = ?, QUERY_STAUTS_NT = ?, QUERY_STAUTS = ?, RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, "
				+ " MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null)
				+ "  WHERE COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ? ";
		Object[] args = { vObject.getQueryDescription(), vObject.getPreScript1(), vObject.getPreScript2(),
				vObject.getPreScript3(), vObject.getSqlQuery1(), vObject.getSqlQuery2(), vObject.getSqlQuery3(),
				vObject.getValidatedFlag(), vObject.getQueryStatusNt(), vObject.getQueryStatus(),
				vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(),
				vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlManualQueryListVb vObject) {
		String query = "Delete From ETL_MANUAL_QUERY_LIST Where COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlManualQueryListVb vObject) {
		String query = "Delete From ETL_MANUAL_QUERY_LIST_PEND Where COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deleteUplRecord(EtlManualQueryListVb vObject) {
		String query = "Delete From ETL_MANUAL_QUERY_LIST_UPL Where COUNTRY = ?  AND LE_BOOK = ?  AND CONNECTOR_ID = ?  AND QUERY_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getConnectorId(), vObject.getQueryId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlManualQueryListVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getPreScript1()))
			strAudit.append("PRE_SCRIPT1" + auditDelimiterColVal + vObject.getPreScript1().trim());
		else
			strAudit.append("PRE_SCRIPT1" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getPreScript2()))
			strAudit.append("PRE_SCRIPT2" + auditDelimiterColVal + vObject.getPreScript2().trim());
		else
			strAudit.append("PRE_SCRIPT2" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getPreScript3()))
			strAudit.append("PRE_SCRIPT3" + auditDelimiterColVal + vObject.getPreScript3().trim());
		else
			strAudit.append("PRE_SCRIPT3" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSqlQuery1()))
			strAudit.append("SQL_QUERY1" + auditDelimiterColVal + vObject.getSqlQuery1().trim());
		else
			strAudit.append("SQL_QUERY1" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSqlQuery2()))
			strAudit.append("SQL_QUERY2" + auditDelimiterColVal + vObject.getSqlQuery2().trim());
		else
			strAudit.append("SQL_QUERY2" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getSqlQuery3()))
			strAudit.append("SQL_QUERY3" + auditDelimiterColVal + vObject.getSqlQuery3().trim());
		else
			strAudit.append("SQL_QUERY3" + auditDelimiterColVal + "NULL");
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

		return strAudit.toString();
	}

	@Override
	protected void setServiceDefaults() {
		serviceName = "EtlManualQueryList";
		serviceDesc = "ETL Manual Query List";
		tableName = "ETL_MANUAL_QUERY_LIST";
		childTableName = "ETL_MANUAL_QUERY_LIST";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}

}
