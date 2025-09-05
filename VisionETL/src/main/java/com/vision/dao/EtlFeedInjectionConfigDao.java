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
import com.vision.vb.EtlFeedInjectionConfigVb;
import com.vision.vb.EtlFeedMainVb;

@Component
public class EtlFeedInjectionConfigDao extends AbstractDao<EtlFeedInjectionConfigVb> {

	/******* Mapper Start **********/
	String CountryApprDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TAppr.COUNTRY) COUNTRY_DESC";
	String CountryPendDesc = "(SELECT COUNTRY_DESCRIPTION FROM COUNTRIES WHERE COUNTRY = TPend.COUNTRY) COUNTRY_DESC";
	String LeBookApprDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TAppr.LE_BOOK AND COUNTRY = TAppr.COUNTRY ) LE_BOOK_DESC";
	String LeBookTPendDesc = "(SELECT  LEB_DESCRIPTION FROM LE_BOOK WHERE  LE_BOOK = TPend.LE_BOOK AND COUNTRY = TPend.COUNTRY ) LE_BOOK_DESC";

	String InjectionTypeAtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2107, "TAppr.INJECTION_TYPE",
			"INJECTION_TYPE_DESC");
	String InjectionTypeAtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("AT", 2107, "TPend.INJECTION_TYPE",
			"INJECTION_TYPE_DESC");

	String EtlInjectionStatusNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000,
			"TAppr.ETL_INJECTION_STATUS", "ETL_INJECTION_STATUS_DESC");
	String EtlInjectionStatusNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 2000,
			"TPend.ETL_INJECTION_STATUS", "ETL_INJECTION_STATUS_DESC");

	String RecordIndicatorNtApprDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TAppr.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");
	String RecordIndicatorNtPendDesc = ValidationUtil.numAlphaTabDescritpionQuery("NT", 7, "TPend.RECORD_INDICATOR",
			"RECORD_INDICATOR_DESC");

	@Override
	protected RowMapper getMapper() {
		RowMapper mapper = new RowMapper() {
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				EtlFeedInjectionConfigVb vObject = new EtlFeedInjectionConfigVb();
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
				if (rs.getString("FEED_ID") != null) {
					vObject.setFeedId(rs.getString("FEED_ID"));
				} else {
					vObject.setFeedId("");
				}
				vObject.setInjectionTypeAt(rs.getInt("INJECTION_TYPE_AT"));
				if (rs.getString("INJECTION_TYPE") != null) {
					vObject.setInjectionType(rs.getString("INJECTION_TYPE"));
				} else {
					vObject.setInjectionType("");
				}
				if (rs.getString("BENCH_MARK_COLUMN_ID") != null) {
					vObject.setBenchMarkColumnId(rs.getString("BENCH_MARK_COLUMN_ID"));
				} else {
					vObject.setBenchMarkColumnId("");
				}
				if (rs.getString("BENCH_MARK_RULE") != null) {
					vObject.setBenchMarkRule(rs.getString("BENCH_MARK_RULE"));
				} else {
					vObject.setBenchMarkRule("");
				}
				if (rs.getString("FIRST_TIME_VARIENT_FLAG") != null) {
					vObject.setFirstTimeVarientFlag(rs.getString("FIRST_TIME_VARIENT_FLAG"));
				} else {
					vObject.setFirstTimeVarientFlag("");
				}
				if (rs.getString("FIRST_INJECTION_TYPE") != null) {
					vObject.setFirstInjectionType(rs.getString("FIRST_INJECTION_TYPE"));
				} else {
					vObject.setFirstInjectionType("");
				}
				vObject.setEtlInjectionStatusNt(rs.getInt("ETL_INJECTION_STATUS_NT"));
				vObject.setEtlInjectionStatus(rs.getInt("ETL_INJECTION_STATUS"));
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

	public List<EtlFeedInjectionConfigVb> getQueryPopupResults(EtlFeedInjectionConfigVb dObj) {
		Vector<Object> params = new Vector<Object>();
		StringBuffer strBufApprove = new StringBuffer("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.FEED_ID"
				+ ",TAppr.INJECTION_TYPE_AT,TAppr.INJECTION_TYPE,TAppr.BENCH_MARK_COLUMN_ID"
				+ ",TAppr.BENCH_MARK_RULE,TAppr.FIRST_TIME_VARIENT_FLAG,TAppr.FIRST_INJECTION_TYPE"
				+ ",TAppr.ETL_INJECTION_STATUS_NT,TAppr.ETL_INJECTION_STATUS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_FEED_INJECTION_CONFIG TAppr ");
		String strWhereNotExists = new String(
				" Not Exists (Select 'X' From ETL_FEED_INJECTION_CONFIG_PEND TPend WHERE TAppr.LE_BOOK = TPend.LE_BOOK AND TAppr.COUNTRY = TPend.COUNTRY AND TAppr.FEED_ID = TPend.FEED_ID )");
		StringBuffer strBufPending = new StringBuffer("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.FEED_ID"
				+ ",TPend.INJECTION_TYPE_AT,TPend.INJECTION_TYPE,TPend.BENCH_MARK_COLUMN_ID"
				+ ",TPend.BENCH_MARK_RULE,TPend.FIRST_TIME_VARIENT_FLAG,TPend.FIRST_INJECTION_TYPE"
				+ ",TPend.ETL_INJECTION_STATUS_NT,TPend.ETL_INJECTION_STATUS,TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_FEED_INJECTION_CONFIG_PEND TPend ");
		StringBuffer strBufUpl = new StringBuffer("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.FEED_ID"
				+ ",TUpl.INJECTION_TYPE_AT,TUpl.INJECTION_TYPE,TUpl.BENCH_MARK_COLUMN_ID"
				+ ",TUpl.BENCH_MARK_RULE,TUpl.FIRST_TIME_VARIENT_FLAG,TUpl.FIRST_INJECTION_TYPE"
				+ ",TUpl.ETL_INJECTION_STATUS_NT,TUpl.ETL_INJECTION_STATUS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION from ETL_FEED_INJECTION_CONFIG_UPL TUpl ");
		String strWhereNotExistsApprInUpl = new String(
				" Not Exists (Select 'X' From ETL_FEED_INJECTION_CONFIG_UPL TUpl WHERE TAppr.LE_BOOK = TUpl.LE_BOOK AND TAppr.COUNTRY = TUpl.COUNTRY AND TAppr.FEED_ID = TUpl.FEED_ID) ");
		String strWhereNotExistsPendInUpl = new String(
				" Not Exists (Select 'X' From ETL_FEED_INJECTION_CONFIG_UPL TUpl WHERE TUpl.LE_BOOK = TPend.LE_BOOK AND TUpl.COUNTRY = TPend.COUNTRY AND TUpl.FEED_ID = TPend.FEED_ID) ");
		try {
			if (ValidationUtil.isValid(dObj.getCountry())) {
				params.addElement(dObj.getCountry());
				CommonUtils.addToQuery("UPPER(TAppr.COUNTRY) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.COUNTRY) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.COUNTRY) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getLeBook())) {
				params.addElement(dObj.getLeBook());
				CommonUtils.addToQuery("UPPER(TAppr.LE_BOOK) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.LE_BOOK) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.LE_BOOK) = ?", strBufUpl);
			}
			if (ValidationUtil.isValid(dObj.getFeedId())) {
				params.addElement(dObj.getFeedId());
				CommonUtils.addToQuery("UPPER(TAppr.FEED_ID) = ?", strBufApprove);
				CommonUtils.addToQuery("UPPER(TPend.FEED_ID) = ?", strBufPending);
				CommonUtils.addToQuery("UPPER(TUpl.FEED_ID) = ?", strBufUpl);
			}
			/*
			 * if (ValidationUtil.isValid(dObj.getInjectionType())){
			 * params.addElement(dObj.getInjectionType());
			 * CommonUtils.addToQuery("UPPER(TAppr.INJECTION_TYPE) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.INJECTION_TYPE) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.INJECTION_TYPE) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getBenchMarkColumnId())){
			 * params.addElement(dObj.getBenchMarkColumnId());
			 * CommonUtils.addToQuery("UPPER(TAppr.BENCH_MARK_COLUMN_ID) = ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.BENCH_MARK_COLUMN_ID) = ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.BENCH_MARK_COLUMN_ID) = ?", strBufUpl); }
			 * if (ValidationUtil.isValid(dObj.getBenchMarkRule())){
			 * params.addElement(dObj.getBenchMarkRule());
			 * CommonUtils.addToQuery("UPPER(TAppr.BENCH_MARK_RULE) = ?", strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.BENCH_MARK_RULE) = ?", strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.BENCH_MARK_RULE) = ?", strBufUpl); } if
			 * (ValidationUtil.isValid(dObj.getFirstTimeVarientFlag())){
			 * params.addElement(dObj.getFirstTimeVarientFlag());
			 * CommonUtils.addToQuery("UPPER(TAppr.FIRST_TIME_VARIENT_FLAG) = ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.FIRST_TIME_VARIENT_FLAG) = ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.FIRST_TIME_VARIENT_FLAG) = ?", strBufUpl);
			 * } if (ValidationUtil.isValid(dObj.getFirstInjectionType())){
			 * params.addElement(dObj.getFirstInjectionType());
			 * CommonUtils.addToQuery("UPPER(TAppr.FIRST_INJECTION_TYPE) = ?",
			 * strBufApprove);
			 * CommonUtils.addToQuery("UPPER(TPend.FIRST_INJECTION_TYPE) = ?",
			 * strBufPending);
			 * CommonUtils.addToQuery("UPPER(TUpl.FIRST_INJECTION_TYPE) = ?", strBufUpl); }
			 * if (ValidationUtil.isValid(dObj.getEtlInjectionStatus())){
			 * params.addElement(dObj.getEtlInjectionStatus());
			 * CommonUtils.addToQuery("TAppr.ETL_INJECTION_STATUS = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.ETL_INJECTION_STATUS = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.ETL_INJECTION_STATUS = ?", strBufUpl); } if
			 * (dObj.getRecordIndicator() != -1){ if (dObj.getRecordIndicator() > 3){
			 * params.addElement(new Integer(0));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR > ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR > ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR > ?", strBufUpl); }else{
			 * params.addElement(new Integer(dObj.getRecordIndicator()));
			 * CommonUtils.addToQuery("TAppr.RECORD_INDICATOR = ?", strBufApprove);
			 * CommonUtils.addToQuery("TPend.RECORD_INDICATOR = ?", strBufPending);
			 * CommonUtils.addToQuery("TUpl.RECORD_INDICATOR = ?", strBufUpl); } }
			 */
			String orderBy = " Order By LE_BOOK,  COUNTRY, FEED_ID ";
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

	public List<EtlFeedInjectionConfigVb> getQueryResults(EtlFeedInjectionConfigVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedInjectionConfigVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.FEED_ID"
				+ ",TAppr.INJECTION_TYPE_AT,TAppr.INJECTION_TYPE,TAppr.BENCH_MARK_COLUMN_ID"
				+ ",TAppr.BENCH_MARK_RULE,TAppr.FIRST_TIME_VARIENT_FLAG,TAppr.FIRST_INJECTION_TYPE"
				+ ",TAppr.ETL_INJECTION_STATUS_NT,TAppr.ETL_INJECTION_STATUS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_INJECTION_CONFIG TAppr WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.FEED_ID"
				+ ",TPend.INJECTION_TYPE_AT,TPend.INJECTION_TYPE,TPend.BENCH_MARK_COLUMN_ID"
				+ ",TPend.BENCH_MARK_RULE,TPend.FIRST_TIME_VARIENT_FLAG,TPend.FIRST_INJECTION_TYPE"
				+ ",TPend.ETL_INJECTION_STATUS_NT,TPend.ETL_INJECTION_STATUS,TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_INJECTION_CONFIG_PEND TPend WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.FEED_ID"
				+ ",TUpl.INJECTION_TYPE_AT,TUpl.INJECTION_TYPE,TUpl.BENCH_MARK_COLUMN_ID"
				+ ",TUpl.BENCH_MARK_RULE,TUpl.FIRST_TIME_VARIENT_FLAG,TUpl.FIRST_INJECTION_TYPE"
				+ ",TUpl.ETL_INJECTION_STATUS_NT,TUpl.ETL_INJECTION_STATUS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_INJECTION_CONFIG_UPL TPend WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getLeBook();
		objParams[1] = dObj.getCountry();
		objParams[2] = dObj.getFeedId();
		try {
			if (!dObj.isVerificationRequired()) {
				intStatus = 0;
			}
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getMapper());
			} else if (intStatus == 0) {
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

	public List<EtlFeedInjectionConfigVb> getQueryResultsByParent(EtlFeedMainVb dObj, int intStatus) {
		setServiceDefaults();
		List<EtlFeedInjectionConfigVb> collTemp = null;
		final int intKeyFieldsCount = 3;
		String strQueryAppr = new String("Select TAppr.COUNTRY,TAppr.LE_BOOK,TAppr.FEED_ID"
				+ ",TAppr.INJECTION_TYPE_AT,TAppr.INJECTION_TYPE,TAppr.BENCH_MARK_COLUMN_ID"
				+ ",TAppr.BENCH_MARK_RULE,TAppr.FIRST_TIME_VARIENT_FLAG,TAppr.FIRST_INJECTION_TYPE"
				+ ",TAppr.ETL_INJECTION_STATUS_NT,TAppr.ETL_INJECTION_STATUS,TAppr.RECORD_INDICATOR_NT"
				+ ",TAppr.RECORD_INDICATOR,TAppr.MAKER,TAppr.VERIFIER,TAppr.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TAppr.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_INJECTION_CONFIG TAppr WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?  ");
		String strQueryPend = new String("Select TPend.COUNTRY,TPend.LE_BOOK,TPend.FEED_ID"
				+ ",TPend.INJECTION_TYPE_AT,TPend.INJECTION_TYPE,TPend.BENCH_MARK_COLUMN_ID"
				+ ",TPend.BENCH_MARK_RULE,TPend.FIRST_TIME_VARIENT_FLAG,TPend.FIRST_INJECTION_TYPE"
				+ ",TPend.ETL_INJECTION_STATUS_NT,TPend.ETL_INJECTION_STATUS,TPend.RECORD_INDICATOR_NT"
				+ ",TPend.RECORD_INDICATOR,TPend.MAKER,TPend.VERIFIER,TPend.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TPend.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_INJECTION_CONFIG_PEND TPend WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?   ");
		String strQueryUpl = new String("Select TUpl.COUNTRY,TUpl.LE_BOOK,TUpl.FEED_ID"
				+ ",TUpl.INJECTION_TYPE_AT,TUpl.INJECTION_TYPE,TUpl.BENCH_MARK_COLUMN_ID"
				+ ",TUpl.BENCH_MARK_RULE,TUpl.FIRST_TIME_VARIENT_FLAG,TUpl.FIRST_INJECTION_TYPE"
				+ ",TUpl.ETL_INJECTION_STATUS_NT,TUpl.ETL_INJECTION_STATUS,TUpl.RECORD_INDICATOR_NT"
				+ ",TUpl.RECORD_INDICATOR,TUpl.MAKER,TUpl.VERIFIER,TUpl.INTERNAL_STATUS, "
				+ getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_LAST_MODIFIED, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_LAST_MODIFIED, " + getDbFunction(Constants.DATEFUNC, null) + "(TUpl.DATE_CREATION, '"
				+ getDbFunction(Constants.DD_Mon_RRRR, null) + " " + getDbFunction(Constants.TIME, null)
				+ "') DATE_CREATION"
				+ " From ETL_FEED_INJECTION_CONFIG_UPL TUpl WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?   ");

		Object objParams[] = new Object[intKeyFieldsCount];
		objParams[0] = dObj.getLeBook();
		objParams[1] = dObj.getCountry();
		objParams[2] = dObj.getFeedId();
		try {
			if (intStatus == 9999) {
				collTemp = getJdbcTemplate().query(strQueryUpl.toString(), objParams, getMapper());
			} else if (intStatus == 0) {
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
	protected List<EtlFeedInjectionConfigVb> selectApprovedRecord(EtlFeedInjectionConfigVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_ZERO);
	}

	@Override
	protected List<EtlFeedInjectionConfigVb> doSelectPendingRecord(EtlFeedInjectionConfigVb vObject) {
		return getQueryResults(vObject, Constants.STATUS_PENDING);
	}

	@Override
	protected List<EtlFeedInjectionConfigVb> doSelectUplRecord(EtlFeedInjectionConfigVb vObject) {
		return getQueryResults(vObject, 9999);
	}

	@Override
	protected int getStatus(EtlFeedInjectionConfigVb records) {
		return records.getEtlInjectionStatus();
	}

	@Override
	protected void setStatus(EtlFeedInjectionConfigVb vObject, int status) {
		vObject.setEtlInjectionStatus(status);
	}

	@Override
	protected int doInsertionAppr(EtlFeedInjectionConfigVb vObject) {
		String query = "Insert Into ETL_FEED_INJECTION_CONFIG (COUNTRY, LE_BOOK, FEED_ID, INJECTION_TYPE_AT, INJECTION_TYPE, BENCH_MARK_COLUMN_ID, "
				+ "BENCH_MARK_RULE, FIRST_TIME_VARIENT_FLAG, FIRST_INJECTION_TYPE, ETL_INJECTION_STATUS_NT, ETL_INJECTION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getInjectionTypeAt(),
				vObject.getInjectionType(), vObject.getBenchMarkColumnId(), vObject.getBenchMarkRule(),
				vObject.getFirstTimeVarientFlag(), vObject.getFirstInjectionType(), vObject.getEtlInjectionStatusNt(),
				vObject.getEtlInjectionStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPend(EtlFeedInjectionConfigVb vObject) {
		String query = "Insert Into ETL_FEED_INJECTION_CONFIG_PEND (COUNTRY, LE_BOOK, FEED_ID, INJECTION_TYPE_AT, INJECTION_TYPE, BENCH_MARK_COLUMN_ID, "
				+ "BENCH_MARK_RULE, FIRST_TIME_VARIENT_FLAG, FIRST_INJECTION_TYPE, ETL_INJECTION_STATUS_NT, ETL_INJECTION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getInjectionTypeAt(),
				vObject.getInjectionType(), vObject.getBenchMarkColumnId(), vObject.getBenchMarkRule(),
				vObject.getFirstTimeVarientFlag(), vObject.getFirstInjectionType(), vObject.getEtlInjectionStatusNt(),
				vObject.getEtlInjectionStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionUpl(EtlFeedInjectionConfigVb vObject) {
		String query = "Insert Into ETL_FEED_INJECTION_CONFIG_UPL (COUNTRY, LE_BOOK, FEED_ID, INJECTION_TYPE_AT, INJECTION_TYPE, BENCH_MARK_COLUMN_ID, "
				+ "BENCH_MARK_RULE, FIRST_TIME_VARIENT_FLAG, FIRST_INJECTION_TYPE, ETL_INJECTION_STATUS_NT, ETL_INJECTION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR,"
				+ " MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.SYSDATE, null) + ")";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getInjectionTypeAt(),
				vObject.getInjectionType(), vObject.getBenchMarkColumnId(), vObject.getBenchMarkRule(),
				vObject.getFirstTimeVarientFlag(), vObject.getFirstInjectionType(), vObject.getEtlInjectionStatusNt(),
				vObject.getEtlInjectionStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doInsertionPendWithDc(EtlFeedInjectionConfigVb vObject) {
		String query = "Insert Into ETL_FEED_INJECTION_CONFIG_PEND (COUNTRY, LE_BOOK, FEED_ID, INJECTION_TYPE_AT, "
				+ "INJECTION_TYPE, BENCH_MARK_COLUMN_ID, BENCH_MARK_RULE, FIRST_TIME_VARIENT_FLAG, FIRST_INJECTION_TYPE, ETL_INJECTION_STATUS_NT,"
				+ " ETL_INJECTION_STATUS, RECORD_INDICATOR_NT, RECORD_INDICATOR, MAKER, VERIFIER, INTERNAL_STATUS, DATE_LAST_MODIFIED, DATE_CREATION)"
				+ "Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + getDbFunction(Constants.SYSDATE, null)
				+ ", " + getDbFunction(Constants.TO_DATE, vObject.getDateCreation()) + "  )";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getInjectionTypeAt(),
				vObject.getInjectionType(), vObject.getBenchMarkColumnId(), vObject.getBenchMarkRule(),
				vObject.getFirstTimeVarientFlag(), vObject.getFirstInjectionType(), vObject.getEtlInjectionStatusNt(),
				vObject.getEtlInjectionStatus(), vObject.getRecordIndicatorNt(), vObject.getRecordIndicator(),
				vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdateAppr(EtlFeedInjectionConfigVb vObject) {
		String query = "Update ETL_FEED_INJECTION_CONFIG Set INJECTION_TYPE_AT = ?, INJECTION_TYPE = ?, BENCH_MARK_COLUMN_ID = ?,"
				+ " BENCH_MARK_RULE = ?, FIRST_TIME_VARIENT_FLAG = ?, FIRST_INJECTION_TYPE = ?, ETL_INJECTION_STATUS_NT = ?, ETL_INJECTION_STATUS = ?,"
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ "WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getInjectionTypeAt(), vObject.getInjectionType(), vObject.getBenchMarkColumnId(),
				vObject.getBenchMarkRule(), vObject.getFirstTimeVarientFlag(), vObject.getFirstInjectionType(),
				vObject.getEtlInjectionStatusNt(), vObject.getEtlInjectionStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doUpdatePend(EtlFeedInjectionConfigVb vObject) {
		String query = "Update ETL_FEED_INJECTION_CONFIG_UPL Set INJECTION_TYPE_AT = ?, INJECTION_TYPE = ?, BENCH_MARK_COLUMN_ID = ?,"
				+ " BENCH_MARK_RULE = ?, FIRST_TIME_VARIENT_FLAG = ?, FIRST_INJECTION_TYPE = ?, ETL_INJECTION_STATUS_NT = ?, ETL_INJECTION_STATUS = ?,"
				+ " RECORD_INDICATOR_NT = ?, RECORD_INDICATOR = ?, MAKER = ?, VERIFIER = ?, INTERNAL_STATUS = ?, DATE_LAST_MODIFIED = "
				+ getDbFunction(Constants.SYSDATE, null) + "  "
				+ "WHERE LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ? ";
		Object[] args = { vObject.getInjectionTypeAt(), vObject.getInjectionType(), vObject.getBenchMarkColumnId(),
				vObject.getBenchMarkRule(), vObject.getFirstTimeVarientFlag(), vObject.getFirstInjectionType(),
				vObject.getEtlInjectionStatusNt(), vObject.getEtlInjectionStatus(), vObject.getRecordIndicatorNt(),
				vObject.getRecordIndicator(), vObject.getMaker(), vObject.getVerifier(), vObject.getInternalStatus(),
				vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };

		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int doDeleteAppr(EtlFeedInjectionConfigVb vObject) {
		String query = "Delete From ETL_FEED_INJECTION_CONFIG Where LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getLeBook(), vObject.getCountry(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deletePendingRecord(EtlFeedInjectionConfigVb vObject) {
		String query = "Delete From ETL_FEED_INJECTION_CONFIG_PEND Where LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getLeBook(), vObject.getCountry(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected int deleteUplRecord(EtlFeedInjectionConfigVb vObject) {
		String query = "Delete From ETL_FEED_INJECTION_CONFIG_UPL Where LE_BOOK = ?  AND COUNTRY = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getLeBook(), vObject.getCountry(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_INJECTION_CONFIG";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_INJECTION_CONFIG_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_INJECTION_CONFIG_PEND";
		}
		String query = "Delete From " + table + " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	public int deleteRecordByPRSParent(EtlFeedMainVb vObject, String tableType) {
		String table = "ETL_FEED_INJECTION_CONFIG_PRS";
		String query = "Delete From " + table
				+ " Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ? AND SESSION_ID=? ";
		Object[] args = { vObject.getCountry(), vObject.getLeBook(), vObject.getFeedId(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

	public int updateStatusByParent(EtlFeedMainVb vObject, String tableType, int fStatus) {
		String table = "ETL_FEED_INJECTION_CONFIG";
		if ("UPL".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_INJECTION_CONFIG_UPL";
		} else if ("PEND".equalsIgnoreCase(tableType)) {
			table = "ETL_FEED_INJECTION_CONFIG_PEND";
		}
		String query = "update " + table
				+ " set ETL_INJECTION_STATUS = ?, RECORD_INDICATOR =?   Where COUNTRY = ?  AND LE_BOOK = ?  AND FEED_ID = ?  ";
		Object[] args = { fStatus, vObject.getRecordIndicator(), vObject.getCountry(), vObject.getLeBook(),
				vObject.getFeedId() };
		return getJdbcTemplate().update(query, args);
	}

	@Override
	protected String getAuditString(EtlFeedInjectionConfigVb vObject) {
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

		if (ValidationUtil.isValid(vObject.getFeedId()))
			strAudit.append("FEED_ID" + auditDelimiterColVal + vObject.getFeedId().trim());
		else
			strAudit.append("FEED_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("INJECTION_TYPE_AT" + auditDelimiterColVal + vObject.getInjectionTypeAt());
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getInjectionType()))
			strAudit.append("INJECTION_TYPE" + auditDelimiterColVal + vObject.getInjectionType().trim());
		else
			strAudit.append("INJECTION_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getBenchMarkColumnId()))
			strAudit.append("BENCH_MARK_COLUMN_ID" + auditDelimiterColVal + vObject.getBenchMarkColumnId().trim());
		else
			strAudit.append("BENCH_MARK_COLUMN_ID" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getBenchMarkRule()))
			strAudit.append("BENCH_MARK_RULE" + auditDelimiterColVal + vObject.getBenchMarkRule().trim());
		else
			strAudit.append("BENCH_MARK_RULE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFirstTimeVarientFlag()))
			strAudit.append(
					"FIRST_TIME_VARIENT_FLAG" + auditDelimiterColVal + vObject.getFirstTimeVarientFlag().trim());
		else
			strAudit.append("FIRST_TIME_VARIENT_FLAG" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		if (ValidationUtil.isValid(vObject.getFirstInjectionType()))
			strAudit.append("FIRST_INJECTION_TYPE" + auditDelimiterColVal + vObject.getFirstInjectionType().trim());
		else
			strAudit.append("FIRST_INJECTION_TYPE" + auditDelimiterColVal + "NULL");
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_INJECTION_STATUS_NT" + auditDelimiterColVal + vObject.getEtlInjectionStatusNt());
		strAudit.append(auditDelimiter);

		strAudit.append("ETL_INJECTION_STATUS" + auditDelimiterColVal + vObject.getEtlInjectionStatus());
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
		serviceName = "EtlFeedInjectionConfig";
		serviceDesc = "ETL Feed Injection Config";
		tableName = "ETL_FEED_INJECTION_CONFIG";
		childTableName = "ETL_FEED_INJECTION_CONFIG";
		intCurrentUserId = CustomContextHolder.getContext().getVisionId();
	}
	
	public int deleteRecordByPRSParentByCategory(EtlFeedMainVb vObject) {
		String query = "delete FROM ETL_FEED_INJECTION_CONFIG_PRS WHERE EXISTS (    SELECT FEED_ID"
				+ "    FROM ETL_FEED_MAIN_PRS    WHERE FEED_CATEGORY = ? AND SESSION_ID = ? "
				+ "	AND ETL_FEED_MAIN_PRS.FEED_ID = ETL_FEED_INJECTION_CONFIG_PRS.FEED_ID AND ETL_FEED_MAIN_PRS.SESSION_ID "
				+ " = ETL_FEED_INJECTION_CONFIG_PRS.SESSION_ID ) ";
		Object[] args = { vObject.getFeedCategory(), vObject.getSessionId() };
		return getJdbcTemplate().update(query, args);
	}

}
